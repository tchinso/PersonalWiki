from __future__ import annotations

import json
import math
import re
import sqlite3
import sys
import unicodedata
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

from flask import (
    Flask,
    abort,
    g,
    jsonify,
    redirect,
    render_template,
    request,
    send_from_directory,
    url_for,
)
from markupsafe import Markup

from markdown_engine import MarkdownEngine, extract_reference_targets


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def runtime_paths() -> tuple[Path, Path]:
    if getattr(sys, "frozen", False):
        resource_dir = Path(getattr(sys, "_MEIPASS", Path(sys.executable).resolve().parent))
        data_dir = Path(sys.executable).resolve().parent
    else:
        resource_dir = Path(__file__).resolve().parent
        data_dir = resource_dir
    return resource_dir, data_dir


RESOURCE_DIR, DATA_DIR = runtime_paths()
DOC_DIR = DATA_DIR / "doc"
IMG_DIR = DATA_DIR / "img"
DB_PATH = DATA_DIR / "wiki.db"
FTS_DB_PATH = DATA_DIR / "wiki_fts.db"
TEMPLATE_DIR = RESOURCE_DIR / "templates"
STATIC_DIR = RESOURCE_DIR / "static"


app = Flask(
    __name__,
    template_folder=str(TEMPLATE_DIR),
    static_folder=str(STATIC_DIR),
)
app.config["JSON_AS_ASCII"] = False

markdown_engine = MarkdownEngine()

TOKEN_RE = re.compile(r"[A-Za-z0-9가-힣]{2,}")
STOPWORDS = {
    "the",
    "a",
    "an",
    "and",
    "or",
    "not",
    "with",
    "from",
    "into",
    "about",
    "that",
    "this",
    "there",
    "their",
    "your",
    "you",
    "for",
    "are",
    "was",
    "were",
    "been",
    "will",
    "shall",
    "have",
    "has",
    "had",
    "can",
    "could",
    "would",
    "should",
    "to",
    "of",
    "in",
    "on",
    "at",
    "as",
    "is",
    "it",
    "its",
    "by",
    "be",
    "if",
    "else",
    "when",
    "where",
    "which",
    "who",
    "what",
    "why",
    "how",
    "문서",
    "내용",
    "그리고",
    "또는",
    "에서",
    "으로",
    "입니다",
    "있는",
    "하는",
    "합니다",
    "대한",
    "통해",
    "관련",
    "사용",
    "기능",
    "추가",
}


def connect_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def connect_fts_db() -> sqlite3.Connection:
    conn = sqlite3.connect(FTS_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def get_db() -> sqlite3.Connection:
    if "db" not in g:
        g.db = connect_db()
    return g.db


def get_fts_db() -> sqlite3.Connection:
    if "fts_db" not in g:
        g.fts_db = connect_fts_db()
    return g.fts_db


@app.teardown_appcontext
def close_db(_error: BaseException | None) -> None:
    conn = g.pop("db", None)
    if conn is not None:
        conn.close()
    fts_conn = g.pop("fts_db", None)
    if fts_conn is not None:
        fts_conn.close()


def init_storage() -> None:
    DOC_DIR.mkdir(parents=True, exist_ok=True)
    IMG_DIR.mkdir(parents=True, exist_ok=True)


def init_db() -> None:
    with connect_db() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS docs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL COLLATE NOCASE UNIQUE,
                slug TEXT NOT NULL UNIQUE,
                file_path TEXT NOT NULL,
                meta_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tags (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL COLLATE NOCASE UNIQUE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS doc_tags (
                doc_id INTEGER NOT NULL,
                tag_id INTEGER NOT NULL,
                PRIMARY KEY (doc_id, tag_id),
                FOREIGN KEY (doc_id) REFERENCES docs (id) ON DELETE CASCADE,
                FOREIGN KEY (tag_id) REFERENCES tags (id) ON DELETE CASCADE
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_docs_slug ON docs (slug)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_doc_tags_tag_id ON doc_tags (tag_id)")
        # Legacy cleanup: older versions stored docs_fts in wiki.db.
        conn.execute("DROP TABLE IF EXISTS docs_fts")


def init_fts_db() -> None:
    with connect_fts_db() as conn:
        conn.execute(
            """
            CREATE VIRTUAL TABLE IF NOT EXISTS docs_fts
            USING fts5(title, content)
            """
        )


def safe_load_json(raw: str) -> dict:
    try:
        data = json.loads(raw)
        if isinstance(data, dict):
            return data
    except (TypeError, json.JSONDecodeError):
        pass
    return {}


def slugify(title: str) -> str:
    normalized = unicodedata.normalize("NFKC", title).strip()
    normalized = re.sub(r"[^\w\s\-가-힣]", "", normalized, flags=re.UNICODE)
    normalized = re.sub(r"[\s_]+", "-", normalized, flags=re.UNICODE)
    slug = normalized.strip("-").lower()
    return slug or "untitled"


def ensure_unique_slug(conn: sqlite3.Connection, base_slug: str, exclude_doc_id: int | None = None) -> str:
    root = base_slug or "untitled"
    candidate = root
    idx = 2
    while True:
        row = conn.execute("SELECT id FROM docs WHERE slug = ?", (candidate,)).fetchone()
        if row is None or row["id"] == exclude_doc_id:
            return candidate
        candidate = f"{root}-{idx}"
        idx += 1


def document_path(slug: str) -> Path:
    return DOC_DIR / f"{slug}.md"


def sidecar_path(slug: str) -> Path:
    return DOC_DIR / f"{slug}.json"


def read_document(slug: str) -> str:
    path = document_path(slug)
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


def read_document_if_exists(slug: str) -> str | None:
    path = document_path(slug)
    if not path.exists():
        return None
    return path.read_text(encoding="utf-8")


def write_document(slug: str, content: str) -> None:
    document_path(slug).write_text(content, encoding="utf-8")


def load_sidecar(slug: str) -> dict:
    path = sidecar_path(slug)
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            return data
    except json.JSONDecodeError:
        return {}
    return {}


def write_sidecar(
    *,
    slug: str,
    title: str,
    created_at: str,
    updated_at: str,
    tags: list[str],
    meta: dict,
) -> None:
    payload = {
        "title": title,
        "slug": slug,
        "created_at": created_at,
        "updated_at": updated_at,
        "tags": tags,
        "meta": meta,
    }
    sidecar_path(slug).write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def list_doc_tags(conn: sqlite3.Connection, doc_id: int) -> list[str]:
    rows = conn.execute(
        """
        SELECT t.name
        FROM tags t
        JOIN doc_tags dt ON dt.tag_id = t.id
        WHERE dt.doc_id = ?
        ORDER BY t.name COLLATE NOCASE
        """,
        (doc_id,),
    ).fetchall()
    return [row["name"] for row in rows]


def parse_tags(raw: str) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for part in raw.split(","):
        tag = part.strip()
        if not tag:
            continue
        lowered = tag.casefold()
        if lowered in seen:
            continue
        seen.add(lowered)
        result.append(tag)
    return result


def set_doc_tags(conn: sqlite3.Connection, doc_id: int, tags: list[str]) -> None:
    conn.execute("DELETE FROM doc_tags WHERE doc_id = ?", (doc_id,))
    for tag in tags:
        conn.execute("INSERT INTO tags (name) VALUES (?) ON CONFLICT(name) DO NOTHING", (tag,))
        tag_row = conn.execute("SELECT id FROM tags WHERE name = ?", (tag,)).fetchone()
        if tag_row:
            conn.execute(
                "INSERT OR IGNORE INTO doc_tags (doc_id, tag_id) VALUES (?, ?)",
                (doc_id, tag_row["id"]),
            )
    conn.execute("DELETE FROM tags WHERE id NOT IN (SELECT DISTINCT tag_id FROM doc_tags)")


def update_fts(fts_conn: sqlite3.Connection, doc_id: int, title: str, content: str) -> None:
    fts_conn.execute("DELETE FROM docs_fts WHERE rowid = ?", (doc_id,))
    fts_conn.execute(
        "INSERT INTO docs_fts (rowid, title, content) VALUES (?, ?, ?)",
        (doc_id, title, content),
    )


def rebuild_fts_index(conn: sqlite3.Connection, fts_conn: sqlite3.Connection) -> None:
    rows = conn.execute("SELECT id, title, slug FROM docs").fetchall()
    fts_conn.execute("DELETE FROM docs_fts")
    for row in rows:
        content = read_document(row["slug"])
        update_fts(fts_conn, row["id"], row["title"], content)


def infer_title_from_content(content: str, fallback: str) -> str:
    match = re.search(r"^\s*#\s+(.+)$", content, flags=re.MULTILINE)
    if match:
        title = match.group(1).strip()
        if title:
            return title
    return fallback


def resolve_doc_reference(conn: sqlite3.Connection, ref: str) -> str | None:
    lookup = ref.strip()
    if not lookup:
        return None

    row = conn.execute(
        "SELECT slug FROM docs WHERE title = ? COLLATE NOCASE",
        (lookup,),
    ).fetchone()
    if row:
        return row["slug"]

    row = conn.execute(
        "SELECT slug FROM docs WHERE slug = ? COLLATE NOCASE",
        (slugify(lookup),),
    ).fetchone()
    if row:
        return row["slug"]
    return None


def render_markdown(conn: sqlite3.Connection, text: str) -> Markup:
    html = markdown_engine.render(
        text,
        resolve_doc_reference=lambda ref: resolve_doc_reference(conn, ref),
        read_document=read_document_if_exists,
    )
    return Markup(html)


def normalize_search_query(query: str) -> str:
    query = re.sub(
        r"\b(and|or|not)\b",
        lambda m: m.group(1).upper(),
        query,
        flags=re.IGNORECASE,
    )
    return " ".join(query.split())


def singularize_token(token: str) -> str:
    if not token.isascii() or not token.isalpha():
        return token
    if token.endswith("ies") and len(token) > 4:
        return token[:-3] + "y"
    if token.endswith(("sses", "xes", "zes", "ches", "shes")) and len(token) > 4:
        return token[:-2]
    if token.endswith("s") and len(token) > 3 and not token.endswith("ss"):
        return token[:-1]
    return token


def tokenize_text(text: str) -> list[str]:
    tokens: list[str] = []
    for raw in TOKEN_RE.findall(text.lower()):
        token = singularize_token(raw)
        if token in STOPWORDS:
            continue
        if len(token) < 2:
            continue
        tokens.append(token)
    return tokens


def build_tfidf_vector(tf_counter: Counter[str], df_counter: Counter[str], total_docs: int) -> dict[str, float]:
    if total_docs <= 0:
        return {}
    vec: dict[str, float] = {}
    for token, tf in tf_counter.items():
        if tf <= 0:
            continue
        df = df_counter.get(token, 0)
        idf = math.log((total_docs + 1) / (df + 1)) + 1
        vec[token] = float(tf) * idf
    return vec


def cosine_similarity(vec_a: dict[str, float], vec_b: dict[str, float]) -> float:
    if not vec_a or not vec_b:
        return 0.0
    common = set(vec_a.keys()) & set(vec_b.keys())
    if not common:
        return 0.0
    numerator = sum(vec_a[token] * vec_b[token] for token in common)
    norm_a = math.sqrt(sum(v * v for v in vec_a.values()))
    norm_b = math.sqrt(sum(v * v for v in vec_b.values()))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return numerator / (norm_a * norm_b)


def recommend_tags(
    conn: sqlite3.Connection,
    *,
    title: str,
    content: str,
    current_slug: str | None = None,
    exclude_tags: list[str] | None = None,
    limit: int = 10,
) -> list[str]:
    query_tokens = tokenize_text(f"{title}\n{content}")
    if not query_tokens:
        return []

    rows = conn.execute("SELECT id, title, slug FROM docs").fetchall()
    corpus: list[dict] = []
    for row in rows:
        if current_slug and row["slug"] == current_slug:
            continue
        doc_content = read_document(row["slug"])
        doc_tokens = tokenize_text(f"{row['title']}\n{doc_content}")
        if not doc_tokens:
            continue
        corpus.append(
            {
                "tokens": doc_tokens,
                "tags": list_doc_tags(conn, row["id"]),
            }
        )
    if not corpus:
        return []

    df_counter: Counter[str] = Counter()
    for doc in corpus:
        for token in set(doc["tokens"]):
            df_counter[token] += 1

    total_docs = len(corpus)
    query_vec = build_tfidf_vector(Counter(query_tokens), df_counter, total_docs)
    if not query_vec:
        return []

    scored_docs: list[tuple[float, list[str]]] = []
    for doc in corpus:
        doc_vec = build_tfidf_vector(Counter(doc["tokens"]), df_counter, total_docs)
        similarity = cosine_similarity(query_vec, doc_vec)
        if similarity > 0:
            scored_docs.append((similarity, doc["tags"]))
    if not scored_docs:
        return []

    scored_docs.sort(key=lambda item: item[0], reverse=True)
    similar_docs = scored_docs[:30]

    excluded = {tag.casefold() for tag in (exclude_tags or [])}
    tag_counts: Counter[str] = Counter()
    tag_weights: defaultdict[str, float] = defaultdict(float)
    display_names: dict[str, str] = {}

    for similarity, tags in similar_docs:
        seen_in_doc: set[str] = set()
        for tag in tags:
            key = tag.casefold()
            if key in excluded or key in seen_in_doc:
                continue
            seen_in_doc.add(key)
            display_names.setdefault(key, tag)
            tag_counts[key] += 1
            tag_weights[key] += similarity

    ordered = sorted(
        tag_counts.keys(),
        key=lambda key: (-tag_counts[key], -tag_weights[key], display_names[key].casefold()),
    )
    return [display_names[key] for key in ordered[:limit]]


def find_backlinks(conn: sqlite3.Connection, target_slug: str) -> list[dict]:
    rows = conn.execute(
        """
        SELECT id, title, slug, updated_at
        FROM docs
        WHERE slug != ?
        ORDER BY updated_at DESC, title COLLATE NOCASE
        """,
        (target_slug,),
    ).fetchall()

    backlinks: list[dict] = []
    for row in rows:
        content = read_document(row["slug"])
        wiki_refs, template_refs = extract_reference_targets(content)

        has_link_reference = any(
            resolve_doc_reference(conn, ref) == target_slug
            for ref in wiki_refs
        )
        has_template_reference = any(
            resolve_doc_reference(conn, ref) == target_slug
            for ref in template_refs
        )

        if not has_link_reference and not has_template_reference:
            continue

        reasons: list[str] = []
        if has_link_reference:
            reasons.append("link")
        if has_template_reference:
            reasons.append("template")

        item = dict(row)
        item["reasons"] = reasons
        backlinks.append(item)
    return backlinks


def sync_documents() -> None:
    with connect_db() as conn, connect_fts_db() as fts_conn:
        md_files = sorted(DOC_DIR.glob("*.md"))
        existing_rows = conn.execute("SELECT id, slug FROM docs").fetchall()
        existing_by_slug = {row["slug"]: row for row in existing_rows}
        seen_slugs: set[str] = set()

        for md_file in md_files:
            slug = md_file.stem
            seen_slugs.add(slug)
            content = md_file.read_text(encoding="utf-8")
            row = existing_by_slug.get(slug)

            if row is None:
                sidecar = load_sidecar(slug)
                sidecar_title = str(sidecar.get("title", "")).strip()
                title = sidecar_title or infer_title_from_content(content, slug)
                if not title:
                    title = slug

                unique_title = title
                suffix = 2
                while conn.execute(
                    "SELECT 1 FROM docs WHERE title = ? COLLATE NOCASE",
                    (unique_title,),
                ).fetchone():
                    unique_title = f"{title} ({suffix})"
                    suffix += 1

                created_at = str(sidecar.get("created_at") or now_iso())
                updated_at = str(sidecar.get("updated_at") or now_iso())
                meta = sidecar.get("meta") if isinstance(sidecar.get("meta"), dict) else {}
                meta["sidecar"] = f"{slug}.json"
                conn.execute(
                    """
                    INSERT INTO docs (title, slug, file_path, meta_json, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        unique_title,
                        slug,
                        str(md_file),
                        json.dumps(meta, ensure_ascii=False),
                        created_at,
                        updated_at,
                    ),
                )
                doc_id = conn.execute(
                    "SELECT id FROM docs WHERE slug = ?",
                    (slug,),
                ).fetchone()["id"]
                sidecar_tags = sidecar.get("tags") if isinstance(sidecar.get("tags"), list) else []
                tags = [str(t).strip() for t in sidecar_tags if str(t).strip()]
                set_doc_tags(conn, doc_id, tags)
                write_sidecar(
                    slug=slug,
                    title=unique_title,
                    created_at=created_at,
                    updated_at=updated_at,
                    tags=tags,
                    meta=meta,
                )
                continue

            doc_id = row["id"]
            current_title_row = conn.execute("SELECT title FROM docs WHERE id = ?", (doc_id,)).fetchone()
            current_title = current_title_row["title"] if current_title_row else slug
            conn.execute(
                "UPDATE docs SET file_path = ? WHERE id = ?",
                (str(md_file), doc_id),
            )
            if not current_title:
                conn.execute(
                    "UPDATE docs SET title = ? WHERE id = ?",
                    (infer_title_from_content(content, slug), doc_id),
                )

        for row in existing_rows:
            if row["slug"] in seen_slugs:
                continue
            conn.execute("DELETE FROM docs WHERE id = ?", (row["id"],))

        rebuild_fts_index(conn, fts_conn)


def ensure_default_home() -> None:
    with connect_db() as conn, connect_fts_db() as fts_conn:
        count = conn.execute("SELECT COUNT(*) AS c FROM docs").fetchone()["c"]
        if count > 0:
            return

        title = "Home"
        slug = "home"
        content = """# Home

개인 위키에 오신 것을 환영합니다.

## 기본 문법

- 문서 링크: `[[문서명]]` 또는 `[[문서명|표시 텍스트]]`
- 이미지 삽입: `![[sample.png]]`
- 하이라이트: `==강조==`
- 스포일러: `||숨김 텍스트||`
- 유튜브 임베드: `![[youtube(HhnETSN6U_E)]]`
- 템플릿 포함: `{{공통문서}}` (중첩 템플릿은 확장되지 않음)
- 콜아웃: `!!! note 내용`, `!!! info 내용`, `!!! warn 내용`, `!!! danger 내용`

## 검색
검색창에서 `AND`, `OR`, `NOT` 연산자를 지원합니다.
예: `flask AND sqlite`, `python NOT django`
"""
        now = now_iso()
        meta = {"sidecar": f"{slug}.json"}
        conn.execute(
            """
            INSERT INTO docs (title, slug, file_path, meta_json, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                title,
                slug,
                str(document_path(slug)),
                json.dumps(meta, ensure_ascii=False),
                now,
                now,
            ),
        )
        doc_id = conn.execute("SELECT id FROM docs WHERE slug = ?", (slug,)).fetchone()["id"]
        tags = ["guide", "start"]
        write_document(slug, content)
        set_doc_tags(conn, doc_id, tags)
        update_fts(fts_conn, doc_id, title, content)
        write_sidecar(
            slug=slug,
            title=title,
            created_at=now,
            updated_at=now,
            tags=tags,
            meta=meta,
        )


def fetch_doc_with_tags(conn: sqlite3.Connection, slug: str) -> dict | None:
    row = conn.execute("SELECT * FROM docs WHERE slug = ?", (slug,)).fetchone()
    if row is None:
        return None
    data = dict(row)
    data["tags"] = list_doc_tags(conn, row["id"])
    data["meta"] = safe_load_json(row["meta_json"])
    return data


def fetch_docs_for_index(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        """
        SELECT id, title, slug, created_at, updated_at
        FROM docs
        ORDER BY updated_at DESC, title COLLATE NOCASE
        """
    ).fetchall()
    docs: list[dict] = []
    for row in rows:
        item = dict(row)
        item["tags"] = list_doc_tags(conn, row["id"])
        docs.append(item)
    return docs


@app.template_filter("pretty_time")
def pretty_time_filter(value: str) -> str:
    try:
        dt = datetime.fromisoformat(value)
        return dt.strftime("%Y-%m-%d %H:%M")
    except ValueError:
        return value


@app.route("/")
def index():
    conn = get_db()
    docs = fetch_docs_for_index(conn)
    return render_template("index.html", docs=docs)


@app.route("/doc/<path:slug>")
def view_doc(slug: str):
    conn = get_db()
    doc = fetch_doc_with_tags(conn, slug)
    if doc is None:
        return render_template("not_found.html", requested=slug), 404

    content = read_document(doc["slug"])
    rendered = render_markdown(conn, content)
    backlinks = find_backlinks(conn, doc["slug"])
    return render_template(
        "view.html",
        doc=doc,
        content=content,
        rendered=rendered,
        backlinks=backlinks,
    )


@app.route("/new", methods=["GET", "POST"])
def new_doc():
    conn = get_db()
    fts_conn = get_fts_db()

    if request.method == "POST":
        title = request.form.get("title", "").strip()
        content = request.form.get("content", "")
        tags = parse_tags(request.form.get("tags", ""))
        ignore_tag_warning = request.form.get("ignore_tag_warning") == "1"
        suggested_tags = recommend_tags(
            conn,
            title=title,
            content=content,
            exclude_tags=tags,
            limit=10,
        )

        if not title:
            return render_template(
                "edit.html",
                mode="new",
                doc={"title": "", "slug": ""},
                content=content,
                tags_text=", ".join(tags),
                error="문서 제목을 입력해 주세요.",
                tag_warning=None,
                show_ignore_tag_warning=False,
                recommended_tags=suggested_tags,
            )

        duplicate = conn.execute(
            "SELECT 1 FROM docs WHERE title = ? COLLATE NOCASE",
            (title,),
        ).fetchone()
        if duplicate:
            return render_template(
                "edit.html",
                mode="new",
                doc={"title": title, "slug": ""},
                content=content,
                tags_text=", ".join(tags),
                error="같은 제목의 문서가 이미 있습니다.",
                tag_warning=None,
                show_ignore_tag_warning=False,
                recommended_tags=suggested_tags,
            )

        if len(tags) < 2 and not ignore_tag_warning:
            return render_template(
                "edit.html",
                mode="new",
                doc={"title": title, "slug": ""},
                content=content,
                tags_text=", ".join(tags),
                error=None,
                tag_warning="태그를 2개 이상 등록하면 나중에 검색이 더 쉬워집니다. 계속 생성하려면 아래 버튼을 눌러 주세요.",
                show_ignore_tag_warning=True,
                recommended_tags=suggested_tags,
            )

        slug = ensure_unique_slug(conn, slugify(title))
        created_at = now_iso()
        meta = {"sidecar": f"{slug}.json"}
        try:
            conn.execute(
                """
                INSERT INTO docs (title, slug, file_path, meta_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    title,
                    slug,
                    str(document_path(slug)),
                    json.dumps(meta, ensure_ascii=False),
                    created_at,
                    created_at,
                ),
            )
            doc_id = conn.execute("SELECT id FROM docs WHERE slug = ?", (slug,)).fetchone()["id"]

            write_document(slug, content)
            set_doc_tags(conn, doc_id, tags)
            update_fts(fts_conn, doc_id, title, content)
            write_sidecar(
                slug=slug,
                title=title,
                created_at=created_at,
                updated_at=created_at,
                tags=tags,
                meta=meta,
            )
            conn.commit()
            fts_conn.commit()
        except Exception:
            conn.rollback()
            fts_conn.rollback()
            raise
        return redirect(url_for("view_doc", slug=slug))

    prefilled_title = request.args.get("title", "").strip()
    return render_template(
        "edit.html",
        mode="new",
        doc={"title": prefilled_title, "slug": ""},
        content="",
        tags_text="",
        error=None,
        tag_warning=None,
        show_ignore_tag_warning=False,
        recommended_tags=[],
    )


@app.route("/edit/<path:slug>", methods=["GET", "POST"])
def edit_doc(slug: str):
    conn = get_db()
    fts_conn = get_fts_db()
    row = conn.execute("SELECT * FROM docs WHERE slug = ?", (slug,)).fetchone()
    if row is None:
        abort(404)

    doc = dict(row)
    tags = list_doc_tags(conn, row["id"])
    current_content = read_document(row["slug"])

    if request.method == "POST":
        new_title = request.form.get("title", "").strip()
        new_content = request.form.get("content", "")
        new_tags = parse_tags(request.form.get("tags", ""))
        suggested_tags = recommend_tags(
            conn,
            title=new_title or row["title"],
            content=new_content,
            current_slug=row["slug"],
            exclude_tags=new_tags,
            limit=10,
        )

        if not new_title:
            return render_template(
                "edit.html",
                mode="edit",
                doc=doc,
                content=new_content,
                tags_text=", ".join(new_tags),
                error="문서 제목을 입력해 주세요.",
                tag_warning=None,
                show_ignore_tag_warning=False,
                recommended_tags=suggested_tags,
            )

        duplicate = conn.execute(
            "SELECT id FROM docs WHERE title = ? COLLATE NOCASE AND id != ?",
            (new_title, row["id"]),
        ).fetchone()
        if duplicate:
            return render_template(
                "edit.html",
                mode="edit",
                doc=doc,
                content=new_content,
                tags_text=", ".join(new_tags),
                error="같은 제목의 문서가 이미 있습니다.",
                tag_warning=None,
                show_ignore_tag_warning=False,
                recommended_tags=suggested_tags,
            )

        new_slug_candidate = slugify(new_title)
        new_slug = ensure_unique_slug(conn, new_slug_candidate, exclude_doc_id=row["id"])
        old_slug = row["slug"]

        if new_slug != old_slug:
            old_md = document_path(old_slug)
            new_md = document_path(new_slug)
            if old_md.exists():
                old_md.rename(new_md)
            old_json = sidecar_path(old_slug)
            new_json = sidecar_path(new_slug)
            if old_json.exists():
                old_json.rename(new_json)

        updated_at = now_iso()
        meta = safe_load_json(row["meta_json"])
        meta["sidecar"] = f"{new_slug}.json"
        try:
            conn.execute(
                """
                UPDATE docs
                SET title = ?, slug = ?, file_path = ?, meta_json = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    new_title,
                    new_slug,
                    str(document_path(new_slug)),
                    json.dumps(meta, ensure_ascii=False),
                    updated_at,
                    row["id"],
                ),
            )
            write_document(new_slug, new_content)
            set_doc_tags(conn, row["id"], new_tags)
            update_fts(fts_conn, row["id"], new_title, new_content)
            write_sidecar(
                slug=new_slug,
                title=new_title,
                created_at=row["created_at"],
                updated_at=updated_at,
                tags=new_tags,
                meta=meta,
            )
            conn.commit()
            fts_conn.commit()
        except Exception:
            conn.rollback()
            fts_conn.rollback()
            raise
        return redirect(url_for("view_doc", slug=new_slug))

    doc["tags"] = tags
    return render_template(
        "edit.html",
        mode="edit",
        doc=doc,
        content=current_content,
        tags_text=", ".join(tags),
        error=None,
        tag_warning=None,
        show_ignore_tag_warning=False,
        recommended_tags=recommend_tags(
            conn,
            title=doc["title"],
            content=current_content,
            current_slug=doc["slug"],
            exclude_tags=tags,
            limit=10,
        ),
    )


@app.post("/delete/<path:slug>")
def delete_doc(slug: str):
    conn = get_db()
    fts_conn = get_fts_db()
    row = conn.execute("SELECT * FROM docs WHERE slug = ?", (slug,)).fetchone()
    if row is None:
        abort(404)

    try:
        fts_conn.execute("DELETE FROM docs_fts WHERE rowid = ?", (row["id"],))
        conn.execute("DELETE FROM docs WHERE id = ?", (row["id"],))
        conn.commit()
        fts_conn.commit()
    except Exception:
        conn.rollback()
        fts_conn.rollback()
        raise

    md = document_path(slug)
    side = sidecar_path(slug)
    if md.exists():
        md.unlink()
    if side.exists():
        side.unlink()

    return redirect(url_for("index"))


@app.route("/tag/<path:tag_name>")
def docs_by_tag(tag_name: str):
    conn = get_db()
    rows = conn.execute(
        """
        SELECT d.id, d.title, d.slug, d.updated_at
        FROM docs d
        JOIN doc_tags dt ON dt.doc_id = d.id
        JOIN tags t ON t.id = dt.tag_id
        WHERE t.name = ? COLLATE NOCASE
        ORDER BY d.updated_at DESC, d.title COLLATE NOCASE
        """,
        (tag_name,),
    ).fetchall()
    docs = [dict(row) for row in rows]
    return render_template("tag.html", tag_name=tag_name, docs=docs)


@app.route("/search")
def search():
    conn = get_db()
    fts_conn = get_fts_db()
    query = request.args.get("q", "").strip()
    results: list[dict] = []
    error: str | None = None

    if query:
        normalized = normalize_search_query(query)
        try:
            fts_rows = fts_conn.execute(
                """
                SELECT
                    rowid AS doc_id,
                    snippet(docs_fts, 1, '<mark>', '</mark>', ' ... ', 24) AS excerpt
                FROM docs_fts
                WHERE docs_fts MATCH ?
                ORDER BY bm25(docs_fts)
                LIMIT 200
                """,
                (normalized,),
            ).fetchall()

            doc_ids = [int(row["doc_id"]) for row in fts_rows]
            docs_by_id: dict[int, sqlite3.Row] = {}
            if doc_ids:
                placeholders = ",".join("?" for _ in doc_ids)
                meta_rows = conn.execute(
                    f"SELECT id, title, slug, updated_at FROM docs WHERE id IN ({placeholders})",
                    doc_ids,
                ).fetchall()
                docs_by_id = {int(row["id"]): row for row in meta_rows}

            for row in fts_rows:
                doc_id = int(row["doc_id"])
                doc_meta = docs_by_id.get(doc_id)
                if not doc_meta:
                    continue
                results.append(
                    {
                        "title": doc_meta["title"],
                        "slug": doc_meta["slug"],
                        "excerpt": row["excerpt"],
                    }
                )
        except sqlite3.OperationalError:
            error = "검색식이 올바르지 않습니다. 예: flask AND sqlite, python NOT django"

    return render_template(
        "search.html",
        query=query,
        results=results,
        error=error,
    )


@app.post("/preview")
def preview():
    conn = get_db()
    payload = request.get_json(silent=True) or {}
    content = str(payload.get("content", ""))
    html = render_markdown(conn, content)
    return jsonify({"html": str(html)})


@app.post("/api/tag-suggestions")
def tag_suggestions():
    conn = get_db()
    payload = request.get_json(silent=True) or {}

    title = str(payload.get("title", "")).strip()
    content = str(payload.get("content", ""))
    current_slug = str(payload.get("slug", "")).strip() or None

    raw_tags = payload.get("tags", "")
    if isinstance(raw_tags, list):
        tags = parse_tags(",".join(str(item) for item in raw_tags))
    else:
        tags = parse_tags(str(raw_tags))

    suggestions = recommend_tags(
        conn,
        title=title,
        content=content,
        current_slug=current_slug,
        exclude_tags=tags,
        limit=10,
    )
    return jsonify({"tags": suggestions})


@app.route("/img/<path:filename>")
def serve_image(filename: str):
    return send_from_directory(IMG_DIR, filename)


def bootstrap() -> None:
    init_storage()
    init_db()
    init_fts_db()
    sync_documents()
    ensure_default_home()


bootstrap()


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=False)
