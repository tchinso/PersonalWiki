import json
import re
import sqlite3
import sys
import unicodedata
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

import mistune
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
TEMPLATE_DIR = RESOURCE_DIR / "templates"
STATIC_DIR = RESOURCE_DIR / "static"


app = Flask(
    __name__,
    template_folder=str(TEMPLATE_DIR),
    static_folder=str(STATIC_DIR),
)
app.config["JSON_AS_ASCII"] = False

WIKI_LINK_RE = re.compile(r"(?<!\!)\[\[([^\[\]]+)\]\]")
IMAGE_SHORTCUT_RE = re.compile(r"!\[\[([^\[\]]+)\]\]")
TEMPLATE_RE = re.compile(r"\{\{([^{}]+)\}\}")

markdown = mistune.create_markdown(
    escape=False,
    plugins=["strikethrough", "table", "task_lists", "url", "footnotes"],
)


def connect_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def get_db() -> sqlite3.Connection:
    if "db" not in g:
        g.db = connect_db()
    return g.db


@app.teardown_appcontext
def close_db(_error: BaseException | None) -> None:
    conn = g.pop("db", None)
    if conn is not None:
        conn.close()


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

        # FTS5 supports AND/OR/NOT search natively.
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
        if "," in tag:
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


def update_fts(conn: sqlite3.Connection, doc_id: int, title: str, content: str) -> None:
    conn.execute("DELETE FROM docs_fts WHERE rowid = ?", (doc_id,))
    conn.execute(
        "INSERT INTO docs_fts (rowid, title, content) VALUES (?, ?, ?)",
        (doc_id, title, content),
    )


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


def expand_templates(conn: sqlite3.Connection, text: str, depth: int = 0, stack: set[str] | None = None) -> str:
    if stack is None:
        stack = set()
    if depth > 8:
        return f"{text}\n\n> [Template depth limit reached]"

    def repl(match: re.Match[str]) -> str:
        ref = match.group(1).strip()
        slug = resolve_doc_reference(conn, ref)
        if not slug:
            return f"\n\n> [Missing template: {ref}]\n\n"
        if slug in stack:
            return f"\n\n> [Template loop detected: {ref}]\n\n"
        path = document_path(slug)
        if not path.exists():
            return f"\n\n> [Missing template file: {ref}]\n\n"
        template_content = path.read_text(encoding="utf-8")
        return expand_templates(conn, template_content, depth + 1, stack | {slug})

    return TEMPLATE_RE.sub(repl, text)


def preprocess_markup(conn: sqlite3.Connection, text: str) -> str:
    processed = expand_templates(conn, text)

    def image_repl(match: re.Match[str]) -> str:
        raw = match.group(1).strip()
        if "|" in raw:
            filename, alt = [part.strip() for part in raw.split("|", 1)]
        else:
            filename = raw
            alt = Path(filename).stem or "image"
        safe = quote(filename.replace("\\", "/"))
        return f"![{alt}](/img/{safe})"

    processed = IMAGE_SHORTCUT_RE.sub(image_repl, processed)

    def link_repl(match: re.Match[str]) -> str:
        raw = match.group(1).strip()
        if "|" in raw:
            target, label = [part.strip() for part in raw.split("|", 1)]
        else:
            target, label = raw, raw

        slug = resolve_doc_reference(conn, target)
        if slug:
            return f"[{label}](/doc/{quote(slug)})"
        return f"[{label}](/new?title={quote(target)})"

    return WIKI_LINK_RE.sub(link_repl, processed)


def render_markdown(conn: sqlite3.Connection, text: str) -> Markup:
    return Markup(markdown(preprocess_markup(conn, text)))


def normalize_search_query(query: str) -> str:
    query = re.sub(
        r"\b(and|or|not)\b",
        lambda m: m.group(1).upper(),
        query,
        flags=re.IGNORECASE,
    )
    return " ".join(query.split())


def sync_documents() -> None:
    with connect_db() as conn:
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
                # If duplicate title appears from manual file copy, keep importing by suffixing.
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
                update_fts(conn, doc_id, unique_title, content)
                continue

            doc_id = row["id"]
            title = conn.execute("SELECT title FROM docs WHERE id = ?", (doc_id,)).fetchone()["title"]
            update_fts(conn, doc_id, title, content)

        for row in existing_rows:
            if row["slug"] in seen_slugs:
                continue
            conn.execute("DELETE FROM docs_fts WHERE rowid = ?", (row["id"],))
            conn.execute("DELETE FROM docs WHERE id = ?", (row["id"],))


def ensure_default_home() -> None:
    with connect_db() as conn:
        count = conn.execute("SELECT COUNT(*) AS c FROM docs").fetchone()["c"]
        if count > 0:
            return

        title = "Home"
        slug = "home"
        content = """# Home

개인 위키에 오신 것을 환영합니다.

## 기본 문법

- 문서 링크: `[[문서명]]` 또는 `[[문서명|표시 텍스트]]`
- 이미지 삽입: `![[샘플.png]]` (파일은 `/img` 폴더에 저장)
- 템플릿 포함: `{{공통문서}}`
- 태그는 편집 화면에서 콤마(`,`)로 구분해서 입력

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
        update_fts(conn, doc_id, title, content)
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
    return render_template(
        "view.html",
        doc=doc,
        content=content,
        rendered=rendered,
    )


@app.route("/new", methods=["GET", "POST"])
def new_doc():
    conn = get_db()
    if request.method == "POST":
        title = request.form.get("title", "").strip()
        content = request.form.get("content", "")
        tags = parse_tags(request.form.get("tags", ""))

        if not title:
            return render_template(
                "edit.html",
                mode="new",
                doc={"title": "", "slug": ""},
                content=content,
                tags_text=", ".join(tags),
                error="문서 제목을 입력해 주세요.",
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
            )

        slug = ensure_unique_slug(conn, slugify(title))
        created_at = now_iso()
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
                created_at,
                created_at,
            ),
        )
        doc_id = conn.execute("SELECT id FROM docs WHERE slug = ?", (slug,)).fetchone()["id"]

        write_document(slug, content)
        set_doc_tags(conn, doc_id, tags)
        update_fts(conn, doc_id, title, content)
        write_sidecar(
            slug=slug,
            title=title,
            created_at=created_at,
            updated_at=created_at,
            tags=tags,
            meta=meta,
        )
        conn.commit()
        return redirect(url_for("view_doc", slug=slug))

    prefilled_title = request.args.get("title", "").strip()
    return render_template(
        "edit.html",
        mode="new",
        doc={"title": prefilled_title, "slug": ""},
        content="",
        tags_text="",
        error=None,
    )


@app.route("/edit/<path:slug>", methods=["GET", "POST"])
def edit_doc(slug: str):
    conn = get_db()
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

        if not new_title:
            return render_template(
                "edit.html",
                mode="edit",
                doc=doc,
                content=new_content,
                tags_text=", ".join(new_tags),
                error="문서 제목을 입력해 주세요.",
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
        update_fts(conn, row["id"], new_title, new_content)
        write_sidecar(
            slug=new_slug,
            title=new_title,
            created_at=row["created_at"],
            updated_at=updated_at,
            tags=new_tags,
            meta=meta,
        )
        conn.commit()
        return redirect(url_for("view_doc", slug=new_slug))

    doc["tags"] = tags
    return render_template(
        "edit.html",
        mode="edit",
        doc=doc,
        content=current_content,
        tags_text=", ".join(tags),
        error=None,
    )


@app.post("/delete/<path:slug>")
def delete_doc(slug: str):
    conn = get_db()
    row = conn.execute("SELECT * FROM docs WHERE slug = ?", (slug,)).fetchone()
    if row is None:
        abort(404)

    conn.execute("DELETE FROM docs_fts WHERE rowid = ?", (row["id"],))
    conn.execute("DELETE FROM docs WHERE id = ?", (row["id"],))
    conn.commit()

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
    query = request.args.get("q", "").strip()
    results: list[dict] = []
    error: str | None = None

    if query:
        normalized = normalize_search_query(query)
        try:
            rows = conn.execute(
                """
                SELECT
                    d.title,
                    d.slug,
                    snippet(docs_fts, 1, '<mark>', '</mark>', ' ... ', 24) AS excerpt
                FROM docs_fts
                JOIN docs d ON d.id = docs_fts.rowid
                WHERE docs_fts MATCH ?
                ORDER BY bm25(docs_fts), d.updated_at DESC
                LIMIT 200
                """,
                (normalized,),
            ).fetchall()
            results = [dict(row) for row in rows]
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


@app.route("/img/<path:filename>")
def serve_image(filename: str):
    return send_from_directory(IMG_DIR, filename)


def bootstrap() -> None:
    init_storage()
    init_db()
    sync_documents()
    ensure_default_home()


bootstrap()


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=False)
