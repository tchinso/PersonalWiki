from __future__ import annotations

import json
import math
import re
import shutil
import sqlite3
import subprocess
import sys
import threading
import unicodedata
from collections import Counter, defaultdict
from datetime import datetime
from functools import lru_cache
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
FILE_DIR = DATA_DIR / "file"
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


_TAG_RECOMMEND_CACHE_LOCK = threading.Lock()
_TAG_RECOMMEND_CACHE: dict[str, object] = {
    "signature": None,
    "corpus": [],
}


def invalidate_tag_recommendation_cache() -> None:
    with _TAG_RECOMMEND_CACHE_LOCK:
        _TAG_RECOMMEND_CACHE["signature"] = None
        _TAG_RECOMMEND_CACHE["corpus"] = []


DIRTY_MTIME_GRACE_SECONDS = 5.0
SEVERE_DIFF_MIN_CHANGES = 50
SEVERE_DIFF_RATIO = 0.9
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 6885
UNLINKABLE_TITLE_PREFIXES = ("http://", "https://", "file/")
TITLE_LINK_LIMIT_WARNING = (
    "이 문서는 위키 엔진 한계상 링크가 제대로 동작하지 않을 수 있습니다. "
    "제목이 file/, http://, https:// 로 시작하면 위키 링크 해석이 충돌할 수 있습니다."
)


class StartupRecoveryNeeded(RuntimeError):
    pass


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
    FILE_DIR.mkdir(parents=True, exist_ok=True)
    ensure_default_favicon()


def ensure_default_favicon() -> None:
    source = RESOURCE_DIR / "img" / "icon.ico"
    target = IMG_DIR / "icon.ico"
    if target.exists() or not source.exists():
        return
    try:
        shutil.copyfile(source, target)
    except OSError as error:
        print(f"[WARN] failed to copy default favicon: {error}")


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
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS doc_references (
                source_doc_id INTEGER NOT NULL,
                ref_type TEXT NOT NULL CHECK (ref_type IN ('link', 'template')),
                raw_target TEXT NOT NULL,
                target_title_key TEXT NOT NULL,
                target_slug_key TEXT NOT NULL,
                PRIMARY KEY (source_doc_id, ref_type, raw_target),
                FOREIGN KEY (source_doc_id) REFERENCES docs (id) ON DELETE CASCADE
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_docs_slug ON docs (slug)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_doc_tags_tag_id ON doc_tags (tag_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_doc_refs_title_key ON doc_references (target_title_key)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_doc_refs_slug_key ON doc_references (target_slug_key)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_doc_refs_source ON doc_references (source_doc_id)")
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


def ensure_unique_title(conn: sqlite3.Connection, base_title: str, exclude_doc_id: int | None = None) -> str:
    title = base_title.strip() or "untitled"
    candidate = title
    suffix = 2
    while True:
        if exclude_doc_id is None:
            row = conn.execute("SELECT id FROM docs WHERE title = ? COLLATE NOCASE", (candidate,)).fetchone()
        else:
            row = conn.execute(
                "SELECT id FROM docs WHERE title = ? COLLATE NOCASE AND id != ?",
                (candidate, exclude_doc_id),
            ).fetchone()
        if row is None:
            return candidate
        candidate = f"{title} ({suffix})"
        suffix += 1


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


def normalize_newlines(text: str) -> str:
    if not text:
        return ""
    # Repair malformed CRCRLF sequences caused by double newline translation.
    normalized = text.replace("\r\r\n", "\n")
    normalized = normalized.replace("\r\n", "\n").replace("\r", "\n")
    return normalized


def read_text_normalized(path: Path) -> str:
    with path.open("r", encoding="utf-8", newline="") as file:
        return normalize_newlines(file.read())


def read_document(slug: str) -> str:
    path = document_path(slug)
    if not path.exists():
        return ""
    return read_text_normalized(path)


def read_document_if_exists(slug: str) -> str | None:
    path = document_path(slug)
    if not path.exists():
        return None
    return read_text_normalized(path)


def write_document(slug: str, content: str) -> None:
    normalized = normalize_newlines(content)
    document_path(slug).write_text(normalized, encoding="utf-8", newline="\n")


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


def normalize_reference_target(value: str) -> str:
    return unicodedata.normalize("NFKC", str(value)).strip()


def dedupe_reference_targets(values: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for raw in values:
        target = normalize_reference_target(raw)
        if not target:
            continue
        key = target.casefold()
        if key in seen:
            continue
        seen.add(key)
        result.append(target)
    return result


def extract_reference_payload(content: str) -> dict[str, list[str]]:
    wiki_refs, template_refs = extract_reference_targets(content)
    return {
        "links": dedupe_reference_targets(wiki_refs),
        "templates": dedupe_reference_targets(template_refs),
    }


def normalize_reference_payload(references: dict | None) -> dict[str, list[str]]:
    if not isinstance(references, dict):
        return {"links": [], "templates": []}

    links_raw = references.get("links")
    templates_raw = references.get("templates")

    links = dedupe_reference_targets([str(item) for item in links_raw]) if isinstance(links_raw, list) else []
    templates = (
        dedupe_reference_targets([str(item) for item in templates_raw]) if isinstance(templates_raw, list) else []
    )
    return {
        "links": links,
        "templates": templates,
    }


def reference_title_key(value: str) -> str:
    return normalize_reference_target(value).casefold()


def write_sidecar(
    *,
    slug: str,
    title: str,
    created_at: str,
    updated_at: str,
    tags: list[str],
    meta: dict,
    references: dict | None = None,
) -> None:
    normalized_references = normalize_reference_payload(references)
    payload = {
        "title": title,
        "slug": slug,
        "created_at": created_at,
        "updated_at": updated_at,
        "tags": tags,
        "meta": meta,
        "references": normalized_references,
    }
    sidecar_path(slug).write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
        newline="\n",
    )


def build_doc_tag_map(conn: sqlite3.Connection, doc_ids: list[int]) -> dict[int, list[str]]:
    unique_doc_ids: list[int] = []
    seen: set[int] = set()
    for doc_id in doc_ids:
        if doc_id in seen:
            continue
        seen.add(doc_id)
        unique_doc_ids.append(doc_id)
    if not unique_doc_ids:
        return {}

    placeholders = ",".join("?" for _ in unique_doc_ids)
    rows = conn.execute(
        f"""
        SELECT dt.doc_id, t.name
        FROM doc_tags dt
        JOIN tags t ON t.id = dt.tag_id
        WHERE dt.doc_id IN ({placeholders})
        ORDER BY dt.doc_id, t.name COLLATE NOCASE
        """,
        unique_doc_ids,
    ).fetchall()

    mapping: dict[int, list[str]] = defaultdict(list)
    for row in rows:
        mapping[int(row["doc_id"])].append(str(row["name"]))
    return dict(mapping)


def list_doc_tags(conn: sqlite3.Connection, doc_id: int) -> list[str]:
    return build_doc_tag_map(conn, [doc_id]).get(doc_id, [])


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


def collect_sidecar_tags(sidecar: dict) -> list[str]:
    tags_value = sidecar.get("tags")
    if isinstance(tags_value, list):
        return parse_tags(",".join(str(item) for item in tags_value))
    if isinstance(tags_value, str):
        return parse_tags(tags_value)
    return []


def has_unlinkable_title_prefix(title: str) -> bool:
    lowered = title.strip().casefold()
    return any(lowered.startswith(prefix) for prefix in UNLINKABLE_TITLE_PREFIXES)


def title_prefix_warning(title: str) -> str | None:
    if not title:
        return None
    if has_unlinkable_title_prefix(title):
        return TITLE_LINK_LIMIT_WARNING
    return None


def set_doc_tags(conn: sqlite3.Connection, doc_id: int, tags: list[str]) -> None:
    conn.execute("DELETE FROM doc_tags WHERE doc_id = ?", (doc_id,))
    normalized_tags = parse_tags(",".join(str(tag) for tag in tags))
    if normalized_tags:
        conn.executemany(
            "INSERT INTO tags (name) VALUES (?) ON CONFLICT(name) DO NOTHING",
            [(tag,) for tag in normalized_tags],
        )
        name_filters = " OR ".join("name = ? COLLATE NOCASE" for _ in normalized_tags)
        tag_rows = conn.execute(
            f"SELECT id FROM tags WHERE {name_filters}",
            normalized_tags,
        ).fetchall()
        if tag_rows:
            conn.executemany(
                "INSERT OR IGNORE INTO doc_tags (doc_id, tag_id) VALUES (?, ?)",
                [(doc_id, int(row["id"])) for row in tag_rows],
            )
    conn.execute("DELETE FROM tags WHERE id NOT IN (SELECT DISTINCT tag_id FROM doc_tags)")


def set_doc_references(conn: sqlite3.Connection, doc_id: int, references: dict[str, list[str]]) -> None:
    payload = normalize_reference_payload(references)
    conn.execute("DELETE FROM doc_references WHERE source_doc_id = ?", (doc_id,))

    for ref_type, key in (("link", "links"), ("template", "templates")):
        for raw_target in payload[key]:
            conn.execute(
                """
                INSERT OR IGNORE INTO doc_references
                (source_doc_id, ref_type, raw_target, target_title_key, target_slug_key)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    doc_id,
                    ref_type,
                    raw_target,
                    reference_title_key(raw_target),
                    slugify(raw_target),
                ),
            )


def update_fts(fts_conn: sqlite3.Connection, doc_id: int, title: str, content: str) -> None:
    fts_conn.execute("DELETE FROM docs_fts WHERE rowid = ?", (doc_id,))
    fts_conn.execute(
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


def extract_tag_search_terms(query: str) -> list[str]:
    terms: list[str] = []
    seen: set[str] = set()
    for raw in re.split(r"\s+", query):
        token = raw.strip().strip("\"'()")
        token = token.lstrip("#")
        if not token:
            continue
        if token.casefold() in {"and", "or", "not"}:
            continue
        key = token.casefold()
        if key in seen:
            continue
        seen.add(key)
        terms.append(token)
    return terms


def search_docs_by_tags(conn: sqlite3.Connection, terms: list[str], *, limit: int = 200) -> list[dict]:
    if not terms:
        return []

    patterns = [f"%{term}%" for term in terms if term]
    if not patterns:
        return []

    where_clause = " OR ".join("t.name LIKE ? COLLATE NOCASE" for _ in patterns)
    rows = conn.execute(
        f"""
        SELECT
            d.id AS doc_id,
            d.title,
            d.slug,
            d.updated_at,
            GROUP_CONCAT(DISTINCT t.name) AS matched_tags_csv
        FROM docs d
        JOIN doc_tags dt ON dt.doc_id = d.id
        JOIN tags t ON t.id = dt.tag_id
        WHERE {where_clause}
        GROUP BY d.id, d.title, d.slug, d.updated_at
        ORDER BY d.updated_at DESC, d.title COLLATE NOCASE
        LIMIT ?
        """,
        [*patterns, limit],
    ).fetchall()

    hits: list[dict] = []
    for row in rows:
        matched_tags = parse_tags(str(row["matched_tags_csv"] or ""))
        hits.append(
            {
                "doc_id": int(row["doc_id"]),
                "title": str(row["title"]),
                "slug": str(row["slug"]),
                "matched_tags": matched_tags,
            }
        )
    return hits


@lru_cache(maxsize=8192)
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


def build_tag_recommendation_signature(
    conn: sqlite3.Connection,
    fts_conn: sqlite3.Connection,
) -> tuple[int, str, int, int]:
    row = conn.execute(
        """
        SELECT
            (SELECT COUNT(*) FROM docs) AS docs_count,
            (SELECT COALESCE(MAX(updated_at), '') FROM docs) AS docs_max_updated_at,
            (SELECT COUNT(*) FROM doc_tags) AS doc_tag_count
        """
    ).fetchone()
    if row is None:
        return (0, "", 0, 0)
    fts_count_row = fts_conn.execute("SELECT COUNT(*) AS c FROM docs_fts").fetchone()
    fts_count = int(fts_count_row["c"]) if fts_count_row is not None else 0
    return (
        int(row["docs_count"]),
        str(row["docs_max_updated_at"]),
        int(row["doc_tag_count"]),
        fts_count,
    )


def build_tag_recommendation_corpus(
    conn: sqlite3.Connection,
    fts_conn: sqlite3.Connection,
) -> list[dict[str, object]]:
    signature = build_tag_recommendation_signature(conn, fts_conn)
    with _TAG_RECOMMEND_CACHE_LOCK:
        cached_signature = _TAG_RECOMMEND_CACHE.get("signature")
        cached_corpus = _TAG_RECOMMEND_CACHE.get("corpus")
        if cached_signature == signature and isinstance(cached_corpus, list):
            return cached_corpus

    rows = conn.execute("SELECT id, title, slug FROM docs").fetchall()
    if not rows:
        with _TAG_RECOMMEND_CACHE_LOCK:
            _TAG_RECOMMEND_CACHE["signature"] = signature
            _TAG_RECOMMEND_CACHE["corpus"] = []
        return []

    doc_ids = [int(row["id"]) for row in rows]
    tag_map = build_doc_tag_map(conn, doc_ids)
    fts_rows = fts_conn.execute("SELECT rowid AS doc_id, content FROM docs_fts").fetchall()
    content_map = {int(row["doc_id"]): str(row["content"] or "") for row in fts_rows}

    corpus: list[dict[str, object]] = []
    for row in rows:
        doc_id = int(row["id"])
        doc_tokens = tokenize_text(f"{row['title']}\n{content_map.get(doc_id, '')}")
        if not doc_tokens:
            continue
        tf_counter = Counter(doc_tokens)
        corpus.append(
            {
                "slug": str(row["slug"]),
                "tags": tag_map.get(doc_id, []),
                "tf_counter": tf_counter,
                "token_set": set(tf_counter.keys()),
            }
        )

    with _TAG_RECOMMEND_CACHE_LOCK:
        _TAG_RECOMMEND_CACHE["signature"] = signature
        _TAG_RECOMMEND_CACHE["corpus"] = corpus
    return corpus


def recommend_tags(
    conn: sqlite3.Connection,
    fts_conn: sqlite3.Connection,
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

    base_corpus = build_tag_recommendation_corpus(conn, fts_conn)
    corpus = [
        doc
        for doc in base_corpus
        if not current_slug or str(doc["slug"]) != current_slug
    ]
    if not corpus:
        return []

    df_counter: Counter[str] = Counter()
    for doc in corpus:
        for token in doc["token_set"]:
            df_counter[token] += 1

    total_docs = len(corpus)
    query_vec = build_tfidf_vector(Counter(query_tokens), df_counter, total_docs)
    if not query_vec:
        return []

    scored_docs: list[tuple[float, list[str]]] = []
    for doc in corpus:
        doc_vec = build_tfidf_vector(doc["tf_counter"], df_counter, total_docs)
        similarity = cosine_similarity(query_vec, doc_vec)
        if similarity > 0:
            scored_docs.append((similarity, list(doc["tags"])))
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
    target_row = conn.execute("SELECT title, slug FROM docs WHERE slug = ?", (target_slug,)).fetchone()
    if target_row is None:
        return []

    title_key = reference_title_key(target_row["title"])
    slug_key = str(target_row["slug"])

    rows = conn.execute(
        """
        SELECT
            d.id,
            d.title,
            d.slug,
            d.updated_at,
            MAX(CASE WHEN r.ref_type = 'link' THEN 1 ELSE 0 END) AS has_link,
            MAX(CASE WHEN r.ref_type = 'template' THEN 1 ELSE 0 END) AS has_template
        FROM doc_references r
        JOIN docs d ON d.id = r.source_doc_id
        WHERE d.slug != ?
          AND (r.target_title_key = ? OR r.target_slug_key = ?)
        GROUP BY d.id, d.title, d.slug, d.updated_at
        ORDER BY d.updated_at DESC, d.title COLLATE NOCASE
        """,
        (target_slug, title_key, slug_key),
    ).fetchall()

    backlinks: list[dict] = []
    for row in rows:
        reasons: list[str] = []
        if row["has_link"]:
            reasons.append("link")
        if row["has_template"]:
            reasons.append("template")

        item = dict(row)
        item.pop("has_link", None)
        item.pop("has_template", None)
        item["reasons"] = reasons
        backlinks.append(item)
    return backlinks


def iso_from_timestamp(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp).isoformat(timespec="seconds")


def timestamp_from_iso(value: str | None) -> float:
    if not value:
        return 0.0
    try:
        return datetime.fromisoformat(str(value)).timestamp()
    except ValueError:
        return 0.0


def build_markdown_snapshot() -> dict[str, dict[str, object]]:
    snapshot: dict[str, dict[str, object]] = {}
    for md_file in sorted(DOC_DIR.glob("*.md")):
        try:
            mtime = float(md_file.stat().st_mtime)
        except OSError:
            continue
        snapshot[md_file.stem] = {
            "path": md_file,
            "mtime": mtime,
        }
    return snapshot


def build_db_snapshot(conn: sqlite3.Connection) -> dict[str, sqlite3.Row]:
    rows = conn.execute("SELECT id, slug, title, created_at, updated_at, meta_json FROM docs").fetchall()
    return {row["slug"]: row for row in rows}


def detect_incremental_changes(
    md_snapshot: dict[str, dict[str, object]],
    db_snapshot: dict[str, sqlite3.Row],
) -> tuple[list[str], list[str], list[str]]:
    md_slugs = set(md_snapshot.keys())
    db_slugs = set(db_snapshot.keys())

    new_slugs = sorted(md_slugs - db_slugs)
    deleted_slugs = sorted(db_slugs - md_slugs)

    modified_slugs: list[str] = []
    for slug in sorted(md_slugs & db_slugs):
        md_mtime = float(md_snapshot[slug]["mtime"])
        db_updated_ts = timestamp_from_iso(str(db_snapshot[slug]["updated_at"]))
        if md_mtime > (db_updated_ts + DIRTY_MTIME_GRACE_SECONDS):
            modified_slugs.append(slug)
    return new_slugs, deleted_slugs, modified_slugs


def is_severe_divergence(*, md_count: int, db_count: int, total_changes: int) -> bool:
    if total_changes < SEVERE_DIFF_MIN_CHANGES:
        return False
    baseline = max(md_count, db_count, 1)
    return (total_changes / baseline) >= SEVERE_DIFF_RATIO


def sync_new_doc(
    conn: sqlite3.Connection,
    fts_conn: sqlite3.Connection,
    *,
    slug: str,
    md_file: Path,
    mtime: float,
) -> None:
    content = read_text_normalized(md_file)
    references = extract_reference_payload(content)
    sidecar = load_sidecar(slug)

    sidecar_title = str(sidecar.get("title", "")).strip()
    title_candidate = sidecar_title or infer_title_from_content(content, slug) or slug
    title = ensure_unique_title(conn, title_candidate)

    created_at_raw = str(sidecar.get("created_at") or "").strip()
    created_at = created_at_raw or iso_from_timestamp(mtime)
    updated_at = iso_from_timestamp(mtime)

    meta_value = sidecar.get("meta")
    meta = meta_value if isinstance(meta_value, dict) else {}
    meta["sidecar"] = f"{slug}.json"

    conn.execute(
        """
        INSERT INTO docs (title, slug, file_path, meta_json, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            title,
            slug,
            str(md_file),
            json.dumps(meta, ensure_ascii=False),
            created_at,
            updated_at,
        ),
    )
    doc_row = conn.execute("SELECT id FROM docs WHERE slug = ?", (slug,)).fetchone()
    if doc_row is None:
        raise RuntimeError(f"failed to create doc row for slug '{slug}'")
    doc_id = int(doc_row["id"])

    tags = collect_sidecar_tags(sidecar)

    set_doc_tags(conn, doc_id, tags)
    set_doc_references(conn, doc_id, references)
    update_fts(fts_conn, doc_id, title, content)
    write_sidecar(
        slug=slug,
        title=title,
        created_at=created_at,
        updated_at=updated_at,
        tags=tags,
        meta=meta,
        references=references,
    )


def sync_modified_doc(
    conn: sqlite3.Connection,
    fts_conn: sqlite3.Connection,
    *,
    row: sqlite3.Row,
    md_file: Path,
    mtime: float,
) -> None:
    slug = str(row["slug"])
    doc_id = int(row["id"])
    content = read_text_normalized(md_file)
    references = extract_reference_payload(content)

    current_title = str(row["title"] or "").strip()
    if not current_title:
        inferred = infer_title_from_content(content, slug) or slug
        current_title = ensure_unique_title(conn, inferred, exclude_doc_id=doc_id)

    meta = safe_load_json(row["meta_json"])
    meta["sidecar"] = f"{slug}.json"
    updated_at = iso_from_timestamp(mtime)

    conn.execute(
        """
        UPDATE docs
        SET title = ?, file_path = ?, meta_json = ?, updated_at = ?
        WHERE id = ?
        """,
        (
            current_title,
            str(md_file),
            json.dumps(meta, ensure_ascii=False),
            updated_at,
            doc_id,
        ),
    )
    tags = list_doc_tags(conn, doc_id)
    set_doc_references(conn, doc_id, references)
    update_fts(fts_conn, doc_id, current_title, content)
    write_sidecar(
        slug=slug,
        title=current_title,
        created_at=row["created_at"],
        updated_at=updated_at,
        tags=tags,
        meta=meta,
        references=references,
    )


def sync_deleted_doc(conn: sqlite3.Connection, fts_conn: sqlite3.Connection, *, row: sqlite3.Row) -> None:
    doc_id = int(row["id"])
    fts_conn.execute("DELETE FROM docs_fts WHERE rowid = ?", (doc_id,))
    conn.execute("DELETE FROM docs WHERE id = ?", (doc_id,))


def repair_fts_mismatch(conn: sqlite3.Connection, fts_conn: sqlite3.Connection) -> tuple[int, int]:
    doc_rows = conn.execute("SELECT id, title, slug FROM docs").fetchall()
    fts_rows = fts_conn.execute("SELECT rowid FROM docs_fts").fetchall()

    docs_by_id = {int(row["id"]): row for row in doc_rows}
    doc_ids = set(docs_by_id.keys())
    fts_ids = {int(row["rowid"]) for row in fts_rows}

    missing_ids = sorted(doc_ids - fts_ids)
    orphan_ids = sorted(fts_ids - doc_ids)

    for doc_id in missing_ids:
        row = docs_by_id[doc_id]
        content = read_document_if_exists(str(row["slug"])) or ""
        update_fts(fts_conn, doc_id, str(row["title"]), content)

    for doc_id in orphan_ids:
        fts_conn.execute("DELETE FROM docs_fts WHERE rowid = ?", (doc_id,))

    return len(missing_ids), len(orphan_ids)


def sync_documents_incremental() -> dict[str, int]:
    with connect_db() as conn, connect_fts_db() as fts_conn:
        md_snapshot = build_markdown_snapshot()
        db_snapshot = build_db_snapshot(conn)
        new_slugs, deleted_slugs, modified_slugs = detect_incremental_changes(md_snapshot, db_snapshot)

        total_changes = len(new_slugs) + len(deleted_slugs) + len(modified_slugs)
        if len(md_snapshot) != len(db_snapshot):
            print(f"[SYNC] count mismatch detected md={len(md_snapshot)} db={len(db_snapshot)}")
        if is_severe_divergence(
            md_count=len(md_snapshot),
            db_count=len(db_snapshot),
            total_changes=total_changes,
        ):
            raise StartupRecoveryNeeded(
                "severe startup divergence detected "
                f"(md={len(md_snapshot)}, db={len(db_snapshot)}, changes={total_changes})"
            )

        for slug in new_slugs:
            md_file = md_snapshot[slug]["path"]
            mtime = float(md_snapshot[slug]["mtime"])
            sync_new_doc(conn, fts_conn, slug=slug, md_file=md_file, mtime=mtime)

        for slug in modified_slugs:
            md_file = md_snapshot[slug]["path"]
            mtime = float(md_snapshot[slug]["mtime"])
            row = db_snapshot[slug]
            sync_modified_doc(conn, fts_conn, row=row, md_file=md_file, mtime=mtime)

        for slug in deleted_slugs:
            sync_deleted_doc(conn, fts_conn, row=db_snapshot[slug])

        docs_count = int(conn.execute("SELECT COUNT(*) AS c FROM docs").fetchone()["c"])
        fts_count = int(fts_conn.execute("SELECT COUNT(*) AS c FROM docs_fts").fetchone()["c"])
        fts_missing = 0
        fts_orphan = 0
        if docs_count != fts_count:
            fts_missing, fts_orphan = repair_fts_mismatch(conn, fts_conn)

        conn.commit()
        fts_conn.commit()
        if total_changes > 0 or fts_missing > 0 or fts_orphan > 0:
            invalidate_tag_recommendation_cache()

        print(
            "[SYNC] startup incremental sync "
            f"new={len(new_slugs)} deleted={len(deleted_slugs)} modified={len(modified_slugs)} "
            f"fts_missing={fts_missing} fts_orphan={fts_orphan}"
        )
        return {
            "new": len(new_slugs),
            "deleted": len(deleted_slugs),
            "modified": len(modified_slugs),
            "fts_missing": fts_missing,
            "fts_orphan": fts_orphan,
        }


def resolve_db_fix_command() -> list[str] | None:
    if getattr(sys, "frozen", False):
        exe_path = Path(sys.executable).resolve().parent / "PersonalWikiDBFix.exe"
        if exe_path.exists():
            return [str(exe_path)]
        return None

    script_path = DATA_DIR / "personal_wiki_db_fix.py"
    if script_path.exists():
        return [sys.executable, str(script_path)]

    exe_path = DATA_DIR / "PersonalWikiDBFix.exe"
    if exe_path.exists():
        return [str(exe_path)]
    return None


def run_db_fix_tool(reason: str) -> bool:
    command = resolve_db_fix_command()
    if command is None:
        print(f"[ERROR] DB fix tool not found. reason={reason}")
        return False

    print(f"[WARN] Running PersonalWikiDBFix due to startup issue: {reason}")
    try:
        completed = subprocess.run(
            command,
            cwd=str(DATA_DIR),
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError as error:
        print(f"[ERROR] Failed to launch PersonalWikiDBFix: {error}")
        return False

    if completed.stdout:
        print(completed.stdout.rstrip())
    if completed.stderr:
        print(completed.stderr.rstrip())

    if completed.returncode != 0:
        print(f"[ERROR] PersonalWikiDBFix failed with exit code {completed.returncode}")
        return False
    return True


def sync_documents_on_startup() -> None:
    try:
        sync_documents_incremental()
        return
    except StartupRecoveryNeeded as error:
        reason = str(error)
    except sqlite3.DatabaseError as error:
        reason = f"database error: {error}"

    if not run_db_fix_tool(reason):
        raise RuntimeError(f"startup sync failed and DB fix did not complete: {reason}")
    print("[SYNC] Startup recovery completed by PersonalWikiDBFix.")


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
        references = extract_reference_payload(content)
        write_document(slug, content)
        set_doc_tags(conn, doc_id, tags)
        set_doc_references(conn, doc_id, references)
        update_fts(fts_conn, doc_id, title, content)
        write_sidecar(
            slug=slug,
            title=title,
            created_at=now,
            updated_at=now,
            tags=tags,
            meta=meta,
            references=references,
        )
        invalidate_tag_recommendation_cache()


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
    tag_map = build_doc_tag_map(conn, [int(row["id"]) for row in rows])
    docs: list[dict] = []
    for row in rows:
        item = dict(row)
        item["tags"] = tag_map.get(int(row["id"]), [])
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
        content = normalize_newlines(request.form.get("content", ""))
        tags = parse_tags(request.form.get("tags", ""))
        ignore_tag_warning = request.form.get("ignore_tag_warning") == "1"
        suggested_tags = recommend_tags(
            conn,
            fts_conn,
            title=title,
            content=content,
            exclude_tags=tags,
            limit=10,
        )
        title_warning = title_prefix_warning(title)

        if not title:
            return render_template(
                "edit.html",
                mode="new",
                doc={"title": "", "slug": ""},
                content=content,
                tags_text=", ".join(tags),
                error="문서 제목을 입력해 주세요.",
                tag_warning=None,
                title_warning=title_warning,
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
                title_warning=title_warning,
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
                title_warning=title_warning,
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

            references = extract_reference_payload(content)
            write_document(slug, content)
            set_doc_tags(conn, doc_id, tags)
            set_doc_references(conn, doc_id, references)
            update_fts(fts_conn, doc_id, title, content)
            write_sidecar(
                slug=slug,
                title=title,
                created_at=created_at,
                updated_at=created_at,
                tags=tags,
                meta=meta,
                references=references,
            )
            conn.commit()
            fts_conn.commit()
            invalidate_tag_recommendation_cache()
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
        title_warning=title_prefix_warning(prefilled_title),
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
        new_content = normalize_newlines(request.form.get("content", ""))
        new_tags = parse_tags(request.form.get("tags", ""))
        suggested_tags = recommend_tags(
            conn,
            fts_conn,
            title=new_title or row["title"],
            content=new_content,
            current_slug=row["slug"],
            exclude_tags=new_tags,
            limit=10,
        )
        title_warning = title_prefix_warning(new_title)

        if not new_title:
            return render_template(
                "edit.html",
                mode="edit",
                doc=doc,
                content=new_content,
                tags_text=", ".join(new_tags),
                error="문서 제목을 입력해 주세요.",
                tag_warning=None,
                title_warning=title_warning,
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
                title_warning=title_warning,
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
            references = extract_reference_payload(new_content)
            write_document(new_slug, new_content)
            set_doc_tags(conn, row["id"], new_tags)
            set_doc_references(conn, row["id"], references)
            update_fts(fts_conn, row["id"], new_title, new_content)
            write_sidecar(
                slug=new_slug,
                title=new_title,
                created_at=row["created_at"],
                updated_at=updated_at,
                tags=new_tags,
                meta=meta,
                references=references,
            )
            conn.commit()
            fts_conn.commit()
            invalidate_tag_recommendation_cache()
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
        title_warning=title_prefix_warning(doc["title"]),
        show_ignore_tag_warning=False,
        recommended_tags=recommend_tags(
            conn,
            fts_conn,
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
        invalidate_tag_recommendation_cache()
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
        merged_by_doc_id: dict[int, dict] = {}
        ordered_doc_ids: list[int] = []
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
                merged_by_doc_id[doc_id] = {
                    "title": doc_meta["title"],
                    "slug": doc_meta["slug"],
                    "excerpt": row["excerpt"],
                    "matched_tags": [],
                }
                ordered_doc_ids.append(doc_id)
        except sqlite3.OperationalError:
            error = "검색식이 올바르지 않습니다. 예: flask AND sqlite, python NOT django"

        tag_terms = extract_tag_search_terms(query)
        for hit in search_docs_by_tags(conn, tag_terms, limit=200):
            doc_id = int(hit["doc_id"])
            if doc_id in merged_by_doc_id:
                existing = merged_by_doc_id[doc_id]
                existing_tags = [str(tag) for tag in existing.get("matched_tags", [])]
                existing["matched_tags"] = parse_tags(",".join([*existing_tags, *hit["matched_tags"]]))
                continue

            merged_by_doc_id[doc_id] = {
                "title": hit["title"],
                "slug": hit["slug"],
                "excerpt": "",
                "matched_tags": hit["matched_tags"],
            }
            ordered_doc_ids.append(doc_id)

        results = [merged_by_doc_id[doc_id] for doc_id in ordered_doc_ids]

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
    content = normalize_newlines(str(payload.get("content", "")))
    html = render_markdown(conn, content)
    return jsonify({"html": str(html)})


@app.post("/api/tag-suggestions")
def tag_suggestions():
    conn = get_db()
    fts_conn = get_fts_db()
    payload = request.get_json(silent=True) or {}

    title = str(payload.get("title", "")).strip()
    content = normalize_newlines(str(payload.get("content", "")))
    current_slug = str(payload.get("slug", "")).strip() or None

    raw_tags = payload.get("tags", "")
    if isinstance(raw_tags, list):
        tags = parse_tags(",".join(str(item) for item in raw_tags))
    else:
        tags = parse_tags(str(raw_tags))

    suggestions = recommend_tags(
        conn,
        fts_conn,
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


@app.route("/file/<path:filename>")
def serve_file(filename: str):
    return send_from_directory(FILE_DIR, filename)


@app.route("/favicon.ico")
def favicon():
    return send_from_directory(IMG_DIR, "icon.ico")


def bootstrap() -> None:
    init_storage()
    init_db()
    init_fts_db()
    sync_documents_on_startup()
    ensure_default_home()


bootstrap()


if __name__ == "__main__":
    app.run(host=DEFAULT_HOST, port=DEFAULT_PORT, debug=False)

