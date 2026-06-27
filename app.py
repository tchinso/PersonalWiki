from __future__ import annotations

import json
import atexit
import base64
import io
import mimetypes
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import unicodedata
import zipfile
from collections import defaultdict
from contextlib import closing
from datetime import datetime
from pathlib import Path, PurePosixPath
from urllib.parse import quote, unquote, urlsplit

from flask import (
    Flask,
    abort,
    g,
    jsonify,
    redirect,
    render_template,
    request,
    send_file,
    send_from_directory,
    url_for,
)
from markupsafe import Markup

from markdown_engine import MarkdownEngine, TOC_RESERVED_TITLES, extract_reference_targets
from language_tools import (
    TAG_RECOMMEND_LIMIT,
    apply_korean_spell_autofix,
    build_language_index_source_signature,
    collect_korean_spell_issues,
    delete_language_doc_tokens,
    ensure_language_token_index_current,
    ensure_language_token_tables,
    finalize_language_token_batch,
    invalidate_tag_recommendation_cache,
    korean_spell_warning_message,
    language_token_index_needs_rebuild,
    recommend_tags,
    rebuild_language_token_index,
    upsert_language_doc_tokens,
)


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
JSON_DIR = DOC_DIR / "json"
IMG_DIR = DATA_DIR / "img"
FILE_DIR = DATA_DIR / "file"
DB_PATH = DATA_DIR / "wiki.db"
FTS_DB_PATH = DATA_DIR / "wiki_fts.db"
TOKEN_DB_PATH = DATA_DIR / "wiki_token.db"
DATA_LOCK_PATH = DATA_DIR / "wiki.lock"
TEMPLATE_DIR = RESOURCE_DIR / "templates"
STATIC_DIR = RESOURCE_DIR / "static"
_DATA_LOCK_FILE = None


app = Flask(
    __name__,
    template_folder=str(TEMPLATE_DIR),
    static_folder=str(STATIC_DIR),
)
app.config["JSON_AS_ASCII"] = False

markdown_engine = MarkdownEngine()

DIRTY_MTIME_GRACE_SECONDS = 1.0
SEVERE_DIFF_MIN_CHANGES = 50
SEVERE_DIFF_RATIO = 0.9
SQLITE_IN_CLAUSE_CHUNK_SIZE = 400
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 6885
SETTINGS_PATH = DATA_DIR / "wikisettings.cfg"
LOCAL_ASSET_URL_RE = re.compile(
    r"(?P<prefix>\b(?:src|href)\s*=\s*)(?P<quote>['\"])(?P<url>/(?:img|file)/[^'\"?#]*)(?P=quote)",
    flags=re.IGNORECASE,
)
FILE_REFERENCE_RE = re.compile(r"\[\[\s*file/", flags=re.IGNORECASE)
UNLINKABLE_TITLE_PREFIXES = ("img/", "http://", "https://", "file/")
UNLINKABLE_TITLE_NAMES = tuple(title.casefold() for title in TOC_RESERVED_TITLES)
TITLE_LINK_LIMIT_WARNING = (
    "이 문서는 위키 엔진 한계상 링크 또는 템플릿 해석이 제대로 동작하지 않을 수 있습니다. "
    "제목이 img/, file/, http://, https:// 로 시작하거나 TOC, TOC1~TOC6이면 내장 문법과 충돌할 수 있습니다."
)


class StartupRecoveryNeeded(RuntimeError):
    pass


class ExportError(ValueError):
    pass


def read_server_port(settings_path: Path = SETTINGS_PATH) -> int:
    """Read port=<number> from wikisettings.cfg, falling back safely."""
    try:
        raw = settings_path.read_text(encoding="utf-8-sig")
    except (OSError, UnicodeError):
        return DEFAULT_PORT

    for raw_line in raw.splitlines():
        line = raw_line.strip()
        if not line or line.startswith(("#", ";")) or "=" not in line:
            continue
        key, value = (part.strip() for part in line.split("=", 1))
        if key.casefold() != "port" or not value.isascii() or not value.isdigit():
            continue
        port = int(value)
        if 1 <= port <= 65535:
            return port
    return DEFAULT_PORT


def _lock_file_handle(handle) -> None:
    handle.seek(0)
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        return

    import fcntl

    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)


def _unlock_file_handle(handle) -> None:
    handle.seek(0)
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        return

    import fcntl

    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def acquire_data_lock() -> None:
    global _DATA_LOCK_FILE
    if _DATA_LOCK_FILE is not None:
        return

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    handle = DATA_LOCK_PATH.open("a+b")
    try:
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"\0")
            handle.flush()
        _lock_file_handle(handle)
    except OSError as error:
        handle.close()
        raise RuntimeError(
            "PersonalWiki 데이터베이스가 이미 다른 프로세스에서 사용 중입니다. "
            "실행 중인 PersonalWiki 또는 DBFix를 종료한 뒤 다시 시작해 주세요."
        ) from error

    _DATA_LOCK_FILE = handle
    atexit.register(release_data_lock)


def release_data_lock() -> None:
    global _DATA_LOCK_FILE
    handle = _DATA_LOCK_FILE
    if handle is None:
        return
    _DATA_LOCK_FILE = None
    try:
        _unlock_file_handle(handle)
    except OSError:
        pass
    handle.close()


def configure_sqlite_connection(conn: sqlite3.Connection, *, foreign_keys: bool) -> None:
    conn.execute("PRAGMA busy_timeout = 5000")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA wal_autocheckpoint = 1000")
    conn.execute("PRAGMA temp_store = MEMORY")
    conn.execute("PRAGMA cache_size = -20000")
    if foreign_keys:
        conn.execute("PRAGMA foreign_keys = ON")


def connect_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    configure_sqlite_connection(conn, foreign_keys=True)
    return conn


def connect_fts_db() -> sqlite3.Connection:
    conn = sqlite3.connect(FTS_DB_PATH)
    conn.row_factory = sqlite3.Row
    configure_sqlite_connection(conn, foreign_keys=False)
    return conn


def connect_token_db() -> sqlite3.Connection:
    conn = sqlite3.connect(TOKEN_DB_PATH)
    conn.row_factory = sqlite3.Row
    configure_sqlite_connection(conn, foreign_keys=False)
    return conn


def get_db() -> sqlite3.Connection:
    if "db" not in g:
        g.db = connect_db()
    return g.db


def get_fts_db() -> sqlite3.Connection:
    if "fts_db" not in g:
        g.fts_db = connect_fts_db()
    return g.fts_db


def get_token_db() -> sqlite3.Connection:
    if "token_db" not in g:
        g.token_db = connect_token_db()
    return g.token_db


@app.teardown_appcontext
def close_db(_error: BaseException | None) -> None:
    conn = g.pop("db", None)
    if conn is not None:
        conn.close()
    fts_conn = g.pop("fts_db", None)
    if fts_conn is not None:
        fts_conn.close()
    token_conn = g.pop("token_db", None)
    if token_conn is not None:
        token_conn.close()


def init_storage() -> None:
    DOC_DIR.mkdir(parents=True, exist_ok=True)
    JSON_DIR.mkdir(parents=True, exist_ok=True)
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
    with closing(connect_db()) as conn:
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
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_doc_refs_title_lookup "
            "ON doc_references (target_title_key, source_doc_id, ref_type)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_doc_refs_slug_lookup "
            "ON doc_references (target_slug_key, source_doc_id, ref_type)"
        )
        # Legacy cleanup: older versions stored docs_fts in wiki.db.
        conn.execute("DROP TABLE IF EXISTS docs_fts")
        conn.commit()


def init_fts_db() -> None:
    with closing(connect_fts_db()) as conn:
        conn.execute(
            """
            CREATE VIRTUAL TABLE IF NOT EXISTS docs_fts
            USING fts5(title, content)
            """
        )
        ensure_fts_index_meta_table(conn)
        conn.commit()


def init_token_db() -> None:
    with closing(connect_token_db()) as conn:
        ensure_language_token_tables(conn)
        conn.commit()


def ensure_fts_index_meta_table(fts_conn: sqlite3.Connection) -> None:
    fts_conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fts_index_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """
    )


def get_fts_index_meta(fts_conn: sqlite3.Connection, key: str) -> str | None:
    ensure_fts_index_meta_table(fts_conn)
    row = fts_conn.execute(
        "SELECT value FROM fts_index_meta WHERE key = ?",
        (key,),
    ).fetchone()
    return str(row["value"]) if row is not None else None


def set_fts_index_meta(fts_conn: sqlite3.Connection, key: str, value: object) -> None:
    ensure_fts_index_meta_table(fts_conn)
    fts_conn.execute(
        """
        INSERT INTO fts_index_meta (key, value)
        VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        (key, str(value)),
    )


def mark_fts_index_current(conn: sqlite3.Connection, fts_conn: sqlite3.Connection) -> None:
    set_fts_index_meta(
        fts_conn,
        "source_signature",
        build_language_index_source_signature(conn),
    )


def fts_index_needs_rebuild(conn: sqlite3.Connection, fts_conn: sqlite3.Connection) -> bool:
    stored_signature = get_fts_index_meta(fts_conn, "source_signature")
    if stored_signature is None:
        return False
    return stored_signature != build_language_index_source_signature(conn)


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
    return JSON_DIR / f"{slug}.json"


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


def move_document_assets_for_slug_change(old_slug: str, new_slug: str) -> list[tuple[Path, Path]]:
    moved: list[tuple[Path, Path]] = []
    for old_path, new_path in (
        (document_path(old_slug), document_path(new_slug)),
        (sidecar_path(old_slug), sidecar_path(new_slug)),
    ):
        if not old_path.exists():
            continue
        if new_path.exists():
            raise FileExistsError(f"target document asset already exists: {new_path}")
        old_path.rename(new_path)
        moved.append((old_path, new_path))
    return moved


def rollback_document_asset_moves(moved: list[tuple[Path, Path]]) -> None:
    for old_path, new_path in reversed(moved):
        try:
            if new_path.exists() and not old_path.exists():
                new_path.rename(old_path)
        except OSError as error:
            print(f"[WARN] failed to rollback document asset move {new_path} -> {old_path}: {error}")


def load_sidecar(slug: str) -> dict:
    path = sidecar_path(slug)
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            return data
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
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


def canonical_reference_payload(references: dict | None) -> dict[str, list[str]]:
    payload = normalize_reference_payload(references)
    return {
        "links": sorted(payload["links"], key=lambda item: item.casefold()),
        "templates": sorted(payload["templates"], key=lambda item: item.casefold()),
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


def build_doc_reference_map(conn: sqlite3.Connection, doc_ids: list[int]) -> dict[int, dict[str, list[str]]]:
    unique_doc_ids: list[int] = []
    seen: set[int] = set()
    for doc_id in doc_ids:
        if doc_id in seen:
            continue
        seen.add(doc_id)
        unique_doc_ids.append(doc_id)
    if not unique_doc_ids:
        return {}

    mapping: dict[int, dict[str, list[str]]] = {
        doc_id: {"links": [], "templates": []}
        for doc_id in unique_doc_ids
    }
    for chunk in iter_sqlite_chunks([str(doc_id) for doc_id in unique_doc_ids]):
        placeholders = ",".join("?" for _ in chunk)
        rows = conn.execute(
            f"""
            SELECT source_doc_id, ref_type, raw_target
            FROM doc_references
            WHERE source_doc_id IN ({placeholders})
            ORDER BY source_doc_id, ref_type, raw_target COLLATE NOCASE
            """,
            chunk,
        ).fetchall()
        for row in rows:
            doc_id = int(row["source_doc_id"])
            key = "templates" if row["ref_type"] == "template" else "links"
            mapping.setdefault(doc_id, {"links": [], "templates": []})[key].append(str(row["raw_target"]))
    return mapping


def sidecar_matches_doc_row(row: sqlite3.Row, sidecar: dict, tags: list[str], meta: dict) -> bool:
    if not sidecar:
        return False
    if str(sidecar.get("title") or "") != str(row["title"]):
        return False
    if str(sidecar.get("slug") or "") != str(row["slug"]):
        return False
    if str(sidecar.get("created_at") or "") != str(row["created_at"]):
        return False
    if str(sidecar.get("updated_at") or "") != str(row["updated_at"]):
        return False
    if collect_sidecar_tags(sidecar) != tags:
        return False

    sidecar_meta = sidecar.get("meta")
    if not isinstance(sidecar_meta, dict):
        return False
    expected_meta = dict(meta)
    expected_meta["sidecar"] = f"json/{row['slug']}.json"
    if sidecar_meta != expected_meta:
        return False

    return True


def repair_sidecar_mismatches(conn: sqlite3.Connection) -> int:
    rows = conn.execute("SELECT id, slug, title, created_at, updated_at, meta_json FROM docs").fetchall()
    repaired = 0
    doc_ids = [int(row["id"]) for row in rows]
    tag_map = build_doc_tag_map(conn, doc_ids)
    reference_map = build_doc_reference_map(conn, doc_ids)
    for row in rows:
        slug = str(row["slug"])
        sidecar = load_sidecar(slug)
        doc_id = int(row["id"])
        tags = tag_map.get(int(row["id"]), [])
        meta = safe_load_json(row["meta_json"])
        meta["sidecar"] = f"json/{slug}.json"
        expected_references = reference_map.get(doc_id, {"links": [], "templates": []})
        sidecar_ok = (
            sidecar_matches_doc_row(row, sidecar, tags, meta)
            and canonical_reference_payload(sidecar.get("references")) == canonical_reference_payload(expected_references)
        )
        if sidecar_ok:
            continue

        write_sidecar(
            slug=slug,
            title=str(row["title"]),
            created_at=str(row["created_at"]),
            updated_at=str(row["updated_at"]),
            tags=tags,
            meta=meta,
            references=expected_references,
        )
        repaired += 1
    return repaired


def has_unlinkable_title_prefix(title: str) -> bool:
    lowered = title.strip().casefold()
    return lowered in UNLINKABLE_TITLE_NAMES or any(lowered.startswith(prefix) for prefix in UNLINKABLE_TITLE_PREFIXES)


def title_prefix_warning(title: str) -> str | None:
    if not title:
        return None
    if has_unlinkable_title_prefix(title):
        return TITLE_LINK_LIMIT_WARNING
    return None


def render_edit_form(**context):
    context.setdefault("spell_warning", None)
    context.setdefault("show_spellcheck_warning", False)
    context.setdefault("spellcheck_samples", [])
    context.setdefault("ignore_tag_warning", False)
    return render_template("edit.html", **context)


def set_doc_tags(conn: sqlite3.Connection, doc_id: int, tags: list[str]) -> None:
    old_tag_rows = conn.execute(
        "SELECT tag_id FROM doc_tags WHERE doc_id = ?",
        (doc_id,),
    ).fetchall()
    affected_tag_ids = {int(row["tag_id"]) for row in old_tag_rows}

    conn.execute("DELETE FROM doc_tags WHERE doc_id = ?", (doc_id,))
    normalized_tags = parse_tags(",".join(str(tag) for tag in tags))
    if normalized_tags:
        conn.executemany(
            "INSERT INTO tags (name) VALUES (?) ON CONFLICT(name) DO NOTHING",
            [(tag,) for tag in normalized_tags],
        )
        placeholders = ",".join("?" for _ in normalized_tags)
        tag_rows = conn.execute(
            f"SELECT id FROM tags WHERE name COLLATE NOCASE IN ({placeholders})",
            normalized_tags,
        ).fetchall()
        if tag_rows:
            affected_tag_ids.update(int(row["id"]) for row in tag_rows)
            conn.executemany(
                "INSERT OR IGNORE INTO doc_tags (doc_id, tag_id) VALUES (?, ?)",
                [(doc_id, int(row["id"])) for row in tag_rows],
            )
    if affected_tag_ids:
        placeholders = ",".join("?" for _ in affected_tag_ids)
        conn.execute(
            f"""
            DELETE FROM tags
            WHERE id IN ({placeholders})
              AND NOT EXISTS (
                SELECT 1 FROM doc_tags WHERE doc_tags.tag_id = tags.id
              )
            """,
            list(affected_tag_ids),
        )


def set_doc_references(conn: sqlite3.Connection, doc_id: int, references: dict[str, list[str]]) -> None:
    payload = normalize_reference_payload(references)
    conn.execute("DELETE FROM doc_references WHERE source_doc_id = ?", (doc_id,))

    rows: list[tuple[int, str, str, str, str]] = []
    for ref_type, key in (("link", "links"), ("template", "templates")):
        for raw_target in payload[key]:
            rows.append(
                (
                    doc_id,
                    ref_type,
                    raw_target,
                    reference_title_key(raw_target),
                    slugify(raw_target),
                )
            )
    if rows:
        conn.executemany(
            """
            INSERT OR IGNORE INTO doc_references
            (source_doc_id, ref_type, raw_target, target_title_key, target_slug_key)
            VALUES (?, ?, ?, ?, ?)
            """,
            rows,
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


def iter_sqlite_chunks(values: list[str], size: int = SQLITE_IN_CLAUSE_CHUNK_SIZE):
    for index in range(0, len(values), size):
        yield values[index : index + size]


def bulk_resolve_doc_references(conn: sqlite3.Connection, refs: list[str]) -> dict[str, str | None]:
    lookups: list[str] = []
    lookup_keys: list[str] = []
    seen: set[str] = set()
    for ref in refs:
        lookup = ref.strip()
        if not lookup:
            continue
        key = lookup.casefold()
        if key in seen:
            continue
        seen.add(key)
        lookups.append(lookup)
        lookup_keys.append(key)

    if not lookups:
        return {}

    resolved: dict[str, str | None] = {key: None for key in lookup_keys}
    for chunk in iter_sqlite_chunks(lookups):
        placeholders = ",".join("?" for _ in chunk)
        rows = conn.execute(
            f"SELECT title, slug FROM docs WHERE title COLLATE NOCASE IN ({placeholders})",
            chunk,
        ).fetchall()
        for row in rows:
            resolved[str(row["title"]).strip().casefold()] = str(row["slug"])

    slug_to_keys: defaultdict[str, list[str]] = defaultdict(list)
    for lookup, key in zip(lookups, lookup_keys):
        if resolved.get(key):
            continue
        slug_to_keys[slugify(lookup).casefold()].append(key)

    slug_lookups = list(slug_to_keys.keys())
    for chunk in iter_sqlite_chunks(slug_lookups):
        placeholders = ",".join("?" for _ in chunk)
        rows = conn.execute(
            f"SELECT slug FROM docs WHERE slug COLLATE NOCASE IN ({placeholders})",
            chunk,
        ).fetchall()
        for row in rows:
            slug = str(row["slug"])
            for key in slug_to_keys.get(slug.casefold(), []):
                resolved[key] = slug

    return resolved


def render_markdown(conn: sqlite3.Connection, text: str) -> Markup:
    wiki_refs, template_refs = extract_reference_targets(text)
    reference_cache = bulk_resolve_doc_references(conn, [*wiki_refs, *template_refs])

    def resolve_cached(ref: str) -> str | None:
        key = ref.strip().casefold()
        if key not in reference_cache:
            reference_cache[key] = resolve_doc_reference(conn, ref)
        return reference_cache[key]

    html = markdown_engine.render(
        text,
        resolve_doc_reference=resolve_cached,
        read_document=read_document_if_exists,
    )
    return Markup(html)


def parse_export_doc_address(raw_address: str) -> str:
    address = str(raw_address or "").strip()
    if not address:
        raise ExportError("내보낼 문서 주소를 입력해 주세요.")

    if address.startswith("/"):
        parsed = urlsplit(address)
    else:
        parsed = urlsplit(address if "://" in address else f"http://{address}")
        if parsed.scheme.casefold() not in {"http", "https"}:
            raise ExportError("http 또는 https 형식의 문서 주소만 사용할 수 있습니다.")
        if parsed.hostname not in {"127.0.0.1", "localhost", "::1"}:
            raise ExportError("이 PersonalWiki의 로컬 문서 주소만 내보낼 수 있습니다.")
        if parsed.username or parsed.password:
            raise ExportError("사용자 정보가 포함된 주소는 사용할 수 없습니다.")
        try:
            parsed.port
        except ValueError as error:
            raise ExportError("포트 번호가 올바르지 않습니다.") from error

    path = unquote(parsed.path)
    if not path.startswith("/doc/"):
        raise ExportError("주소는 /doc/문서주소 형식이어야 합니다.")
    slug = path[len("/doc/") :].strip("/")
    if not slug or "\0" in slug:
        raise ExportError("문서 주소에 문서명이 없습니다.")
    if any(part in {".", ".."} for part in PurePosixPath(slug).parts):
        raise ExportError("안전하지 않은 문서 경로는 사용할 수 없습니다.")
    return slug


def _export_asset_path(url: str) -> tuple[str, Path, PurePosixPath]:
    decoded = unquote(url)
    if decoded.startswith("/img/"):
        kind = "img"
        root = IMG_DIR
        relative_raw = decoded[len("/img/") :]
    elif decoded.startswith("/file/"):
        kind = "file"
        root = FILE_DIR
        relative_raw = decoded[len("/file/") :]
    else:
        raise ExportError(f"지원하지 않는 로컬 자산 주소입니다: {url}")

    relative = PurePosixPath(relative_raw.replace("\\", "/"))
    if not relative.parts or relative.is_absolute() or any(part in {"", ".", ".."} for part in relative.parts):
        raise ExportError(f"안전하지 않은 자산 경로입니다: {url}")
    source = root.joinpath(*relative.parts).resolve()
    try:
        source.relative_to(root.resolve())
    except ValueError as error:
        raise ExportError(f"안전하지 않은 자산 경로입니다: {url}") from error
    if not source.is_file():
        raise ExportError(f"문서가 참조하는 파일을 찾을 수 없습니다: {decoded}")
    return kind, source, relative


def _rewrite_local_asset_urls(html_text: str, replacer) -> str:
    def replace_match(match: re.Match[str]) -> str:
        rewritten = replacer(match.group("url"))
        return f'{match.group("prefix")}{match.group("quote")}{rewritten}{match.group("quote")}'

    return LOCAL_ASSET_URL_RE.sub(replace_match, html_text)


def _export_document_shell(
    doc: dict,
    rendered_content: str,
    *,
    stylesheet: str,
    script: str,
    inline_assets: bool,
) -> str:
    safe_title = str(Markup.escape(str(doc["title"])))
    safe_created = str(Markup.escape(str(doc.get("created_at", ""))))
    safe_updated = str(Markup.escape(str(doc.get("updated_at", ""))))
    tags = "".join(
        f'<span class="tag">#{Markup.escape(str(tag))}</span>'
        for tag in doc.get("tags", [])
    )
    if inline_assets:
        style_tag = f"<style>\n{stylesheet}\n</style>"
        script_tag = f"<script>\n{script}\n</script>"
    else:
        style_tag = f'<link rel="stylesheet" href="{stylesheet}">'
        script_tag = f'<script src="{script}" defer></script>'

    return f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{safe_title} | Personal Wiki</title>
  {style_tag}
</head>
<body class="app-shell page-exported-doc">
  <main class="container">
    <section class="panel">
      <header class="doc-header">
        <div>
          <p class="eyebrow">Exported Document</p>
          <h1>{safe_title}</h1>
          <p class="muted">생성: {safe_created} / 수정: {safe_updated}</p>
          <div class="tag-row">{tags}</div>
        </div>
      </header>
      <article class="markdown-body">{rendered_content}</article>
    </section>
  </main>
  {script_tag}
</body>
</html>
"""


def build_single_html_export(doc: dict, rendered_content: str) -> bytes:
    css = read_text_normalized(STATIC_DIR / "style.css")
    script = read_text_normalized(STATIC_DIR / "wiki.js")

    def embed_image(url: str) -> str:
        kind, source, _relative = _export_asset_path(url)
        if kind == "file":
            return url
        mime = mimetypes.guess_type(source.name)[0] or "application/octet-stream"
        encoded = base64.b64encode(source.read_bytes()).decode("ascii")
        return f"data:{mime};base64,{encoded}"

    rewritten = _rewrite_local_asset_urls(rendered_content, embed_image)
    html_text = _export_document_shell(
        doc,
        rewritten,
        stylesheet=css,
        script=script,
        inline_assets=True,
    )
    return html_text.encode("utf-8")


def build_zip_export(doc: dict, rendered_content: str) -> bytes:
    output = io.BytesIO()
    copied: set[str] = set()

    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        def package_asset(url: str) -> str:
            kind, source, relative = _export_asset_path(url)
            archive_name = PurePosixPath("assets", kind, *relative.parts).as_posix()
            if archive_name not in copied:
                archive.write(source, archive_name)
                copied.add(archive_name)
            return quote(archive_name, safe="/")

        rewritten = _rewrite_local_asset_urls(rendered_content, package_asset)
        archive.writestr("assets/style.css", (STATIC_DIR / "style.css").read_bytes())
        archive.writestr("assets/wiki.js", (STATIC_DIR / "wiki.js").read_bytes())
        archive.writestr(
            "index.html",
            _export_document_shell(
                doc,
                rewritten,
                stylesheet="assets/style.css",
                script="assets/wiki.js",
                inline_assets=False,
            ).encode("utf-8"),
        )

    output.seek(0)
    return output.getvalue()


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


def escape_sql_like(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def search_docs_by_tags(conn: sqlite3.Connection, terms: list[str], *, limit: int = 200) -> list[dict]:
    if not terms:
        return []

    patterns = [f"%{escape_sql_like(term)}%" for term in terms if term]
    if not patterns:
        return []

    where_clause = " OR ".join("t.name LIKE ? ESCAPE '\\' COLLATE NOCASE" for _ in patterns)
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


def find_backlinks(conn: sqlite3.Connection, target_slug: str) -> list[dict]:
    target_row = conn.execute("SELECT title, slug FROM docs WHERE slug = ?", (target_slug,)).fetchone()
    if target_row is None:
        return []

    title_key = reference_title_key(target_row["title"])
    slug_key = str(target_row["slug"])

    rows = conn.execute(
        """
        WITH matched_refs AS (
            SELECT source_doc_id, ref_type
            FROM doc_references
            WHERE target_title_key = ?
            UNION ALL
            SELECT source_doc_id, ref_type
            FROM doc_references
            WHERE target_slug_key = ?
        )
        SELECT
            d.id,
            d.title,
            d.slug,
            d.updated_at,
            MAX(CASE WHEN r.ref_type = 'link' THEN 1 ELSE 0 END) AS has_link,
            MAX(CASE WHEN r.ref_type = 'template' THEN 1 ELSE 0 END) AS has_template
        FROM matched_refs r
        JOIN docs d ON d.id = r.source_doc_id
        WHERE d.slug != ?
        GROUP BY d.id, d.title, d.slug, d.updated_at
        ORDER BY d.updated_at DESC, d.title COLLATE NOCASE
        """,
        (title_key, slug_key, target_slug),
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
    token_conn: sqlite3.Connection,
    *,
    slug: str,
    md_file: Path,
    mtime: float,
    refresh_token_idf: bool = True,
    mark_fts_current: bool = True,
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
    meta["sidecar"] = f"json/{slug}.json"

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
    upsert_language_doc_tokens(token_conn, conn, doc_id, title, content, refresh_idf=refresh_token_idf)
    if mark_fts_current:
        mark_fts_index_current(conn, fts_conn)
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
    token_conn: sqlite3.Connection,
    *,
    row: sqlite3.Row,
    md_file: Path,
    mtime: float,
    refresh_token_idf: bool = True,
    mark_fts_current: bool = True,
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
    meta["sidecar"] = f"json/{slug}.json"
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
    upsert_language_doc_tokens(token_conn, conn, doc_id, current_title, content, refresh_idf=refresh_token_idf)
    if mark_fts_current:
        mark_fts_index_current(conn, fts_conn)
    write_sidecar(
        slug=slug,
        title=current_title,
        created_at=row["created_at"],
        updated_at=updated_at,
        tags=tags,
        meta=meta,
        references=references,
    )


def sync_deleted_doc(
    conn: sqlite3.Connection,
    fts_conn: sqlite3.Connection,
    token_conn: sqlite3.Connection,
    *,
    row: sqlite3.Row,
    refresh_token_idf: bool = True,
    mark_fts_current: bool = True,
) -> None:
    doc_id = int(row["id"])
    fts_conn.execute("DELETE FROM docs_fts WHERE rowid = ?", (doc_id,))
    conn.execute("DELETE FROM docs WHERE id = ?", (doc_id,))
    delete_language_doc_tokens(token_conn, conn, doc_id, refresh_idf=refresh_token_idf)
    if mark_fts_current:
        mark_fts_index_current(conn, fts_conn)


def repair_fts_mismatch(
    conn: sqlite3.Connection,
    fts_conn: sqlite3.Connection,
    token_conn: sqlite3.Connection,
    *,
    force_rebuild: bool = False,
) -> tuple[int, int, int]:
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
        upsert_language_doc_tokens(
            token_conn,
            conn,
            doc_id,
            str(row["title"]),
            content,
            refresh_idf=False,
        )

    for doc_id in orphan_ids:
        fts_conn.execute("DELETE FROM docs_fts WHERE rowid = ?", (doc_id,))

    rebuilt_count = 0
    if force_rebuild:
        rebuilt_count = rebuild_fts_index_from_docs(conn, fts_conn)
    else:
        mark_fts_index_current(conn, fts_conn)

    return len(missing_ids), len(orphan_ids), rebuilt_count


def rebuild_fts_index_from_docs(conn: sqlite3.Connection, fts_conn: sqlite3.Connection) -> int:
    rows = conn.execute("SELECT id, title, slug FROM docs ORDER BY id").fetchall()
    fts_conn.execute("DELETE FROM docs_fts")
    for row in rows:
        content = read_document_if_exists(str(row["slug"])) or ""
        update_fts(fts_conn, int(row["id"]), str(row["title"]), content)
    mark_fts_index_current(conn, fts_conn)
    return len(rows)


def sync_documents_incremental() -> dict[str, int]:
    with closing(connect_db()) as conn, closing(connect_fts_db()) as fts_conn, closing(connect_token_db()) as token_conn:
        md_snapshot = build_markdown_snapshot()
        db_snapshot = build_db_snapshot(conn)
        new_slugs, deleted_slugs, modified_slugs = detect_incremental_changes(md_snapshot, db_snapshot)
        fts_rebuild_needed_before_changes = fts_index_needs_rebuild(conn, fts_conn)
        token_rebuild_needed_before_changes = language_token_index_needs_rebuild(token_conn, conn)

        total_changes = len(new_slugs) + len(deleted_slugs) + len(modified_slugs)
        doc_count_changed = bool(new_slugs or deleted_slugs)
        refresh_modified_tokens_incrementally = (
            bool(modified_slugs)
            and not doc_count_changed
            and not token_rebuild_needed_before_changes
        )
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
            sync_new_doc(
                conn,
                fts_conn,
                token_conn,
                slug=slug,
                md_file=md_file,
                mtime=mtime,
                refresh_token_idf=False,
                mark_fts_current=False,
            )

        for slug in modified_slugs:
            md_file = md_snapshot[slug]["path"]
            mtime = float(md_snapshot[slug]["mtime"])
            row = db_snapshot[slug]
            sync_modified_doc(
                conn,
                fts_conn,
                token_conn,
                row=row,
                md_file=md_file,
                mtime=mtime,
                refresh_token_idf=refresh_modified_tokens_incrementally,
                mark_fts_current=False,
            )

        for slug in deleted_slugs:
            sync_deleted_doc(
                conn,
                fts_conn,
                token_conn,
                row=db_snapshot[slug],
                refresh_token_idf=False,
                mark_fts_current=False,
            )

        fts_missing, fts_orphan, fts_rebuilt = repair_fts_mismatch(
            conn,
            fts_conn,
            token_conn,
            force_rebuild=fts_rebuild_needed_before_changes,
        )
        sidecar_repaired = repair_sidecar_mismatches(conn)
        deferred_token_idf = (
            doc_count_changed
            or fts_missing > 0
            or fts_rebuilt > 0
            or (bool(modified_slugs) and not refresh_modified_tokens_incrementally)
        )
        if fts_rebuilt > 0:
            token_docs, token_terms = rebuild_language_token_index(token_conn, conn, fts_conn)
            token_rebuilt = True
        else:
            if deferred_token_idf and not token_rebuild_needed_before_changes:
                finalize_language_token_batch(token_conn, conn)
            token_rebuilt, token_docs, token_terms = ensure_language_token_index_current(token_conn, conn, fts_conn)

        conn.commit()
        fts_conn.commit()
        token_conn.commit()
        if total_changes > 0 or fts_missing > 0 or fts_orphan > 0 or fts_rebuilt > 0 or token_rebuilt:
            invalidate_tag_recommendation_cache()

        print(
            "[SYNC] startup incremental sync "
            f"new={len(new_slugs)} deleted={len(deleted_slugs)} modified={len(modified_slugs)} "
            f"fts_missing={fts_missing} fts_orphan={fts_orphan} fts_rebuilt={fts_rebuilt} "
            f"sidecar_repaired={sidecar_repaired} "
            f"token_rebuilt={token_rebuilt} token_docs={token_docs} token_terms={token_terms}"
        )
        return {
            "new": len(new_slugs),
            "deleted": len(deleted_slugs),
            "modified": len(modified_slugs),
            "fts_missing": fts_missing,
            "fts_orphan": fts_orphan,
            "fts_rebuilt": fts_rebuilt,
            "sidecar_repaired": sidecar_repaired,
            "token_rebuilt": int(token_rebuilt),
            "token_docs": token_docs,
            "token_terms": token_terms,
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

    release_data_lock()
    try:
        db_fix_ok = run_db_fix_tool(reason)
    finally:
        acquire_data_lock()

    if not db_fix_ok:
        raise RuntimeError(f"startup sync failed and DB fix did not complete: {reason}")
    print("[SYNC] Startup recovery completed by PersonalWikiDBFix.")


def ensure_default_home() -> None:
    with closing(connect_db()) as conn, closing(connect_fts_db()) as fts_conn, closing(connect_token_db()) as token_conn:
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
        meta = {"sidecar": f"json/{slug}.json"}
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
        upsert_language_doc_tokens(token_conn, conn, doc_id, title, content)
        mark_fts_index_current(conn, fts_conn)
        write_sidecar(
            slug=slug,
            title=title,
            created_at=now,
            updated_at=now,
            tags=tags,
            meta=meta,
            references=references,
        )
        conn.commit()
        fts_conn.commit()
        token_conn.commit()
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
        LIMIT 100
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
    token_conn = get_token_db()

    if request.method == "POST":
        title = request.form.get("title", "").strip()
        content = normalize_newlines(request.form.get("content", ""))
        tags = parse_tags(request.form.get("tags", ""))
        ignore_tag_warning = request.form.get("ignore_tag_warning") == "1"
        spellcheck_action = request.form.get("spellcheck_action", "")
        if spellcheck_action == "auto_fix":
            title, content = apply_korean_spell_autofix(title, content)

        def suggested_tags_for_form() -> list[str]:
            return recommend_tags(
                conn,
                fts_conn,
                token_conn,
                title=title,
                content=content,
                exclude_tags=tags,
                limit=TAG_RECOMMEND_LIMIT,
            )

        title_warning = title_prefix_warning(title)

        if not title:
            return render_edit_form(
                mode="new",
                doc={"title": "", "slug": ""},
                content=content,
                tags_text=", ".join(tags),
                error="문서 제목을 입력해 주세요.",
                tag_warning=None,
                title_warning=title_warning,
                show_ignore_tag_warning=False,
                recommended_tags=suggested_tags_for_form(),
            )

        duplicate = conn.execute(
            "SELECT 1 FROM docs WHERE title = ? COLLATE NOCASE",
            (title,),
        ).fetchone()
        if duplicate:
            return render_edit_form(
                mode="new",
                doc={"title": title, "slug": ""},
                content=content,
                tags_text=", ".join(tags),
                error="같은 제목의 문서가 이미 있습니다.",
                tag_warning=None,
                title_warning=title_warning,
                show_ignore_tag_warning=False,
                recommended_tags=suggested_tags_for_form(),
            )

        if len(tags) < 2 and not ignore_tag_warning:
            return render_edit_form(
                mode="new",
                doc={"title": title, "slug": ""},
                content=content,
                tags_text=", ".join(tags),
                error=None,
                tag_warning="태그를 2개 이상 등록하면 나중에 검색이 더 쉬워집니다. 계속 생성하려면 아래 버튼을 눌러 주세요.",
                title_warning=title_warning,
                show_ignore_tag_warning=True,
                recommended_tags=suggested_tags_for_form(),
            )

        if spellcheck_action not in {"auto_fix", "save_as_is"}:
            spell_issues = collect_korean_spell_issues(title, content)
            if spell_issues:
                return render_edit_form(
                    mode="new",
                    doc={"title": title, "slug": ""},
                    content=content,
                    tags_text=", ".join(tags),
                    error=None,
                    tag_warning=None,
                    title_warning=title_warning,
                    show_ignore_tag_warning=False,
                    ignore_tag_warning=ignore_tag_warning,
                    spell_warning=korean_spell_warning_message(spell_issues["count"]),
                    show_spellcheck_warning=True,
                    spellcheck_samples=spell_issues["samples"],
                    recommended_tags=suggested_tags_for_form(),
                )

        slug = ensure_unique_slug(conn, slugify(title))
        created_at = now_iso()
        meta = {"sidecar": f"json/{slug}.json"}
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
            upsert_language_doc_tokens(token_conn, conn, doc_id, title, content)
            mark_fts_index_current(conn, fts_conn)
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
            token_conn.commit()
            invalidate_tag_recommendation_cache()
        except Exception:
            conn.rollback()
            fts_conn.rollback()
            token_conn.rollback()
            raise
        return redirect(url_for("view_doc", slug=slug))

    prefilled_title = request.args.get("title", "").strip()
    return render_edit_form(
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
    token_conn = get_token_db()
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
        spellcheck_action = request.form.get("spellcheck_action", "")
        if spellcheck_action == "auto_fix":
            new_title, new_content = apply_korean_spell_autofix(new_title, new_content)

        def suggested_tags_for_form() -> list[str]:
            return recommend_tags(
                conn,
                fts_conn,
                token_conn,
                title=new_title or row["title"],
                content=new_content,
                current_slug=row["slug"],
                exclude_tags=new_tags,
                limit=TAG_RECOMMEND_LIMIT,
            )

        title_warning = title_prefix_warning(new_title)

        if not new_title:
            return render_edit_form(
                mode="edit",
                doc=doc,
                content=new_content,
                tags_text=", ".join(new_tags),
                error="문서 제목을 입력해 주세요.",
                tag_warning=None,
                title_warning=title_warning,
                show_ignore_tag_warning=False,
                recommended_tags=suggested_tags_for_form(),
            )

        duplicate = conn.execute(
            "SELECT id FROM docs WHERE title = ? COLLATE NOCASE AND id != ?",
            (new_title, row["id"]),
        ).fetchone()
        if duplicate:
            return render_edit_form(
                mode="edit",
                doc=doc,
                content=new_content,
                tags_text=", ".join(new_tags),
                error="같은 제목의 문서가 이미 있습니다.",
                tag_warning=None,
                title_warning=title_warning,
                show_ignore_tag_warning=False,
                recommended_tags=suggested_tags_for_form(),
            )

        if spellcheck_action not in {"auto_fix", "save_as_is"}:
            spell_issues = collect_korean_spell_issues(new_title, new_content)
            if spell_issues:
                return render_edit_form(
                    mode="edit",
                    doc=doc,
                    content=new_content,
                    tags_text=", ".join(new_tags),
                    error=None,
                    tag_warning=None,
                    title_warning=title_warning,
                    show_ignore_tag_warning=False,
                    spell_warning=korean_spell_warning_message(spell_issues["count"]),
                    show_spellcheck_warning=True,
                    spellcheck_samples=spell_issues["samples"],
                    recommended_tags=suggested_tags_for_form(),
                )

        new_slug_candidate = slugify(new_title)
        new_slug = ensure_unique_slug(conn, new_slug_candidate, exclude_doc_id=row["id"])
        old_slug = row["slug"]

        updated_at = now_iso()
        meta = safe_load_json(row["meta_json"])
        meta["sidecar"] = f"json/{new_slug}.json"
        moved_assets: list[tuple[Path, Path]] = []
        try:
            if new_slug != old_slug:
                moved_assets = move_document_assets_for_slug_change(old_slug, new_slug)
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
            upsert_language_doc_tokens(token_conn, conn, row["id"], new_title, new_content)
            mark_fts_index_current(conn, fts_conn)
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
            token_conn.commit()
            invalidate_tag_recommendation_cache()
        except Exception:
            conn.rollback()
            fts_conn.rollback()
            token_conn.rollback()
            rollback_document_asset_moves(moved_assets)
            raise
        return redirect(url_for("view_doc", slug=new_slug))

    doc["tags"] = tags
    return render_edit_form(
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
            token_conn,
            title=doc["title"],
            content=current_content,
            current_slug=doc["slug"],
            exclude_tags=tags,
            limit=TAG_RECOMMEND_LIMIT,
        ),
    )


@app.post("/delete/<path:slug>")
def delete_doc(slug: str):
    conn = get_db()
    fts_conn = get_fts_db()
    token_conn = get_token_db()
    row = conn.execute("SELECT * FROM docs WHERE slug = ?", (slug,)).fetchone()
    if row is None:
        abort(404)

    try:
        fts_conn.execute("DELETE FROM docs_fts WHERE rowid = ?", (row["id"],))
        conn.execute("DELETE FROM docs WHERE id = ?", (row["id"],))
        delete_language_doc_tokens(token_conn, conn, row["id"])
        mark_fts_index_current(conn, fts_conn)
        conn.commit()
        fts_conn.commit()
        token_conn.commit()
        invalidate_tag_recommendation_cache()
    except Exception:
        conn.rollback()
        fts_conn.rollback()
        token_conn.rollback()
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


@app.get("/tool/table")
def table_editor():
    return render_template("table_editor.html")


def _load_export_document(address: str) -> tuple[dict, str]:
    slug = parse_export_doc_address(address)
    doc = fetch_doc_with_tags(get_db(), slug)
    if doc is None:
        raise ExportError("해당 주소의 문서를 찾을 수 없습니다.")
    content = read_document_if_exists(str(doc["slug"]))
    if content is None:
        raise ExportError("문서 원본 파일을 찾을 수 없습니다.")
    return doc, content


@app.get("/tool/package")
def package_tool():
    return render_template(
        "package_tool.html",
        error=None,
        document_address=f"http://{request.host}/doc/",
        needs_confirmation=False,
        pending_format="",
    )


@app.post("/api/package/check")
def package_check():
    payload = request.get_json(silent=True) or {}
    address = str(payload.get("document_address", ""))
    try:
        doc, content = _load_export_document(address)
    except ExportError as error:
        return jsonify({"ok": False, "error": str(error)}), 400
    file_count = len(FILE_REFERENCE_RE.findall(content))
    return jsonify(
        {
            "ok": True,
            "title": str(doc["title"]),
            "has_files": file_count > 0,
            "file_count": file_count,
        }
    )


@app.post("/tool/package/export")
def package_export():
    address = request.form.get("document_address", "").strip()
    export_format = request.form.get("export_format", "").strip().casefold()
    confirmed_files = request.form.get("confirmed_files") == "1"
    try:
        doc, content = _load_export_document(address)
        if export_format not in {"zip", "html"}:
            raise ExportError("내보내기 형식을 선택해 주세요.")

        has_file_references = FILE_REFERENCE_RE.search(content) is not None
        if export_format == "html" and has_file_references and not confirmed_files:
            return (
                render_template(
                    "package_tool.html",
                    error="첨부 파일 링크([[file/... ]])는 단일 HTML에 포함되지 않습니다. 계속하려면 다시 확인해 주세요.",
                    document_address=address,
                    needs_confirmation=True,
                    pending_format="html",
                ),
                409,
            )

        rendered = str(render_markdown(get_db(), content))
        if export_format == "zip":
            payload = build_zip_export(doc, rendered)
            mimetype = "application/zip"
            extension = "zip"
        else:
            payload = build_single_html_export(doc, rendered)
            mimetype = "text/html; charset=utf-8"
            extension = "html"
    except (ExportError, OSError) as error:
        return (
            render_template(
                "package_tool.html",
                error=str(error),
                document_address=address,
                needs_confirmation=False,
                pending_format=export_format,
            ),
            400,
        )

    return send_file(
        io.BytesIO(payload),
        as_attachment=True,
        download_name=f"{doc['slug']}.{extension}",
        mimetype=mimetype,
        max_age=0,
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
        get_token_db(),
        title=title,
        content=content,
        current_slug=current_slug,
        exclude_tags=tags,
        limit=TAG_RECOMMEND_LIMIT,
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
    acquire_data_lock()
    init_db()
    init_fts_db()
    init_token_db()
    sync_documents_on_startup()
    ensure_default_home()


if os.environ.get("PERSONALWIKI_SKIP_BOOTSTRAP") != "1":
    bootstrap()


if __name__ == "__main__":
    app.run(host=DEFAULT_HOST, port=read_server_port(), debug=False)
