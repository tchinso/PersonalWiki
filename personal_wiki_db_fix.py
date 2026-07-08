from __future__ import annotations

import json
import os
import re
import sqlite3
import sys
import tempfile
import time
import unicodedata
from contextlib import closing
from datetime import datetime
from pathlib import Path

from markdown_engine import extract_reference_targets
from language_tools import build_language_index_source_signature, ensure_language_token_tables, rebuild_language_token_index


def runtime_data_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


DATA_DIR = runtime_data_dir()
DOC_DIR = DATA_DIR / "doc"
JSON_DIR = DOC_DIR / "json"
DB_PATH = DATA_DIR / "wiki.db"
FTS_DB_PATH = DATA_DIR / "wiki_fts.db"
TOKEN_DB_PATH = DATA_DIR / "wiki_token.db"
DATA_LOCK_PATH = DATA_DIR / "wiki.lock"
_DATA_LOCK_FILE = None


def iso_from_timestamp(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp).isoformat(timespec="seconds")


def normalize_newlines(text: str) -> str:
    if not text:
        return ""
    normalized = text.replace("\r\r\n", "\n")
    normalized = normalized.replace("\r\n", "\n").replace("\r", "\n")
    return normalized


def read_text_normalized(path: Path) -> str:
    with path.open("r", encoding="utf-8", newline="") as file:
        return normalize_newlines(file.read())


def write_text_atomic(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_name: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            newline="\n",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as file:
            temp_name = file.name
            file.write(content)
            file.flush()
            os.fsync(file.fileno())
        os.replace(temp_name, path)
    finally:
        if temp_name is not None:
            try:
                Path(temp_name).unlink(missing_ok=True)
            except OSError:
                pass


def cleanup_stale_temp_files() -> int:
    removed = 0
    for directory in (DOC_DIR, JSON_DIR):
        if not directory.exists():
            continue
        for path in directory.glob(".*.tmp"):
            try:
                if path.is_file():
                    path.unlink()
                    removed += 1
            except OSError as error:
                print(f"[WARN] failed to remove stale temp file {path}: {error}")
    return removed


def read_json_dict(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return {}
    if isinstance(data, dict):
        return data
    return {}


def infer_title_from_content(content: str, fallback: str) -> str:
    match = re.search(r"^\s*#\s+(.+)$", content, flags=re.MULTILINE)
    if match:
        title = match.group(1).strip()
        if title:
            return title
    return fallback


def slugify(title: str) -> str:
    normalized = unicodedata.normalize("NFKC", title).strip()
    normalized = re.sub(r"[^\w\s\-가-힣]", "", normalized, flags=re.UNICODE)
    normalized = re.sub(r"[\s_]+", "-", normalized, flags=re.UNICODE)
    slug = normalized.strip("-").lower()
    return slug or "untitled"


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
            "PersonalWiki가 실행 중이거나 다른 DBFix가 작업 중입니다. "
            "DB 손상을 막기 위해 이번 복구를 중단합니다."
        ) from error
    _DATA_LOCK_FILE = handle


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
    conn.execute("PRAGMA temp_store = MEMORY")
    conn.execute("PRAGMA cache_size = -20000")
    conn.execute("PRAGMA mmap_size = 268435456")
    if foreign_keys:
        conn.execute("PRAGMA foreign_keys = ON")


def connect_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    configure_sqlite_connection(conn, foreign_keys=True)
    return conn


def init_main_db(conn: sqlite3.Connection) -> None:
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
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_docs_updated_title "
        "ON docs (updated_at DESC, title COLLATE NOCASE)"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_doc_tags_tag_id ON doc_tags (tag_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_doc_tags_tag_doc ON doc_tags (tag_id, doc_id)")
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


def init_fts_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE VIRTUAL TABLE IF NOT EXISTS docs_fts
        USING fts5(title, content)
        """
    )
    ensure_fts_index_meta_table(conn)


def ensure_fts_index_meta_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fts_index_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """
    )


def set_fts_index_meta(conn: sqlite3.Connection, key: str, value: object) -> None:
    ensure_fts_index_meta_table(conn)
    conn.execute(
        """
        INSERT INTO fts_index_meta (key, value)
        VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        (key, str(value)),
    )


def mark_fts_index_current(main_conn: sqlite3.Connection, fts_conn: sqlite3.Connection) -> None:
    set_fts_index_meta(
        fts_conn,
        "source_signature",
        build_language_index_source_signature(main_conn),
    )


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
        rows = conn.execute(
            f"SELECT id FROM tags WHERE name COLLATE NOCASE IN ({placeholders})",
            normalized_tags,
        ).fetchall()
        if rows:
            affected_tag_ids.update(int(row["id"]) for row in rows)
            conn.executemany(
                "INSERT OR IGNORE INTO doc_tags (doc_id, tag_id) VALUES (?, ?)",
                [(doc_id, int(row["id"])) for row in rows],
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
                ),
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


def update_fts(conn: sqlite3.Connection, doc_id: int, title: str, content: str) -> None:
    conn.execute("DELETE FROM docs_fts WHERE rowid = ?", (doc_id,))
    conn.execute(
        "INSERT INTO docs_fts (rowid, title, content) VALUES (?, ?, ?)",
        (doc_id, title, content),
    )


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
    payload = {
        "title": title,
        "slug": slug,
        "created_at": created_at,
        "updated_at": updated_at,
        "tags": tags,
        "meta": meta,
        "references": normalize_reference_payload(references),
    }
    sidecar_path = JSON_DIR / f"{slug}.json"
    write_text_atomic(
        sidecar_path,
        json.dumps(payload, ensure_ascii=False, indent=2),
    )


def collect_sidecar_tags(sidecar: dict) -> list[str]:
    raw = sidecar.get("tags")
    if isinstance(raw, list):
        return parse_tags(",".join(str(item) for item in raw))
    if isinstance(raw, str):
        return parse_tags(raw)
    return []


def sqlite_related_paths(path: Path) -> list[Path]:
    return [
        path,
        Path(f"{path}-wal"),
        Path(f"{path}-shm"),
        Path(f"{path}-journal"),
    ]


def remove_sqlite_family(path: Path) -> None:
    for related in sqlite_related_paths(path):
        if related.exists():
            related.unlink()


def move_existing_sqlite_family(path: Path, backup_dir: Path) -> list[tuple[Path, Path]]:
    moved: list[tuple[Path, Path]] = []
    for related in sqlite_related_paths(path):
        if not related.exists():
            continue
        target = backup_dir / related.name
        related.replace(target)
        moved.append((target, related))
    return moved


def restore_sqlite_backups(moved: list[tuple[Path, Path]]) -> None:
    for backup, original in reversed(moved):
        if backup.exists() and not original.exists():
            backup.replace(original)


def replace_databases_from_temp(temp_main: Path, temp_fts: Path, temp_token: Path) -> Path:
    backup_dir = DATA_DIR / "db_backups" / time.strftime("%Y%m%d-%H%M%S")
    backup_dir.mkdir(parents=True, exist_ok=True)
    moved: list[tuple[Path, Path]] = []
    replaced: list[Path] = []

    try:
        moved.extend(move_existing_sqlite_family(DB_PATH, backup_dir))
        moved.extend(move_existing_sqlite_family(FTS_DB_PATH, backup_dir))
        moved.extend(move_existing_sqlite_family(TOKEN_DB_PATH, backup_dir))

        temp_main.replace(DB_PATH)
        replaced.append(DB_PATH)
        temp_fts.replace(FTS_DB_PATH)
        replaced.append(FTS_DB_PATH)
        temp_token.replace(TOKEN_DB_PATH)
        replaced.append(TOKEN_DB_PATH)

        remove_sqlite_family(temp_main)
        remove_sqlite_family(temp_fts)
        remove_sqlite_family(temp_token)
        return backup_dir
    except Exception:
        for path in replaced:
            if path.exists():
                path.unlink()
        restore_sqlite_backups(moved)
        raise


def rebuild_from_doc_dir(main_db_path: Path, fts_db_path: Path, token_db_path: Path) -> tuple[int, int, int]:
    imported = 0
    skipped = 0
    token_terms = 0

    with (
        closing(connect_db(main_db_path)) as main_conn,
        closing(connect_db(fts_db_path)) as fts_conn,
        closing(connect_db(token_db_path)) as token_conn,
    ):
        init_main_db(main_conn)
        init_fts_db(fts_conn)
        ensure_language_token_tables(token_conn)

        for md_file in sorted(DOC_DIR.glob("*.md")):
            slug = md_file.stem
            sidecar = read_json_dict(JSON_DIR / f"{slug}.json")
            try:
                content = read_text_normalized(md_file)

                sidecar_title = str(sidecar.get("title", "")).strip()
                title_candidate = sidecar_title or infer_title_from_content(content, slug) or slug
                title = ensure_unique_title(main_conn, title_candidate)

                updated_at = iso_from_timestamp(md_file.stat().st_mtime)
                created_at_raw = str(sidecar.get("created_at") or "").strip()
                created_at = created_at_raw or updated_at

                meta_value = sidecar.get("meta")
                meta = meta_value if isinstance(meta_value, dict) else {}
                meta["sidecar"] = f"json/{slug}.json"

                main_conn.execute(
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
                row = main_conn.execute("SELECT id FROM docs WHERE slug = ?", (slug,)).fetchone()
                if row is None:
                    raise RuntimeError("failed to fetch inserted doc id")
                doc_id = int(row["id"])

                tags = collect_sidecar_tags(sidecar)
                references = extract_reference_payload(content)

                set_doc_tags(main_conn, doc_id, tags)
                set_doc_references(main_conn, doc_id, references)
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
                imported += 1
            except Exception as error:
                skipped += 1
                print(f"[WARN] skipped {md_file.name}: {error}")

        _token_docs, token_terms = rebuild_language_token_index(token_conn, main_conn, fts_conn)
        mark_fts_index_current(main_conn, fts_conn)
        main_conn.commit()
        fts_conn.commit()
        token_conn.commit()
    return imported, skipped, token_terms


def recreate_databases() -> tuple[int, int, int]:
    DOC_DIR.mkdir(parents=True, exist_ok=True)
    JSON_DIR.mkdir(parents=True, exist_ok=True)
    cleanup_stale_temp_files()

    temp_main = DATA_DIR / "wiki.rebuild.db"
    temp_fts = DATA_DIR / "wiki_fts.rebuild.db"
    temp_token = DATA_DIR / "wiki_token.rebuild.db"
    remove_sqlite_family(temp_main)
    remove_sqlite_family(temp_fts)
    remove_sqlite_family(temp_token)

    try:
        imported, skipped, token_terms = rebuild_from_doc_dir(temp_main, temp_fts, temp_token)
    except Exception:
        remove_sqlite_family(temp_main)
        remove_sqlite_family(temp_fts)
        remove_sqlite_family(temp_token)
        raise
    backup_dir = replace_databases_from_temp(temp_main, temp_fts, temp_token)
    print(f"[OK] previous DB backup: {backup_dir}")
    return imported, skipped, token_terms


def main() -> int:
    print("PersonalWiki DB fixer")
    print(f"Data directory: {DATA_DIR}")
    print(f"Doc directory: {DOC_DIR}")
    print(f"Main DB: {DB_PATH}")
    print(f"FTS DB: {FTS_DB_PATH}")
    print(f"Token DB: {TOKEN_DB_PATH}")

    try:
        acquire_data_lock()
        try:
            imported, skipped, token_terms = recreate_databases()
        finally:
            release_data_lock()
    except Exception as error:
        print(f"[ERROR] failed to rebuild databases: {error}")
        return 1

    print(f"[OK] recreated databases from /doc: imported={imported}, skipped={skipped}")
    print(f"[OK] recreated token DB: terms={token_terms}")
    print("[OK] sidecar JSON now includes normalized backlink reference cache.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
