from __future__ import annotations

import json
import os
import re
import sqlite3
import sys
import tempfile
import time
import unicodedata
import uuid
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
SQLITE_MAIN_SYNCHRONOUS = "FULL"
SQLITE_DERIVED_SYNCHRONOUS = "NORMAL"
STAGED_DOCUMENT_ASSET_RE = re.compile(
    r"^\.[^.].*\.(?:edit|delete)-\d+-\d+\.(?:tmp|stage)$",
    flags=re.IGNORECASE,
)


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
            # Legacy server versions used .tmp for staged edit/delete assets.
            # A DBFix run must never erase a possibly sole source document.
            if STAGED_DOCUMENT_ASSET_RE.fullmatch(path.name):
                print(f"[WARN] preserving staged document asset for recovery: {path}")
                continue
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


def configure_sqlite_connection(
    conn: sqlite3.Connection,
    *,
    foreign_keys: bool,
    synchronous: str,
) -> None:
    if synchronous == SQLITE_MAIN_SYNCHRONOUS:
        conn.execute("PRAGMA synchronous = FULL")
    elif synchronous == SQLITE_DERIVED_SYNCHRONOUS:
        conn.execute("PRAGMA synchronous = NORMAL")
    else:
        raise ValueError(f"Unsupported SQLite synchronous mode: {synchronous}")
    conn.execute("PRAGMA busy_timeout = 5000")
    conn.execute("PRAGMA temp_store = MEMORY")
    conn.execute("PRAGMA cache_size = -20000")
    conn.execute("PRAGMA mmap_size = 268435456")
    if foreign_keys:
        conn.execute("PRAGMA foreign_keys = ON")


def configure_sqlite_storage(conn: sqlite3.Connection) -> None:
    """Match the server's durable WAL storage layout for rebuilt databases."""
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA wal_autocheckpoint = 1000")


def checkpoint_database_for_swap(conn: sqlite3.Connection) -> None:
    """Flush a temporary WAL before its base database file is swapped in.

    ``replace_databases_from_temp`` deliberately moves only the base ``.db``
    files.  A clean connection close normally checkpoints WAL, but relying on
    that implicit behavior could discard an uncheckpointed rebuild after a
    busy/failed close.  Refuse the swap unless the explicit checkpoint finishes.
    """
    row = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
    if row is None:
        raise RuntimeError("WAL checkpoint returned no status")
    busy, log_frames, checkpointed_frames = (int(row[index]) for index in range(3))
    if busy or log_frames != checkpointed_frames:
        raise RuntimeError(
            "temporary database WAL checkpoint did not complete "
            f"(busy={busy}, log={log_frames}, checkpointed={checkpointed_frames})"
        )


def connect_db(
    path: Path,
    *,
    foreign_keys: bool = True,
    synchronous: str = SQLITE_MAIN_SYNCHRONOUS,
) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    configure_sqlite_connection(
        conn,
        foreign_keys=foreign_keys,
        synchronous=synchronous,
    )
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
            updated_at TEXT NOT NULL,
            content_mtime_ns INTEGER NOT NULL DEFAULT 0,
            content_size INTEGER NOT NULL DEFAULT -1
        )
        """
    )
    doc_columns = {
        str(row["name"])
        for row in conn.execute("PRAGMA table_info(docs)").fetchall()
    }
    if "content_mtime_ns" not in doc_columns:
        conn.execute(
            "ALTER TABLE docs ADD COLUMN content_mtime_ns INTEGER NOT NULL DEFAULT 0"
        )
    if "content_size" not in doc_columns:
        conn.execute(
            "ALTER TABLE docs ADD COLUMN content_size INTEGER NOT NULL DEFAULT -1"
        )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS wiki_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """
    )
    conn.execute(
        "INSERT OR IGNORE INTO wiki_meta (key, value) VALUES ('corpus_revision', '0')"
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
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_docs_updated_title "
        "ON docs (updated_at DESC, title COLLATE NOCASE)"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_doc_tags_tag_doc ON doc_tags (tag_id, doc_id)")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_doc_refs_title_lookup "
        "ON doc_references (target_title_key, source_doc_id, ref_type)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_doc_refs_slug_lookup "
        "ON doc_references (target_slug_key, source_doc_id, ref_type)"
    )
    for index_name in (
        "idx_docs_slug",
        "idx_doc_tags_tag_id",
        "idx_doc_refs_title_key",
        "idx_doc_refs_slug_key",
        "idx_doc_refs_source",
    ):
        conn.execute(f"DROP INDEX IF EXISTS {index_name}")
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


def database_family_paths() -> tuple[Path, Path, Path]:
    return DB_PATH, FTS_DB_PATH, TOKEN_DB_PATH


def swap_manifest_path() -> Path:
    return DATA_DIR / "wiki.db-swap-recovery.json"


def write_swap_manifest(backup_dir: Path, *, phase: str) -> None:
    if phase not in {"moving_old", "installing_new"}:
        raise ValueError(f"Unsupported database swap phase: {phase}")
    write_text_atomic(
        swap_manifest_path(),
        json.dumps(
            {
                "version": 1,
                "backup_dir": backup_dir.name,
                "phase": phase,
            },
            ensure_ascii=False,
        ),
    )


def read_swap_manifest() -> tuple[Path, str] | None:
    manifest_path = swap_manifest_path()
    if not manifest_path.exists():
        return None
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RuntimeError("database swap recovery manifest is unreadable") from error
    if not isinstance(payload, dict) or payload.get("version") != 1:
        raise RuntimeError("database swap recovery manifest has an unsupported format")

    backup_name = payload.get("backup_dir")
    phase = payload.get("phase")
    if not isinstance(backup_name, str) or Path(backup_name).name != backup_name:
        raise RuntimeError("database swap recovery manifest has an invalid backup path")
    if phase not in {"moving_old", "installing_new"}:
        raise RuntimeError("database swap recovery manifest has an invalid phase")

    backup_root = (DATA_DIR / "db_backups").resolve()
    backup_dir = (backup_root / backup_name).resolve()
    if backup_dir.parent != backup_root or not backup_dir.is_dir():
        raise RuntimeError("database swap backup directory is unavailable")
    return backup_dir, phase


def move_existing_sqlite_family(path: Path, backup_dir: Path) -> None:
    for related in sqlite_related_paths(path):
        if related.exists():
            related.replace(backup_dir / related.name)


def _remove_sqlite_family_best_effort(path: Path, errors: list[str]) -> None:
    for related in sqlite_related_paths(path):
        try:
            if related.exists():
                related.unlink()
        except OSError as error:
            errors.append(f"could not remove {related.name}: {error}")


def recover_incomplete_database_swap() -> bool:
    """Roll back a crash-interrupted three-database swap from its backup.

    SQLite file-family replacement cannot be one filesystem transaction.  The
    manifest is written before the old files move and survives a process/power
    failure.  During installation we prefer the prior complete family over a
    potentially mixed new/old set; document markdown remains the canonical
    source and DBFix will rebuild a fresh set immediately afterwards.
    """
    manifest = read_swap_manifest()
    if manifest is None:
        return False
    backup_dir, phase = manifest
    errors: list[str] = []

    if phase == "installing_new":
        for path in database_family_paths():
            has_backup = any((backup_dir / related.name).exists() for related in sqlite_related_paths(path))
            if has_backup:
                _remove_sqlite_family_best_effort(path, errors)

    for path in database_family_paths():
        for original in sqlite_related_paths(path):
            backup = backup_dir / original.name
            if not backup.exists() or original.exists():
                continue
            try:
                backup.replace(original)
            except OSError as error:
                errors.append(f"could not restore {original.name}: {error}")

    if errors:
        raise RuntimeError(
            "database swap recovery is incomplete; retained manifest and backup: "
            + "; ".join(errors)
        )

    try:
        swap_manifest_path().unlink(missing_ok=True)
    except OSError as error:
        raise RuntimeError("database swap recovered but manifest cleanup failed") from error
    return True


def ensure_temp_database_families_are_checkpointed(paths: tuple[Path, Path, Path]) -> None:
    for path in paths:
        remaining = [related.name for related in sqlite_related_paths(path)[1:] if related.exists()]
        if remaining:
            raise RuntimeError(
                f"temporary database still has uncheckpointed sidecars: {', '.join(remaining)}"
            )


def replace_databases_from_temp(temp_main: Path, temp_fts: Path, temp_token: Path) -> Path:
    backup_root = DATA_DIR / "db_backups"
    backup_root.mkdir(parents=True, exist_ok=True)
    backup_dir = backup_root / f"{time.strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex}"
    backup_dir.mkdir(exist_ok=False)
    temp_paths = (temp_main, temp_fts, temp_token)
    destination_paths = database_family_paths()
    ensure_temp_database_families_are_checkpointed(temp_paths)
    write_swap_manifest(backup_dir, phase="moving_old")

    try:
        for path in destination_paths:
            move_existing_sqlite_family(path, backup_dir)

        write_swap_manifest(backup_dir, phase="installing_new")
        for source, destination in zip(temp_paths, destination_paths):
            source.replace(destination)

        swap_manifest_path().unlink(missing_ok=True)
        return backup_dir
    except BaseException as error:
        try:
            recover_incomplete_database_swap()
        except Exception as recovery_error:
            raise RuntimeError(
                "database swap failed and rollback needs recovery on the next DBFix run"
            ) from recovery_error
        raise


def rebuild_from_doc_dir(main_db_path: Path, fts_db_path: Path, token_db_path: Path) -> tuple[int, int, int]:
    imported = 0
    skipped = 0
    token_terms = 0

    with (
        closing(connect_db(main_db_path)) as main_conn,
        closing(
            connect_db(
                fts_db_path,
                foreign_keys=False,
                synchronous=SQLITE_DERIVED_SYNCHRONOUS,
            )
        ) as fts_conn,
        closing(
            connect_db(
                token_db_path,
                foreign_keys=False,
                synchronous=SQLITE_DERIVED_SYNCHRONOUS,
            )
        ) as token_conn,
    ):
        for connection in (main_conn, fts_conn, token_conn):
            configure_sqlite_storage(connection)
        init_main_db(main_conn)
        init_fts_db(fts_conn)
        ensure_language_token_tables(token_conn)

        connections = (main_conn, fts_conn, token_conn)
        for index, md_file in enumerate(sorted(DOC_DIR.glob("*.md"))):
            slug = md_file.stem
            sidecar = read_json_dict(JSON_DIR / f"{slug}.json")
            savepoint = f"document_{index}"
            for conn in connections:
                conn.execute(f"SAVEPOINT {savepoint}")
            try:
                content = read_text_normalized(md_file)
                file_stat = md_file.stat()

                sidecar_title = str(sidecar.get("title", "")).strip()
                title_candidate = sidecar_title or infer_title_from_content(content, slug) or slug
                title = ensure_unique_title(main_conn, title_candidate)

                updated_at = iso_from_timestamp(file_stat.st_mtime)
                created_at_raw = str(sidecar.get("created_at") or "").strip()
                created_at = created_at_raw or updated_at

                meta_value = sidecar.get("meta")
                meta = meta_value if isinstance(meta_value, dict) else {}
                meta["sidecar"] = f"json/{slug}.json"

                main_conn.execute(
                    """
                    INSERT INTO docs
                    (title, slug, file_path, meta_json, created_at, updated_at, content_mtime_ns, content_size)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        title,
                        slug,
                        str(md_file),
                        json.dumps(meta, ensure_ascii=False),
                        created_at,
                        updated_at,
                        int(file_stat.st_mtime_ns),
                        int(file_stat.st_size),
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
                for conn in connections:
                    conn.execute(f"RELEASE SAVEPOINT {savepoint}")
                imported += 1
            except Exception as error:
                for conn in reversed(connections):
                    conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
                    conn.execute(f"RELEASE SAVEPOINT {savepoint}")
                skipped += 1
                print(f"[WARN] skipped {md_file.name}: {error}")

        main_conn.execute(
            "UPDATE wiki_meta SET value = ? WHERE key = 'corpus_revision'",
            (str(imported),),
        )
        main_conn.execute(
            "INSERT INTO wiki_meta (key, value) VALUES ('file_state_v1', '1') "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value"
        )
        _token_docs, token_terms = rebuild_language_token_index(token_conn, main_conn, fts_conn)
        mark_fts_index_current(main_conn, fts_conn)
        main_conn.commit()
        fts_conn.commit()
        token_conn.commit()
        for connection in connections:
            checkpoint_database_for_swap(connection)
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
            if recover_incomplete_database_swap():
                print("[WARN] rolled back an interrupted database swap from its backup")
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
