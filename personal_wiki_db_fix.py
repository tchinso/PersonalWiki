from __future__ import annotations

import json
import re
import sqlite3
import sys
from datetime import datetime
from pathlib import Path


def runtime_data_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


DATA_DIR = runtime_data_dir()
DOC_DIR = DATA_DIR / "doc"
DB_PATH = DATA_DIR / "wiki.db"
FTS_DB_PATH = DATA_DIR / "wiki_fts.db"


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def normalize_newlines(text: str) -> str:
    if not text:
        return ""
    normalized = text.replace("\r\r\n", "\n")
    normalized = normalized.replace("\r\n", "\n").replace("\r", "\n")
    return normalized


def read_text_normalized(path: Path) -> str:
    with path.open("r", encoding="utf-8", newline="") as file:
        return normalize_newlines(file.read())


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


def dedupe_tags(tags: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for raw in tags:
        tag = str(raw).strip()
        if not tag:
            continue
        key = tag.casefold()
        if key in seen:
            continue
        seen.add(key)
        result.append(tag)
    return result


def ensure_unique_title(conn: sqlite3.Connection, title: str) -> str:
    base = title.strip() or "untitled"
    candidate = base
    suffix = 2
    while conn.execute(
        "SELECT 1 FROM docs WHERE title = ? COLLATE NOCASE",
        (candidate,),
    ).fetchone():
        candidate = f"{base} ({suffix})"
        suffix += 1
    return candidate


def connect_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
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
    conn.execute("CREATE INDEX IF NOT EXISTS idx_docs_slug ON docs (slug)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_doc_tags_tag_id ON doc_tags (tag_id)")
    conn.execute("DROP TABLE IF EXISTS docs_fts")


def init_fts_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE VIRTUAL TABLE IF NOT EXISTS docs_fts
        USING fts5(title, content)
        """
    )


def set_doc_tags(conn: sqlite3.Connection, doc_id: int, tags: list[str]) -> None:
    conn.execute("DELETE FROM doc_tags WHERE doc_id = ?", (doc_id,))
    for tag in tags:
        conn.execute("INSERT INTO tags (name) VALUES (?) ON CONFLICT(name) DO NOTHING", (tag,))
        row = conn.execute("SELECT id FROM tags WHERE name = ?", (tag,)).fetchone()
        if row is None:
            continue
        conn.execute(
            "INSERT OR IGNORE INTO doc_tags (doc_id, tag_id) VALUES (?, ?)",
            (doc_id, row["id"]),
        )
    conn.execute("DELETE FROM tags WHERE id NOT IN (SELECT DISTINCT tag_id FROM doc_tags)")


def update_fts(conn: sqlite3.Connection, doc_id: int, title: str, content: str) -> None:
    conn.execute("DELETE FROM docs_fts WHERE rowid = ?", (doc_id,))
    conn.execute(
        "INSERT INTO docs_fts (rowid, title, content) VALUES (?, ?, ?)",
        (doc_id, title, content),
    )


def collect_tags(sidecar: dict) -> list[str]:
    raw = sidecar.get("tags")
    if isinstance(raw, list):
        return dedupe_tags([str(tag) for tag in raw])
    if isinstance(raw, str):
        return dedupe_tags(raw.split(","))
    return []


def recreate_databases() -> tuple[int, int]:
    DOC_DIR.mkdir(parents=True, exist_ok=True)

    if DB_PATH.exists():
        DB_PATH.unlink()
    if FTS_DB_PATH.exists():
        FTS_DB_PATH.unlink()

    imported = 0
    skipped = 0

    with connect_db(DB_PATH) as main_conn, connect_db(FTS_DB_PATH) as fts_conn:
        init_main_db(main_conn)
        init_fts_db(fts_conn)

        for md_file in sorted(DOC_DIR.glob("*.md")):
            slug = md_file.stem
            json_file = DOC_DIR / f"{slug}.json"

            try:
                content = read_text_normalized(md_file)
                sidecar = read_json_dict(json_file)
                title = ensure_unique_title(
                    main_conn,
                    str(sidecar.get("title", "")).strip() or infer_title_from_content(content, slug) or slug,
                )

                created_at = str(sidecar.get("created_at") or now_iso())
                updated_at = str(sidecar.get("updated_at") or created_at)

                meta = sidecar.get("meta")
                meta_dict = meta if isinstance(meta, dict) else {}
                meta_dict["sidecar"] = f"{slug}.json"

                main_conn.execute(
                    """
                    INSERT INTO docs (title, slug, file_path, meta_json, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        title,
                        slug,
                        str(md_file),
                        json.dumps(meta_dict, ensure_ascii=False),
                        created_at,
                        updated_at,
                    ),
                )
                row = main_conn.execute("SELECT id FROM docs WHERE slug = ?", (slug,)).fetchone()
                if row is None:
                    raise RuntimeError("Failed to read inserted doc id.")

                doc_id = int(row["id"])
                tags = collect_tags(sidecar)
                set_doc_tags(main_conn, doc_id, tags)
                update_fts(fts_conn, doc_id, title, content)
                imported += 1
            except Exception as error:
                skipped += 1
                print(f"[WARN] Skipped {md_file.name}: {error}")

        main_conn.commit()
        fts_conn.commit()

    return imported, skipped


def main() -> int:
    print("PersonalWiki DB fixer")
    print(f"Data directory: {DATA_DIR}")
    print(f"Doc directory: {DOC_DIR}")
    print(f"Main DB: {DB_PATH}")
    print(f"FTS DB: {FTS_DB_PATH}")

    try:
        imported, skipped = recreate_databases()
    except Exception as error:
        print(f"[ERROR] Failed to rebuild databases: {error}")
        return 1

    print(f"[OK] Recreated databases from /doc: imported={imported}, skipped={skipped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
