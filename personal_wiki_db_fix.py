from __future__ import annotations

import json
import re
import sqlite3
import sys
import unicodedata
from datetime import datetime
from pathlib import Path

from markdown_engine import extract_reference_targets


def runtime_data_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


DATA_DIR = runtime_data_dir()
DOC_DIR = DATA_DIR / "doc"
DB_PATH = DATA_DIR / "wiki.db"
FTS_DB_PATH = DATA_DIR / "wiki_fts.db"


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


def init_fts_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE VIRTUAL TABLE IF NOT EXISTS docs_fts
        USING fts5(title, content)
        """
    )


def set_doc_tags(conn: sqlite3.Connection, doc_id: int, tags: list[str]) -> None:
    conn.execute("DELETE FROM doc_tags WHERE doc_id = ?", (doc_id,))
    normalized_tags = parse_tags(",".join(str(tag) for tag in tags))
    if normalized_tags:
        conn.executemany(
            "INSERT INTO tags (name) VALUES (?) ON CONFLICT(name) DO NOTHING",
            [(tag,) for tag in normalized_tags],
        )
        name_filters = " OR ".join("name = ? COLLATE NOCASE" for _ in normalized_tags)
        rows = conn.execute(
            f"SELECT id FROM tags WHERE {name_filters}",
            normalized_tags,
        ).fetchall()
        if rows:
            conn.executemany(
                "INSERT OR IGNORE INTO doc_tags (doc_id, tag_id) VALUES (?, ?)",
                [(doc_id, int(row["id"])) for row in rows],
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
    sidecar_path = DOC_DIR / f"{slug}.json"
    sidecar_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
        newline="\n",
    )


def collect_sidecar_tags(sidecar: dict) -> list[str]:
    raw = sidecar.get("tags")
    if isinstance(raw, list):
        return parse_tags(",".join(str(item) for item in raw))
    if isinstance(raw, str):
        return parse_tags(raw)
    return []


def rebuild_from_doc_dir() -> tuple[int, int]:
    imported = 0
    skipped = 0

    with connect_db(DB_PATH) as main_conn, connect_db(FTS_DB_PATH) as fts_conn:
        init_main_db(main_conn)
        init_fts_db(fts_conn)

        for md_file in sorted(DOC_DIR.glob("*.md")):
            slug = md_file.stem
            sidecar = read_json_dict(DOC_DIR / f"{slug}.json")
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
                meta["sidecar"] = f"{slug}.json"

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

        main_conn.commit()
        fts_conn.commit()
    return imported, skipped


def recreate_databases() -> tuple[int, int]:
    DOC_DIR.mkdir(parents=True, exist_ok=True)
    if DB_PATH.exists():
        DB_PATH.unlink()
    if FTS_DB_PATH.exists():
        FTS_DB_PATH.unlink()
    return rebuild_from_doc_dir()


def main() -> int:
    print("PersonalWiki DB fixer")
    print(f"Data directory: {DATA_DIR}")
    print(f"Doc directory: {DOC_DIR}")
    print(f"Main DB: {DB_PATH}")
    print(f"FTS DB: {FTS_DB_PATH}")

    try:
        imported, skipped = recreate_databases()
    except Exception as error:
        print(f"[ERROR] failed to rebuild databases: {error}")
        return 1

    print(f"[OK] recreated databases from /doc: imported={imported}, skipped={skipped}")
    print("[OK] sidecar JSON now includes normalized backlink reference cache.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
