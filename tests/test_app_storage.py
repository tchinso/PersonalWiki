from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from pathlib import Path

os.environ["PERSONALWIKI_SKIP_BOOTSTRAP"] = "1"

import app


class AppStorageTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.old_doc_dir = app.DOC_DIR
        self.old_json_dir = app.JSON_DIR
        root = Path(self.temp_dir.name)
        app.DOC_DIR = root / "doc"
        app.JSON_DIR = app.DOC_DIR / "json"
        app.JSON_DIR.mkdir(parents=True)

    def tearDown(self) -> None:
        app.DOC_DIR = self.old_doc_dir
        app.JSON_DIR = self.old_json_dir
        self.temp_dir.cleanup()

    def write_assets(self, slug: str) -> None:
        app.write_document(slug, "body")
        app.write_sidecar(
            slug=slug,
            title="Title",
            created_at="2026-01-01T00:00:00",
            updated_at="2026-01-01T00:00:00",
            tags=["tag"],
            meta={"sidecar": f"json/{slug}.json"},
        )

    def test_staged_delete_can_be_rolled_back(self) -> None:
        self.write_assets("sample")
        staged = app.stage_document_assets_for_delete("sample")
        self.assertFalse(app.document_path("sample").exists())
        self.assertFalse(app.sidecar_path("sample").exists())
        self.assertTrue(all(staged_path.exists() for _old_path, staged_path in staged))

        app.rollback_document_asset_moves(staged)

        self.assertEqual("body", app.read_document("sample"))
        self.assertTrue(app.sidecar_path("sample").exists())

    def test_staged_delete_finalize_removes_staged_files(self) -> None:
        self.write_assets("sample")
        staged = app.stage_document_assets_for_delete("sample")

        app.finalize_staged_asset_deletes(staged)

        self.assertFalse(app.document_path("sample").exists())
        self.assertFalse(app.sidecar_path("sample").exists())
        self.assertTrue(all(not staged_path.exists() for _old_path, staged_path in staged))

    def test_cleanup_stale_temp_files_removes_only_temp_artifacts(self) -> None:
        app.DOC_DIR.mkdir(parents=True, exist_ok=True)
        app.JSON_DIR.mkdir(parents=True, exist_ok=True)
        stale_doc = app.DOC_DIR / ".sample.md.delete-1-0.tmp"
        stale_sidecar = app.JSON_DIR / ".sample.json.abc.tmp"
        real_doc = app.DOC_DIR / "sample.md"
        stale_doc.write_text("stale", encoding="utf-8")
        stale_sidecar.write_text("stale", encoding="utf-8")
        real_doc.write_text("real", encoding="utf-8")

        removed = app.cleanup_stale_temp_files()

        self.assertEqual(2, removed)
        self.assertFalse(stale_doc.exists())
        self.assertFalse(stale_sidecar.exists())
        self.assertTrue(real_doc.exists())


class AppTagMapTests(unittest.TestCase):
    def test_build_doc_tag_map_chunks_large_doc_id_sets(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        try:
            conn.executescript(
                """
                CREATE TABLE tags (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL COLLATE NOCASE UNIQUE
                );
                CREATE TABLE doc_tags (
                    doc_id INTEGER NOT NULL,
                    tag_id INTEGER NOT NULL,
                    PRIMARY KEY (doc_id, tag_id)
                );
                INSERT INTO tags (id, name) VALUES (1, 'bulk');
                """
            )
            conn.executemany(
                "INSERT INTO doc_tags (doc_id, tag_id) VALUES (?, 1)",
                [(doc_id,) for doc_id in range(1, app.SQLITE_IN_CLAUSE_CHUNK_SIZE + 6)],
            )

            tag_map = app.build_doc_tag_map(
                conn,
                list(range(1, app.SQLITE_IN_CLAUSE_CHUNK_SIZE + 6)),
            )
        finally:
            conn.close()

        self.assertEqual(app.SQLITE_IN_CLAUSE_CHUNK_SIZE + 5, len(tag_map))
        self.assertEqual(["bulk"], tag_map[1])
        self.assertEqual(["bulk"], tag_map[app.SQLITE_IN_CLAUSE_CHUNK_SIZE + 5])


class AppSearchTests(unittest.TestCase):
    def test_extract_tag_search_terms_is_bounded(self) -> None:
        query = " ".join(f"tag{index}" for index in range(app.TAG_SEARCH_TERM_LIMIT + 10))

        terms = app.extract_tag_search_terms(query)

        self.assertEqual(app.TAG_SEARCH_TERM_LIMIT, len(terms))
        self.assertEqual("tag0", terms[0])
        self.assertEqual(f"tag{app.TAG_SEARCH_TERM_LIMIT - 1}", terms[-1])

    def test_search_docs_by_tags_returns_empty_for_non_positive_limit(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        try:
            conn.executescript(
                """
                CREATE TABLE docs (
                    id INTEGER PRIMARY KEY,
                    title TEXT NOT NULL,
                    slug TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE tags (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
                CREATE TABLE doc_tags (doc_id INTEGER NOT NULL, tag_id INTEGER NOT NULL);
                INSERT INTO docs (id, title, slug, updated_at)
                VALUES (1, 'Doc', 'doc', '2026-01-01T00:00:00');
                INSERT INTO tags (id, name) VALUES (1, 'tag');
                INSERT INTO doc_tags (doc_id, tag_id) VALUES (1, 1);
                """
            )
            self.assertEqual([], app.search_docs_by_tags(conn, ["tag"], limit=0))
        finally:
            conn.close()


class AppBootstrapSchemaTests(unittest.TestCase):
    def test_bootstrap_schema_creates_indexes_and_norms(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            old_paths = (
                app.DATA_DIR,
                app.DOC_DIR,
                app.JSON_DIR,
                app.IMG_DIR,
                app.FILE_DIR,
                app.DB_PATH,
                app.FTS_DB_PATH,
                app.TOKEN_DB_PATH,
                app.DATA_LOCK_PATH,
            )
            try:
                app.DATA_DIR = root
                app.DOC_DIR = root / "doc"
                app.JSON_DIR = app.DOC_DIR / "json"
                app.IMG_DIR = root / "img"
                app.FILE_DIR = root / "file"
                app.DB_PATH = root / "wiki.db"
                app.FTS_DB_PATH = root / "wiki_fts.db"
                app.TOKEN_DB_PATH = root / "wiki_token.db"
                app.DATA_LOCK_PATH = root / "wiki.lock"

                app.init_storage()
                app.init_db()
                app.init_fts_db()
                app.init_token_db()
                app.ensure_default_home()

                db = app.connect_db()
                token_db = app.connect_token_db()
                try:
                    indexes = {
                        str(row["name"])
                        for row in db.execute("SELECT name FROM sqlite_master WHERE type = 'index'")
                        if row["name"]
                    }
                    norm_count = token_db.execute(
                        "SELECT COUNT(*) AS c FROM language_doc_norms"
                    ).fetchone()["c"]
                finally:
                    db.close()
                    token_db.close()
            finally:
                (
                    app.DATA_DIR,
                    app.DOC_DIR,
                    app.JSON_DIR,
                    app.IMG_DIR,
                    app.FILE_DIR,
                    app.DB_PATH,
                    app.FTS_DB_PATH,
                    app.TOKEN_DB_PATH,
                    app.DATA_LOCK_PATH,
                ) = old_paths

        self.assertIn("idx_docs_updated_title", indexes)
        self.assertIn("idx_doc_tags_tag_doc", indexes)
        self.assertEqual(1, norm_count)


if __name__ == "__main__":
    unittest.main()
