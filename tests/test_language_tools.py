from __future__ import annotations

import sqlite3
import unittest

import language_tools as lt


def connect_memory() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    return conn


def init_main(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE docs (
            id INTEGER PRIMARY KEY,
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
        CREATE TABLE tags (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL COLLATE NOCASE UNIQUE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE doc_tags (
            doc_id INTEGER NOT NULL,
            tag_id INTEGER NOT NULL,
            PRIMARY KEY (doc_id, tag_id)
        )
        """
    )


def init_fts(conn: sqlite3.Connection) -> None:
    conn.execute("CREATE VIRTUAL TABLE docs_fts USING fts5(title, content)")


def add_doc(
    main_conn: sqlite3.Connection,
    fts_conn: sqlite3.Connection,
    token_conn: sqlite3.Connection,
    *,
    doc_id: int,
    title: str,
    slug: str,
    content: str,
    tags: list[str],
) -> None:
    timestamp = f"2026-01-01T00:00:{doc_id:02d}"
    main_conn.execute(
        """
        INSERT INTO docs (id, title, slug, file_path, meta_json, created_at, updated_at)
        VALUES (?, ?, ?, ?, '{}', ?, ?)
        """,
        (doc_id, title, slug, f"doc/{slug}.md", timestamp, timestamp),
    )
    for tag in tags:
        main_conn.execute("INSERT OR IGNORE INTO tags (name) VALUES (?)", (tag,))
        row = main_conn.execute("SELECT id FROM tags WHERE name = ? COLLATE NOCASE", (tag,)).fetchone()
        main_conn.execute(
            "INSERT OR IGNORE INTO doc_tags (doc_id, tag_id) VALUES (?, ?)",
            (doc_id, int(row["id"])),
        )
    fts_conn.execute(
        "INSERT INTO docs_fts (rowid, title, content) VALUES (?, ?, ?)",
        (doc_id, title, content),
    )
    lt.upsert_language_doc_tokens(token_conn, main_conn, doc_id, title, content)


class LanguageRecommendationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.main_conn = connect_memory()
        self.fts_conn = connect_memory()
        self.token_conn = connect_memory()
        init_main(self.main_conn)
        init_fts(self.fts_conn)
        lt.ensure_language_token_tables(self.token_conn)
        add_doc(
            self.main_conn,
            self.fts_conn,
            self.token_conn,
            doc_id=1,
            title="Flask API routing",
            slug="flask-api-routing",
            content="Python Flask API routing request response sqlite backend",
            tags=["backend", "flask"],
        )
        add_doc(
            self.main_conn,
            self.fts_conn,
            self.token_conn,
            doc_id=2,
            title="Sourdough starter",
            slug="sourdough-starter",
            content="Bread starter oven flour fermentation kitchen recipe",
            tags=["cooking"],
        )
        add_doc(
            self.main_conn,
            self.fts_conn,
            self.token_conn,
            doc_id=3,
            title="SQLite indexing",
            slug="sqlite-indexing",
            content="Python sqlite query indexing database performance backend",
            tags=["backend", "database"],
        )

    def tearDown(self) -> None:
        self.main_conn.close()
        self.fts_conn.close()
        self.token_conn.close()

    def test_doc_norms_are_created_for_indexed_documents(self) -> None:
        rows = self.token_conn.execute(
            "SELECT doc_id, norm, token_count FROM language_doc_norms ORDER BY doc_id"
        ).fetchall()
        self.assertEqual([1, 2, 3], [int(row["doc_id"]) for row in rows])
        self.assertTrue(all(float(row["norm"]) > 0 for row in rows))
        self.assertTrue(all(int(row["token_count"]) > 0 for row in rows))

    def test_recommend_tags_uses_indexed_similarity(self) -> None:
        recommendations = lt.recommend_tags(
            self.main_conn,
            self.fts_conn,
            self.token_conn,
            title="Flask sqlite backend",
            content="API routing database query performance",
            limit=5,
        )
        self.assertIn("backend", recommendations)
        self.assertIn("database", recommendations)

    def test_recommend_tags_returns_empty_for_non_positive_limit(self) -> None:
        recommendations = lt.recommend_tags(
            self.main_conn,
            self.fts_conn,
            self.token_conn,
            title="Flask sqlite backend",
            content="API routing database query performance",
            limit=0,
        )
        self.assertEqual([], recommendations)

    def test_recommendation_refreshes_missing_norm_cache(self) -> None:
        self.token_conn.execute("DELETE FROM language_doc_norms WHERE doc_id = 1")
        self.assertTrue(lt.language_doc_norms_need_refresh(self.token_conn))
        recommendations = lt.recommend_tags(
            self.main_conn,
            self.fts_conn,
            self.token_conn,
            title="Flask backend API",
            content="routing request response",
            limit=5,
        )
        self.assertIn("backend", recommendations)
        self.assertFalse(lt.language_doc_norms_need_refresh(self.token_conn))

    def test_large_corpus_guard_does_not_require_full_scan(self) -> None:
        original_limit = lt.TAG_RECOMMEND_FULL_SCAN_MAX_DOCS
        try:
            lt.TAG_RECOMMEND_FULL_SCAN_MAX_DOCS = 1
            recommendations = lt.recommend_tags(
                self.main_conn,
                self.fts_conn,
                self.token_conn,
                title="starter oven",
                content="fermentation recipe flour",
                limit=5,
            )
        finally:
            lt.TAG_RECOMMEND_FULL_SCAN_MAX_DOCS = original_limit
        self.assertEqual(["cooking"], recommendations)


if __name__ == "__main__":
    unittest.main()
