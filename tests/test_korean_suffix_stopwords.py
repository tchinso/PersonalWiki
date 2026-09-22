from __future__ import annotations

import sqlite3
import unittest
from collections import Counter

import language_tools as lt


class KoreanSuffixStopwordTests(unittest.TestCase):
    def test_legacy_particles_and_endings_are_kept_out_of_general_stopwords(self) -> None:
        migrated = {
            "에서",
            "으로",
            "입니다",
            "있는",
            "하는",
            "합니다",
            "대한",
            "통해",
            "같은",
            "위해",
            "위한",
            "중에서",
            "대해서",
            "대하여",
            "위의",
            "있습니다",
            "없습니다",
            "같습니다",
            "있어",
            "없는",
            "있고",
            "없고",
            "된다",
            "되는",
            "하면",
            "해서",
            "하고",
            "한다",
            "처럼",
            "보다",
            "까지",
            "부터",
            "을",
            "를",
        }
        self.assertTrue(migrated <= lt.KOREAN_ENDING_STOPWORDS)
        self.assertFalse(migrated & lt.KOREAN_STOPWORDS)

    def test_particles_are_removed_only_from_the_end_of_a_word(self) -> None:
        self.assertEqual(lt.remove_korean_stopwords_aggressively("문서화"), "화")
        self.assertEqual(lt.remove_korean_ending_stopwords_aggressively("을바람"), "을바람")
        self.assertEqual(lt.remove_korean_ending_stopwords_aggressively("바람을"), "바람")

    def test_stacked_particles_and_endings_reduce_to_a_shared_stem(self) -> None:
        cases = {
            "전문가": "전문",
            "전문가이고": "전문",
            "전문가이면": "전문",
            "전문가에게서는": "전문",
            "처리하면": "처리",
            "처리하고": "처리",
            "처리합니다": "처리",
            "이동하거나": "이동",
            "달라졌으므로": "달라졌",
            "순서대로": "순서",
        }
        for source, expected in cases.items():
            with self.subTest(source=source):
                self.assertEqual(lt.strip_korean_ending_stopwords(source), expected)

    def test_tokenizer_combines_general_and_suffix_stopword_filters(self) -> None:
        self.assertEqual(
            lt.tokenize_text("문서에서 기능을 사용합니다 전문가 김민수"),
            ["전문", "김민수"],
        )
        self.assertEqual(
            lt.tokenize_text("전문가이고 전문가이면"),
            ["전문", "전문"],
        )
        self.assertEqual(lt.tokenize_text("the Flask API"), ["flask", "api"])
        self.assertEqual(lt.tokenize_text("내가"), [])
        self.assertEqual(lt.tokenize_text("![[전문가.png]] 전문가"), ["전문"])

    def test_oversized_tokens_do_not_bloat_normalization_caches_or_index(self) -> None:
        korean_source = "전" * (lt.TAG_RECOMMEND_MAX_TOKEN_LENGTH + 20) + "이고"
        english_source = "a" * (lt.TAG_RECOMMEND_MAX_TOKEN_LENGTH + 20) + "s"
        lt._strip_korean_ending_stopwords_cached.cache_clear()
        lt._singularize_token_cached.cache_clear()

        self.assertEqual(
            lt.strip_korean_ending_stopwords(korean_source),
            korean_source[:-2],
        )
        self.assertEqual(lt.singularize_token(english_source), english_source[:-1])
        self.assertEqual(lt._strip_korean_ending_stopwords_cached.cache_info().currsize, 0)
        self.assertEqual(lt._singularize_token_cached.cache_info().currsize, 0)
        self.assertEqual(lt.tokenize_text(korean_source), [])
        self.assertEqual(lt.tokenize_text(english_source), [])

    def test_oversized_repeated_particle_has_a_bounded_strip_pass_count(self) -> None:
        prefix = "전" * (lt.TOKEN_NORMALIZATION_CACHE_MAX_LENGTH + 10)
        source = prefix + ("가" * (lt.KOREAN_ENDING_MAX_STRIPS_FOR_LONG_TOKEN + 8))
        self.assertEqual(
            lt.strip_korean_ending_stopwords(source),
            prefix + ("가" * 8),
        )

    def test_bounded_token_counter_preserves_ordinary_text_and_caps_unique_input(self) -> None:
        ordinary = "PersonalWiki design designs Korean 전문가이고"
        self.assertEqual(
            lt.count_bounded_text_tokens(
                ordinary,
                max_chars=lt.TAG_RECOMMEND_MAX_CONTENT_ANALYSIS_CHARS,
                max_tokens=lt.TAG_RECOMMEND_MAX_CONTENT_TOKENS,
            ),
            Counter(lt.tokenize_text(ordinary)),
        )
        self.assertEqual(
            lt.count_bounded_text_tokens("alpha beta", max_chars=5, max_tokens=10),
            Counter({"alpha": 1}),
        )

        unique_source = " ".join(
            f"term{index:05d}"
            for index in range(lt.TAG_RECOMMEND_MAX_CONTENT_TOKENS + 32)
        )
        raw_counter = lt.count_bounded_text_tokens(
            unique_source,
            max_chars=len(unique_source),
            max_tokens=lt.TAG_RECOMMEND_MAX_CONTENT_TOKENS,
        )
        self.assertEqual(sum(raw_counter.values()), lt.TAG_RECOMMEND_MAX_CONTENT_TOKENS)
        self.assertNotIn(f"term{lt.TAG_RECOMMEND_MAX_CONTENT_TOKENS + 31:05d}", raw_counter)

        counters = lt.compute_doc_token_counters("", unique_source)
        self.assertLessEqual(len(counters["title"]), lt.TAG_RECOMMEND_MAX_TITLE_TOKENS)
        self.assertLessEqual(len(counters["content"]), lt.TAG_RECOMMEND_MAX_TOKENS_PER_DOC)

    def test_adaptive_term_limit_preserves_ranked_prefix(self) -> None:
        counter = Counter({f"term{index}": (index % 4) + 1 for index in range(1_500)})
        expected = Counter(
            dict(
                sorted(counter.items(), key=lt.tag_recommend_token_sort_key)[
                    : lt.choose_content_token_limit(counter)
                ]
            )
        )
        self.assertEqual(lt.limit_tf_counter_adaptive(counter), expected)

    def test_token_table_uses_one_covering_lookup_index(self) -> None:
        conn = sqlite3.connect(":memory:")
        try:
            lt.ensure_language_token_tables(conn)
            index_names = {
                str(row[1])
                for row in conn.execute("PRAGMA index_list(language_doc_tokens)").fetchall()
            }
            self.assertIn("idx_language_doc_tokens_token_doc_tf", index_names)
            self.assertFalse(
                {
                    "idx_language_doc_tokens_token",
                    "idx_language_doc_tokens_token_tf",
                    "idx_language_doc_tokens_token_doc",
                }
                & index_names
            )
        finally:
            conn.close()

    def test_v6_tokenizer_version_forces_existing_index_rebuild(self) -> None:
        main_conn = sqlite3.connect(":memory:")
        main_conn.row_factory = sqlite3.Row
        fts_conn = sqlite3.connect(":memory:")
        fts_conn.row_factory = sqlite3.Row
        token_conn = sqlite3.connect(":memory:")
        token_conn.row_factory = sqlite3.Row
        try:
            main_conn.execute(
                """
                CREATE TABLE docs (
                    id INTEGER PRIMARY KEY,
                    title TEXT NOT NULL,
                    slug TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            main_conn.execute(
                """
                INSERT INTO docs (id, title, slug, updated_at)
                VALUES (1, '전문가', 'expert', '2026-09-22T00:00:00')
                """
            )
            main_conn.execute(
                """
                INSERT INTO docs (id, title, slug, updated_at)
                VALUES (2, '김민수', 'minsu', '2026-09-22T00:00:00')
                """
            )
            fts_conn.execute("CREATE VIRTUAL TABLE docs_fts USING fts5(title, content)")
            fts_conn.execute(
                "INSERT INTO docs_fts (rowid, title, content) VALUES (1, '전문가', '처리하면')"
            )
            fts_conn.execute(
                "INSERT INTO docs_fts (rowid, title, content) VALUES (2, '김민수', '데이터를')"
            )
            lt.ensure_language_token_tables(token_conn)
            token_conn.executemany(
                "INSERT INTO language_index_meta (key, value) VALUES (?, ?)",
                [
                    ("total_docs", "2"),
                    ("source_signature", lt.build_language_index_source_signature(main_conn)),
                    ("tokenizer_version", "tag-token-v6-korean-suffix-stopwords-bounded"),
                ],
            )

            # Startup sync can update a changed document before the final
            # index check.  A deferred update must not mask a stale tokenizer
            # version and leave untouched documents on the old tokenizer.
            lt.upsert_language_doc_tokens(
                token_conn,
                main_conn,
                1,
                "전문가",
                "처리하면",
                refresh_idf=False,
            )
            self.assertTrue(lt.language_token_index_needs_rebuild(token_conn, main_conn))
            rebuilt, total_docs, _token_count = lt.ensure_language_token_index_current(
                token_conn,
                main_conn,
                fts_conn,
            )
            self.assertTrue(rebuilt)
            self.assertEqual(total_docs, 2)
            self.assertFalse(lt.language_token_index_needs_rebuild(token_conn, main_conn))
            self.assertEqual(
                token_conn.execute(
                    "SELECT value FROM language_index_meta WHERE key = 'tokenizer_version'"
                ).fetchone()["value"],
                lt.TAG_RECOMMEND_TOKENIZER_VERSION,
            )
            self.assertEqual(
                {
                    str(row["token"])
                    for row in token_conn.execute("SELECT DISTINCT token FROM language_doc_tokens")
                },
                {"전문", "처리", "김민수", "데이터"},
            )
        finally:
            token_conn.close()
            fts_conn.close()
            main_conn.close()

    def test_recommend_tags_matches_the_shared_suffix_stem(self) -> None:
        main_conn = sqlite3.connect(":memory:")
        main_conn.row_factory = sqlite3.Row
        fts_conn = sqlite3.connect(":memory:")
        fts_conn.row_factory = sqlite3.Row
        token_conn = sqlite3.connect(":memory:")
        token_conn.row_factory = sqlite3.Row
        try:
            main_conn.executescript(
                """
                CREATE TABLE docs (
                    id INTEGER PRIMARY KEY,
                    title TEXT NOT NULL COLLATE NOCASE UNIQUE,
                    slug TEXT NOT NULL UNIQUE,
                    file_path TEXT NOT NULL,
                    meta_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE tags (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL COLLATE NOCASE UNIQUE
                );
                CREATE TABLE doc_tags (
                    doc_id INTEGER NOT NULL,
                    tag_id INTEGER NOT NULL,
                    PRIMARY KEY (doc_id, tag_id)
                );
                """
            )
            fts_conn.execute("CREATE VIRTUAL TABLE docs_fts USING fts5(title, content)")
            lt.ensure_language_token_tables(token_conn)

            documents = [
                ("전문가", "전문가가 설계한다", "expert-topic"),
                ("Cooking", "bread oven recipe", "cooking"),
                ("Music", "piano melody rhythm", "music"),
                ("Travel", "hotel airport itinerary", "travel"),
                ("Garden", "soil flower watering", "garden"),
            ]
            for doc_id, (title, content, tag) in enumerate(documents, start=1):
                slug = f"doc-{doc_id}"
                timestamp = f"2026-09-22T00:00:{doc_id:02d}"
                main_conn.execute(
                    """
                    INSERT INTO docs (id, title, slug, file_path, meta_json, created_at, updated_at)
                    VALUES (?, ?, ?, ?, '{}', ?, ?)
                    """,
                    (doc_id, title, slug, f"doc/{slug}.md", timestamp, timestamp),
                )
                main_conn.execute("INSERT INTO tags (name) VALUES (?)", (tag,))
                tag_id = int(
                    main_conn.execute("SELECT id FROM tags WHERE name = ?", (tag,)).fetchone()["id"]
                )
                main_conn.execute("INSERT INTO doc_tags (doc_id, tag_id) VALUES (?, ?)", (doc_id, tag_id))
                fts_conn.execute(
                    "INSERT INTO docs_fts (rowid, title, content) VALUES (?, ?, ?)",
                    (doc_id, title, content),
                )

            lt.rebuild_language_token_index(token_conn, main_conn, fts_conn)
            indexed_tokens = {
                str(row["token"])
                for row in token_conn.execute(
                    "SELECT token FROM language_doc_tokens WHERE doc_id = 1"
                )
            }
            self.assertEqual(lt.tokenize_text("전문가이고"), ["전문"])
            self.assertIn("전문", indexed_tokens)

            lt.invalidate_tag_recommendation_cache()
            self.assertEqual(
                lt.recommend_tags(
                    main_conn,
                    fts_conn,
                    token_conn,
                    title="전문가이고",
                    content="",
                    current_slug="draft-shared-suffix-smoke",
                    limit=3,
                ),
                ["expert-topic"],
            )
        finally:
            lt.invalidate_tag_recommendation_cache()
            token_conn.close()
            fts_conn.close()
            main_conn.close()


if __name__ == "__main__":
    unittest.main()
