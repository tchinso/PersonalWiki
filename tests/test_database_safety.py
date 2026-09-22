from __future__ import annotations

import os
import sqlite3
import subprocess
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

os.environ["PERSONALWIKI_SKIP_BOOTSTRAP"] = "1"

import app
import personal_wiki_db_fix as db_fix


class FailingCommitConnection:
    """Delegate all SQLite operations but fail exactly one derived commit."""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self.connection = connection

    def commit(self) -> None:
        raise sqlite3.OperationalError("simulated derived-index commit failure")

    def __getattr__(self, name: str):
        return getattr(self.connection, name)


class DatabaseSafetyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.original_app_paths = {
            name: getattr(app, name)
            for name in (
                "DATA_DIR",
                "DOC_DIR",
                "JSON_DIR",
                "IMG_DIR",
                "FILE_DIR",
                "DB_PATH",
                "FTS_DB_PATH",
                "TOKEN_DB_PATH",
                "DATA_LOCK_PATH",
                "SETTINGS_PATH",
            )
        }
        self.original_db_fix_paths = {
            name: getattr(db_fix, name)
            for name in (
                "DATA_DIR",
                "DOC_DIR",
                "JSON_DIR",
                "DB_PATH",
                "FTS_DB_PATH",
                "TOKEN_DB_PATH",
                "DATA_LOCK_PATH",
            )
        }

        app.DATA_DIR = root / "server"
        app.DOC_DIR = app.DATA_DIR / "doc"
        app.JSON_DIR = app.DOC_DIR / "json"
        app.IMG_DIR = app.DATA_DIR / "img"
        app.FILE_DIR = app.DATA_DIR / "file"
        app.DB_PATH = app.DATA_DIR / "wiki.db"
        app.FTS_DB_PATH = app.DATA_DIR / "wiki_fts.db"
        app.TOKEN_DB_PATH = app.DATA_DIR / "wiki_token.db"
        app.DATA_LOCK_PATH = app.DATA_DIR / "wiki.lock"
        app.SETTINGS_PATH = app.DATA_DIR / "wikisettings.cfg"
        app.init_storage()
        app.init_db()
        app.init_fts_db()
        app.init_token_db()

        db_fix.DATA_DIR = root / "dbfix"
        db_fix.DOC_DIR = db_fix.DATA_DIR / "doc"
        db_fix.JSON_DIR = db_fix.DOC_DIR / "json"
        db_fix.DB_PATH = db_fix.DATA_DIR / "wiki.db"
        db_fix.FTS_DB_PATH = db_fix.DATA_DIR / "wiki_fts.db"
        db_fix.TOKEN_DB_PATH = db_fix.DATA_DIR / "wiki_token.db"
        db_fix.DATA_LOCK_PATH = db_fix.DATA_DIR / "wiki.lock"
        db_fix.DATA_DIR.mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        for name, value in self.original_app_paths.items():
            setattr(app, name, value)
        for name, value in self.original_db_fix_paths.items():
            setattr(db_fix, name, value)
        self.temp_dir.cleanup()

    def test_main_and_derived_connections_have_explicit_durability_modes(self) -> None:
        with (
            closing(app.connect_db()) as main_conn,
            closing(app.connect_fts_db()) as fts_conn,
            closing(app.connect_token_db()) as token_conn,
        ):
            self.assertEqual(main_conn.execute("PRAGMA synchronous").fetchone()[0], 2)
            self.assertEqual(fts_conn.execute("PRAGMA synchronous").fetchone()[0], 1)
            self.assertEqual(token_conn.execute("PRAGMA synchronous").fetchone()[0], 1)
            self.assertEqual(main_conn.execute("PRAGMA journal_mode").fetchone()[0], "wal")
            self.assertEqual(fts_conn.execute("PRAGMA journal_mode").fetchone()[0], "wal")
            self.assertEqual(token_conn.execute("PRAGMA journal_mode").fetchone()[0], "wal")

        with (
            closing(db_fix.connect_db(db_fix.DATA_DIR / "main.db")) as main_conn,
            closing(
                db_fix.connect_db(
                    db_fix.DATA_DIR / "derived.db",
                    foreign_keys=False,
                    synchronous=db_fix.SQLITE_DERIVED_SYNCHRONOUS,
                )
            ) as derived_conn,
        ):
            self.assertEqual(main_conn.execute("PRAGMA synchronous").fetchone()[0], 2)
            self.assertEqual(derived_conn.execute("PRAGMA synchronous").fetchone()[0], 1)

    def test_delete_preserves_source_files_if_a_derived_commit_fails(self) -> None:
        with (
            closing(app.connect_db()) as main_conn,
            closing(app.connect_fts_db()) as fts_conn,
            closing(app.connect_token_db()) as token_conn,
        ):
            saved = app.create_document_record(
                main_conn,
                fts_conn,
                token_conn,
                title="Recoverable delete",
                content="The source document must remain recoverable.",
                tags=["recovery", "safety"],
            )
            slug = str(saved["slug"])
            row = main_conn.execute("SELECT * FROM docs WHERE slug = ?", (slug,)).fetchone()
            self.assertIsNotNone(row)

            with self.assertRaises(sqlite3.OperationalError):
                app.delete_document_record(
                    main_conn,
                    fts_conn,
                    FailingCommitConnection(token_conn),
                    row=row,
                )

            self.assertTrue(app.document_path(slug).exists())
            self.assertTrue(app.sidecar_path(slug).exists())
            self.assertIsNone(
                main_conn.execute("SELECT 1 FROM docs WHERE slug = ?", (slug,)).fetchone()
            )

        sync_result = app.sync_documents_incremental()
        self.assertEqual(sync_result["new"], 1)
        with closing(app.connect_db()) as recovered_conn:
            recovered = app.fetch_doc_with_tags(recovered_conn, slug)
        self.assertIsNotNone(recovered)
        self.assertEqual(recovered["title"], "Recoverable delete")
        self.assertEqual(recovered["tags"], ["recovery", "safety"])

    def test_dbfix_checkpoints_rebuilt_wal_before_swap(self) -> None:
        db_fix.DOC_DIR.mkdir(parents=True, exist_ok=True)
        db_fix.JSON_DIR.mkdir(parents=True, exist_ok=True)
        (db_fix.DOC_DIR / "checkpoint.md").write_text(
            "# Checkpoint\n\nWAL rebuild source document.",
            encoding="utf-8",
        )

        temp_main = db_fix.DATA_DIR / "wiki.rebuild.db"
        temp_fts = db_fix.DATA_DIR / "wiki_fts.rebuild.db"
        temp_token = db_fix.DATA_DIR / "wiki_token.rebuild.db"
        imported, skipped, _terms = db_fix.rebuild_from_doc_dir(temp_main, temp_fts, temp_token)

        self.assertEqual((imported, skipped), (1, 0))
        for path in (temp_main, temp_fts, temp_token):
            self.assertTrue(path.exists())
            self.assertFalse(Path(f"{path}-wal").exists())
            self.assertFalse(Path(f"{path}-shm").exists())

        with closing(sqlite3.connect(temp_main)) as rebuilt_main:
            self.assertEqual(rebuilt_main.execute("SELECT COUNT(*) FROM docs").fetchone()[0], 1)

    def test_dbfix_restores_every_original_database_when_swap_fails(self) -> None:
        original_contents = {
            db_fix.DB_PATH: b"old-main",
            db_fix.FTS_DB_PATH: b"old-fts",
            db_fix.TOKEN_DB_PATH: b"old-token",
        }
        for path, content in original_contents.items():
            path.write_bytes(content)

        temp_main = db_fix.DATA_DIR / "wiki.rebuild.db"
        temp_fts = db_fix.DATA_DIR / "wiki_fts.rebuild.db"
        temp_token = db_fix.DATA_DIR / "wiki_token.rebuild.db"
        for path, content in (
            (temp_main, b"new-main"),
            (temp_fts, b"new-fts"),
            (temp_token, b"new-token"),
        ):
            path.write_bytes(content)

        original_replace = Path.replace

        def fail_fts_replacement(path: Path, target: str | Path):
            if path == temp_fts and Path(target) == db_fix.FTS_DB_PATH:
                raise OSError("simulated FTS replacement failure")
            return original_replace(path, target)

        with patch.object(Path, "replace", new=fail_fts_replacement):
            with self.assertRaisesRegex(OSError, "simulated FTS replacement failure"):
                db_fix.replace_databases_from_temp(temp_main, temp_fts, temp_token)

        for path, content in original_contents.items():
            self.assertEqual(path.read_bytes(), content)

        backup_dirs = list((db_fix.DATA_DIR / "db_backups").iterdir())
        self.assertEqual(len(backup_dirs), 1)
        self.assertEqual(list(backup_dirs[0].iterdir()), [])

    def test_interrupted_swap_manifest_restores_after_a_temporary_cleanup_failure(self) -> None:
        original_contents = {
            db_fix.DB_PATH: b"old-main",
            db_fix.FTS_DB_PATH: b"old-fts",
            db_fix.TOKEN_DB_PATH: b"old-token",
        }
        for path, content in original_contents.items():
            path.write_bytes(content)

        temp_main = db_fix.DATA_DIR / "wiki.rebuild.db"
        temp_fts = db_fix.DATA_DIR / "wiki_fts.rebuild.db"
        temp_token = db_fix.DATA_DIR / "wiki_token.rebuild.db"
        for path, content in (
            (temp_main, b"new-main"),
            (temp_fts, b"new-fts"),
            (temp_token, b"new-token"),
        ):
            path.write_bytes(content)

        original_replace = Path.replace
        original_unlink = Path.unlink

        def fail_fts_replacement(path: Path, target: str | Path):
            if path == temp_fts and Path(target) == db_fix.FTS_DB_PATH:
                raise OSError("simulated FTS replacement failure")
            return original_replace(path, target)

        def fail_new_main_cleanup(path: Path, missing_ok: bool = False):
            if path == db_fix.DB_PATH:
                raise OSError("simulated new-main cleanup failure")
            return original_unlink(path, missing_ok=missing_ok)

        with (
            patch.object(Path, "replace", new=fail_fts_replacement),
            patch.object(Path, "unlink", new=fail_new_main_cleanup),
        ):
            with self.assertRaisesRegex(RuntimeError, "rollback needs recovery"):
                db_fix.replace_databases_from_temp(temp_main, temp_fts, temp_token)

        self.assertTrue(db_fix.swap_manifest_path().exists())
        self.assertEqual(db_fix.DB_PATH.read_bytes(), b"new-main")
        backup_dir = next((db_fix.DATA_DIR / "db_backups").iterdir())
        self.assertEqual((backup_dir / db_fix.DB_PATH.name).read_bytes(), b"old-main")
        self.assertEqual(db_fix.FTS_DB_PATH.read_bytes(), b"old-fts")
        self.assertEqual(db_fix.TOKEN_DB_PATH.read_bytes(), b"old-token")

        self.assertTrue(db_fix.recover_incomplete_database_swap())
        self.assertFalse(db_fix.swap_manifest_path().exists())
        for path, content in original_contents.items():
            self.assertEqual(path.read_bytes(), content)

    def test_cleanup_preserves_legacy_staged_assets(self) -> None:
        legacy_staged = app.DOC_DIR / ".recover.md.delete-123-0.tmp"
        stale_atomic_write = app.DOC_DIR / ".recover.md.random.tmp"
        legacy_staged.write_text("recoverable source", encoding="utf-8")
        stale_atomic_write.write_text("stale atomic write", encoding="utf-8")

        self.assertEqual(app.cleanup_stale_temp_files(), 1)
        self.assertTrue(legacy_staged.exists())
        self.assertFalse(stale_atomic_write.exists())

        db_fix.DOC_DIR.mkdir(parents=True, exist_ok=True)
        db_fix_staged = db_fix.DOC_DIR / ".recover.md.edit-123-0.tmp"
        db_fix_staged.write_text("recoverable source", encoding="utf-8")
        self.assertEqual(db_fix.cleanup_stale_temp_files(), 0)
        self.assertTrue(db_fix_staged.exists())

    def test_repair_fts_detects_equal_count_missing_and_orphan_rows(self) -> None:
        with (
            closing(app.connect_db()) as main_conn,
            closing(app.connect_fts_db()) as fts_conn,
            closing(app.connect_token_db()) as token_conn,
        ):
            first = app.create_document_record(
                main_conn,
                fts_conn,
                token_conn,
                title="FTS first",
                content="first searchable source",
                tags=["fts", "first"],
            )
            app.create_document_record(
                main_conn,
                fts_conn,
                token_conn,
                title="FTS second",
                content="second searchable source",
                tags=["fts", "second"],
            )
            first_row = main_conn.execute(
                "SELECT id FROM docs WHERE slug = ?",
                (first["slug"],),
            ).fetchone()
            self.assertIsNotNone(first_row)
            first_id = int(first_row["id"])
            fts_conn.execute("DELETE FROM docs_fts WHERE rowid = ?", (first_id,))
            fts_conn.execute(
                "INSERT INTO docs_fts (rowid, title, content) VALUES (?, ?, ?)",
                (999_999, "orphan", "orphan content"),
            )

            missing, orphan, rebuilt = app.repair_fts_mismatch(main_conn, fts_conn, token_conn)
            self.assertEqual((missing, orphan, rebuilt), (1, 1, 0))
            self.assertIsNotNone(
                fts_conn.execute("SELECT 1 FROM docs_fts WHERE rowid = ?", (first_id,)).fetchone()
            )
            self.assertIsNone(
                fts_conn.execute("SELECT 1 FROM docs_fts WHERE rowid = 999999").fetchone()
            )

    def test_automatic_dbfix_handoff_releases_then_reacquires_the_data_lock(self) -> None:
        events: list[str] = []
        completed = subprocess.CompletedProcess(["PersonalWikiDBFix"], 0, "fixed", "")
        with (
            patch.object(app, "resolve_db_fix_command", return_value=["PersonalWikiDBFix"]),
            patch.object(app, "release_data_lock", side_effect=lambda: events.append("release")),
            patch.object(app, "acquire_data_lock", side_effect=lambda: events.append("acquire")),
            patch.object(
                app.subprocess,
                "run",
                side_effect=lambda *_args, **_kwargs: events.append("run") or completed,
            ) as run_mock,
        ):
            self.assertTrue(app.run_db_fix_tool("test recovery"))

        self.assertEqual(events, ["release", "run", "acquire"])
        self.assertNotIn("env", run_mock.call_args.kwargs)


if __name__ == "__main__":
    unittest.main()
