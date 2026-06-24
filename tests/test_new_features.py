import io
import os
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

os.environ["PERSONALWIKI_SKIP_BOOTSTRAP"] = "1"

import app
from language_tools import KOREAN_SPELL_REPLACE_DB, apply_korean_spell_replacements


class SpellReplacementTests(unittest.TestCase):
    def test_requested_replacements(self):
        pairs = {
            "컨텐츠": "콘텐츠",
            "다던지": "다든지",
            "다던가": "다든가",
            "는것": "는 것",
            "는게": "는 게",
            "여러가지": "여러 가지",
            "을때": "을 때",
            "일때": "일 때",
            "할때": "할 때",
            "된때": "된 때",
            "던때": "던 때",
            "이 때": "이때",
            "그 때": "그때",
            "한 때": "한때",
            "을것": "을 것",
            "일것": "일 것",
            "할것": "할 것",
            "된것": "된 것",
            "던것": "던 것",
            "린것": "린 것",
            "치뤘다": "치렀다",
            "기때문": "기 때문",
            "한가지": "한 가지",
            "번번히": "번번이",
            "또 다시": "또다시",
            "맞은 편": "맞은편",
            "일려": "이려",
            "할려": "하려",
            "않는이상": "않는 이상",
            "얼만큼": "얼마만큼",
            "뇌졸증": "뇌졸중",
            "째째하": "쩨쩨하",
            "째째한": "쩨쩨한",
            "돋보적": "독보적",
            "옳바": "올바",
            "프롬포트": "프롬프트",
            "확율": "확률",
            "유렵": "유럽",
            "제 때": "제때",
            "이때문에": "이 때문에",
            "그 것": "그것",
            "그 날": "그날",
            "그때문에": "그 때문에",
            "저 것": "저것",
            "갯수": "개수",
            "떄": "때",
            "스폐셜": "스페셜",
            "댓가": "대가",
        }
        for wrong, expected in pairs.items():
            with self.subTest(wrong=wrong):
                self.assertEqual(apply_korean_spell_replacements(wrong), expected)

    def test_database_is_sorted_longest_first(self):
        lengths = [len(wrong) for wrong, _replacement in KOREAN_SPELL_REPLACE_DB]
        self.assertEqual(lengths, sorted(lengths, reverse=True))

    def test_correct_because_phrases_are_not_changed(self):
        text = "이 때문에 늦었고 그 때문에 다시 확인했다."
        self.assertEqual(apply_korean_spell_replacements(text), text)


class SettingsTests(unittest.TestCase):
    def test_port_setting_and_fallbacks(self):
        cases = {
            "port=7000": 7000,
            "": 6885,
            "port=nope": 6885,
            "port=0": 6885,
            "port=65536": 6885,
            "other=1\nport = 8123": 8123,
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "wikisettings.cfg"
            self.assertEqual(app.read_server_port(path), 6885)
            for raw, expected in cases.items():
                path.write_text(raw, encoding="utf-8")
                with self.subTest(raw=raw):
                    self.assertEqual(app.read_server_port(path), expected)


class ExportTests(unittest.TestCase):
    DOC = {
        "title": "내보내기 테스트",
        "slug": "export-test",
        "created_at": "2026-01-01T00:00:00",
        "updated_at": "2026-01-02T00:00:00",
        "tags": ["테스트"],
    }

    def test_address_parser(self):
        cases = {
            "127.0.0.1:6885/doc/home": "home",
            "http://localhost:6885/doc/%ED%95%9C%EA%B8%80": "한글",
            "/doc/home": "home",
        }
        for raw, expected in cases.items():
            self.assertEqual(app.parse_export_doc_address(raw), expected)
        with self.assertRaises(app.ExportError):
            app.parse_export_doc_address("https://example.com/doc/home")
        with self.assertRaises(app.ExportError):
            app.parse_export_doc_address("http://localhost:6885/search")

    def test_zip_and_single_html_assets(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            image_dir = root / "img"
            file_dir = root / "file"
            image_dir.mkdir()
            file_dir.mkdir()
            (image_dir / "sample.png").write_bytes(b"fake-png")
            (file_dir / "notes.txt").write_text("attachment", encoding="utf-8")
            rendered = '<p><img src="/img/sample.png"><a href="/file/notes.txt">file</a></p>'

            with patch.object(app, "IMG_DIR", image_dir), patch.object(app, "FILE_DIR", file_dir):
                zip_payload = app.build_zip_export(self.DOC, rendered)
                html_payload = app.build_single_html_export(self.DOC, rendered).decode("utf-8")

            with zipfile.ZipFile(io.BytesIO(zip_payload)) as archive:
                self.assertIn("index.html", archive.namelist())
                self.assertIn("assets/img/sample.png", archive.namelist())
                self.assertIn("assets/file/notes.txt", archive.namelist())
                exported_html = archive.read("index.html").decode("utf-8")
                self.assertIn("assets/img/sample.png", exported_html)
                self.assertIn("assets/file/notes.txt", exported_html)

            self.assertIn("data:image/png;base64,", html_payload)
            self.assertIn('href="/file/notes.txt"', html_payload)
            self.assertIn("<style>", html_payload)


class ToolPageTests(unittest.TestCase):
    def test_tool_pages_render(self):
        client = app.app.test_client()
        table_response = client.get("/tool/table")
        package_response = client.get("/tool/package")
        self.assertEqual(table_response.status_code, 200)
        self.assertIn("마크다운 표 편집기".encode(), table_response.data)
        self.assertEqual(package_response.status_code, 200)
        self.assertIn("문서 내보내기".encode(), package_response.data)

    def test_html_file_reference_requires_confirmation(self):
        client = app.app.test_client()
        doc = {
            "title": "첨부 테스트",
            "slug": "attachment-test",
            "created_at": "2026-01-01T00:00:00",
            "updated_at": "2026-01-01T00:00:00",
            "tags": [],
        }
        with patch.object(app, "_load_export_document", return_value=(doc, "[[file/report.pdf]]")):
            check_response = client.post(
                "/api/package/check",
                json={"document_address": "http://127.0.0.1:6885/doc/attachment-test"},
            )
            export_response = client.post(
                "/tool/package/export",
                data={
                    "document_address": "http://127.0.0.1:6885/doc/attachment-test",
                    "export_format": "html",
                    "confirmed_files": "0",
                },
            )
        self.assertEqual(check_response.status_code, 200)
        self.assertTrue(check_response.get_json()["has_files"])
        self.assertEqual(export_response.status_code, 409)
        self.assertIn("첨부 파일 링크".encode(), export_response.data)


if __name__ == "__main__":
    unittest.main()
