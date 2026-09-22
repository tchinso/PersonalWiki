from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

os.environ["PERSONALWIKI_SKIP_BOOTSTRAP"] = "1"

import app


class WikiSettingsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.settings_path = Path(self.temp_dir.name) / "wikisettings.cfg"
        self.original_settings_path = app.SETTINGS_PATH
        self.original_testing = app.app.testing
        app.SETTINGS_PATH = self.settings_path
        app.app.testing = True

    def tearDown(self) -> None:
        app.SETTINGS_PATH = self.original_settings_path
        app.app.testing = self.original_testing
        self.temp_dir.cleanup()

    def test_reads_valid_values_and_skips_malformed_duplicates(self) -> None:
        self.settings_path.write_text(
            """# first malformed entries must not mask later valid values
port=70000
port=7123
browser_font=custom:bad;css
browser_font=nanum-gothic
""",
            encoding="utf-8-sig",
        )

        settings = app.read_wiki_settings()

        self.assertEqual(settings.port, 7123)
        self.assertEqual(settings.browser_font, "nanum-gothic")
        self.assertEqual(app.read_server_port(), 7123)

    def test_malformed_values_fall_back_to_defaults(self) -> None:
        self.settings_path.write_text(
            "port=0\nbrowser_font=custom:font; color: red\n",
            encoding="utf-8-sig",
        )

        self.assertEqual(app.read_wiki_settings(), app.WikiSettings())

    def test_write_preserves_comments_and_unknown_keys(self) -> None:
        self.settings_path.write_text(
            """# keep this comment
extra_setting = leave-me-alone
Port = 6885
browser_font=system
port=6999
; retain this comment too
""",
            encoding="utf-8-sig",
        )

        app.write_wiki_settings(
            app.WikiSettings(port=7123, browser_font="custom:맑은 고딕")
        )

        saved_bytes = self.settings_path.read_bytes()
        self.assertTrue(saved_bytes.startswith(b"\xef\xbb\xbf"))
        saved = saved_bytes.decode("utf-8-sig")
        self.assertIn("# keep this comment", saved)
        self.assertIn("extra_setting = leave-me-alone", saved)
        self.assertIn("; retain this comment too", saved)
        self.assertEqual(saved.count("port="), 1)
        self.assertEqual(saved.count("browser_font="), 1)
        self.assertIn("port=7123", saved)
        self.assertIn("browser_font=custom:맑은 고딕", saved)

    def test_custom_font_validation_never_accepts_css_syntax(self) -> None:
        self.assertEqual(
            app.normalize_browser_font_setting("custom:맑은 고딕"),
            "custom:맑은 고딕",
        )
        self.assertIsNone(app.normalize_browser_font_setting("custom:font; color: red"))
        self.assertIsNone(app.normalize_browser_font_setting('custom:font"}</style>'))
        self.assertEqual(
            app.browser_font_css("custom:맑은 고딕"),
            '"맑은 고딕", Inter, "Noto Sans KR", "Segoe UI", Roboto, sans-serif',
        )

    def test_invalid_configured_font_is_not_rendered_into_the_style_tag(self) -> None:
        self.settings_path.write_text(
            "browser_font=custom:font</style><script>bad()</script>\n",
            encoding="utf-8",
        )

        page = app.app.test_client().get("/settings")
        body = page.get_data(as_text=True)

        self.assertEqual(page.status_code, 200)
        self.assertIn('--app-font: Inter, "Noto Sans KR"', body)
        self.assertNotIn("bad()</script>", body)

    def test_settings_page_persists_browser_font_and_marks_port_restart(self) -> None:
        client = app.app.test_client()

        response = client.post(
            "/settings",
            data={
                "port": "7123",
                "browser_font": "custom",
                "browser_font_custom": "맑은 고딕",
            },
        )

        self.assertEqual(response.status_code, 302)
        self.assertIn("restart_required=1", response.headers["Location"])
        saved = app.read_wiki_settings()
        self.assertEqual(saved, app.WikiSettings(port=7123, browser_font="custom:맑은 고딕"))

        page = client.get(response.headers["Location"])
        body = page.get_data(as_text=True)
        self.assertEqual(page.status_code, 200)
        self.assertIn("포트 변경은 PersonalWiki를 종료한 뒤 다시 시작해야 적용됩니다.", body)
        self.assertIn('--app-font: "맑은 고딕", Inter, "Noto Sans KR"', body)

    def test_font_only_update_does_not_claim_a_server_restart(self) -> None:
        app.write_wiki_settings(app.WikiSettings(port=7123, browser_font="system"))
        client = app.app.test_client()

        response = client.post(
            "/settings",
            data={
                "port": "7123",
                "browser_font": "pretendard",
                "browser_font_custom": "",
            },
        )

        self.assertEqual(response.status_code, 302)
        self.assertIn("restart_required=0", response.headers["Location"])
        page = client.get(response.headers["Location"])
        self.assertIn(
            "브라우저 글꼴은 현재 페이지부터 적용됩니다.",
            page.get_data(as_text=True),
        )

    def test_settings_page_rejects_unsafe_custom_font(self) -> None:
        client = app.app.test_client()

        response = client.post(
            "/settings",
            data={
                "port": "7123",
                "browser_font": "custom",
                "browser_font_custom": "font; color: red",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertIn("글꼴은 목록에서 선택하거나 안전한 설치 글꼴 이름을 입력해 주세요.", response.get_data(as_text=True))
        self.assertFalse(self.settings_path.exists())


if __name__ == "__main__":
    unittest.main()
