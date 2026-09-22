from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

os.environ["PERSONALWIKI_SKIP_BOOTSTRAP"] = "1"

import app


class ClientApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.original_paths = {
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
        self.original_testing = app.app.testing

        app.DATA_DIR = root
        app.DOC_DIR = root / "doc"
        app.JSON_DIR = app.DOC_DIR / "json"
        app.IMG_DIR = root / "img"
        app.FILE_DIR = root / "file"
        app.DB_PATH = root / "wiki.db"
        app.FTS_DB_PATH = root / "wiki_fts.db"
        app.TOKEN_DB_PATH = root / "wiki_token.db"
        app.DATA_LOCK_PATH = root / "wiki.lock"
        app.SETTINGS_PATH = root / "wikisettings.cfg"
        app.app.testing = True
        app.init_storage()
        app.init_db()
        app.init_fts_db()
        app.init_token_db()

        self.ast_patch = patch.object(
            app.markdown_engine,
            "render_ast",
            lambda text, **_callbacks: [{"type": "paragraph", "text": text, "children": [], "attrs": {}}],
            create=True,
        )
        self.ast_patch.start()
        self.client = app.app.test_client()

    def tearDown(self) -> None:
        self.ast_patch.stop()
        for name, value in self.original_paths.items():
            setattr(app, name, value)
        app.app.testing = self.original_testing
        self.temp_dir.cleanup()

    @staticmethod
    def document_payload(
        title: str,
        content: str = "PersonalWiki client body",
        tags: list[str] | None = None,
        **extra: object,
    ) -> dict[str, object]:
        return {
            "title": title,
            "content": content,
            "tags": tags if tags is not None else ["client", "api"],
            **extra,
        }

    def create_document(self, title: str, **kwargs: object) -> dict:
        response = self.client.post("/api/client/documents", json=self.document_payload(title, **kwargs))
        self.assertEqual(response.status_code, 201, response.get_data(as_text=True))
        return response.get_json()

    def test_api_is_loopback_only_and_bootstrap_advertises_ast(self) -> None:
        denied = self.client.get(
            "/api/client/bootstrap",
            environ_overrides={"REMOTE_ADDR": "203.0.113.8"},
        )
        self.assertEqual(denied.status_code, 403)
        self.assertEqual(denied.get_json()["error_code"], "localhost_only")

        # A tunnel proxy can itself connect from 127.0.0.1.  Its forwarding
        # headers must not turn the native-only API into a public endpoint.
        proxied = self.client.get(
            "/api/client/bootstrap",
            headers={"X-Forwarded-For": "203.0.113.8"},
        )
        self.assertEqual(proxied.status_code, 403)
        self.assertEqual(proxied.get_json()["error_code"], "localhost_only")

        allowed = self.client.get("/api/client/bootstrap")
        self.assertEqual(allowed.status_code, 200)
        payload = allowed.get_json()
        self.assertEqual(payload["api_version"], app.CLIENT_API_VERSION)
        self.assertTrue(payload["ast"]["available"])
        self.assertTrue(payload["capabilities"]["native_render"])

    def test_create_get_list_tag_search_update_and_delete_share_storage_workflow(self) -> None:
        created = self.create_document("Client document")
        document = created["document"]
        slug = document["slug"]
        self.assertEqual(document["content"], "PersonalWiki client body")
        self.assertTrue(app.document_path(slug).exists())
        self.assertTrue(app.sidecar_path(slug).exists())

        listed = self.client.get("/api/client/documents")
        self.assertEqual(listed.status_code, 200)
        self.assertEqual([item["slug"] for item in listed.get_json()["documents"]], [slug])
        self.assertIsNone(listed.get_json()["pagination"]["limit"])
        self.assertIsNone(listed.get_json()["pagination"]["next_offset"])

        tags = self.client.get("/api/client/tags")
        self.assertEqual(tags.status_code, 200)
        self.assertEqual({item["name"] for item in tags.get_json()["tags"]}, {"api", "client"})

        tag_documents = self.client.get("/api/client/tags/client")
        self.assertEqual(tag_documents.status_code, 200)
        self.assertEqual(tag_documents.get_json()["documents"][0]["slug"], slug)
        tag_alias = self.client.get("/api/client/tags/client/documents")
        self.assertEqual(tag_alias.status_code, 200)
        self.assertEqual(tag_alias.get_json()["documents"][0]["slug"], slug)

        searched = self.client.get("/api/client/search?q=PersonalWiki")
        self.assertEqual(searched.status_code, 200)
        self.assertEqual(searched.get_json()["documents"][0]["slug"], slug)

        source = self.create_document(
            "Backlink source",
            content="See [[Client document]] from the local client.",
        )
        paged = self.client.get("/api/client/documents?limit=1&offset=0")
        self.assertEqual(paged.status_code, 200)
        self.assertEqual(len(paged.get_json()["documents"]), 1)
        self.assertEqual(paged.get_json()["pagination"]["total"], 2)
        self.assertEqual(paged.get_json()["pagination"]["next_offset"], 1)
        invalid_pagination = self.client.get("/api/client/documents?limit=all")
        self.assertEqual(invalid_pagination.status_code, 400)
        self.assertEqual(invalid_pagination.get_json()["error_code"], "invalid_pagination")
        fetched = self.client.get(f"/api/client/documents/{slug}")
        self.assertEqual(fetched.status_code, 200)
        payload = fetched.get_json()
        self.assertEqual(payload["document"]["content"], "PersonalWiki client body")
        self.assertEqual(payload["ast"][0]["type"], "paragraph")
        self.assertEqual(payload["backlinks"][0]["slug"], source["document"]["slug"])

        updated = self.client.put(
            f"/api/client/documents/{slug}",
            json=self.document_payload(
                "Client document revised",
                content="Updated server-side content",
                tags=["client", "updated"],
            ),
        )
        self.assertEqual(updated.status_code, 200, updated.get_data(as_text=True))
        revised_slug = updated.get_json()["document"]["slug"]
        self.assertNotEqual(revised_slug, slug)
        self.assertFalse(app.document_path(slug).exists())
        self.assertTrue(app.document_path(revised_slug).exists())
        self.assertEqual(self.client.get(f"/api/client/documents/{slug}").status_code, 404)

        deleted = self.client.delete(f"/api/client/documents/{revised_slug}")
        self.assertEqual(deleted.status_code, 200)
        self.assertTrue(deleted.get_json()["deleted"])
        self.assertFalse(app.document_path(revised_slug).exists())
        self.assertEqual(self.client.get(f"/api/client/documents/{revised_slug}").status_code, 404)

    def test_confirmation_json_matches_native_client_fields(self) -> None:
        tag_warning = self.client.post(
            "/api/client/documents",
            json=self.document_payload("One tag", tags=["only"]),
        )
        self.assertEqual(tag_warning.status_code, 409)
        tag_payload = tag_warning.get_json()
        self.assertIsInstance(tag_payload["error"], str)
        self.assertTrue(tag_payload["needs_tag_warning_decision"])
        self.assertIn("suggested_tags", tag_payload)

        confirmed = self.client.post(
            "/api/client/documents",
            json=self.document_payload("One tag", tags=["only"], ignore_tag_warning=True),
        )
        self.assertEqual(confirmed.status_code, 201, confirmed.get_data(as_text=True))

        with patch.object(
            app,
            "collect_korean_spell_issues",
            return_value={"count": 1, "samples": [{"source": "테스트", "replacement": "시험"}]},
        ):
            spell_warning = self.client.post(
                "/api/client/documents",
                json=self.document_payload("Spell decision", tags=["spell", "api"]),
            )
            self.assertEqual(spell_warning.status_code, 409)
            spell_payload = spell_warning.get_json()
            self.assertIsInstance(spell_payload["error"], str)
            self.assertTrue(spell_payload["needs_spellcheck_decision"])
            self.assertEqual(spell_payload["spellcheck_samples"][0]["replacement"], "시험")

            saved_as_is = self.client.post(
                "/api/client/documents",
                json=self.document_payload(
                    "Spell decision",
                    tags=["spell", "api"],
                    save_as_is=True,
                ),
            )
            self.assertEqual(saved_as_is.status_code, 201, saved_as_is.get_data(as_text=True))

    def test_browser_forms_use_the_same_validation_and_persistence_helpers(self) -> None:
        created = self.client.post(
            "/new",
            data={
                "title": "Browser shared flow",
                "content": "Created in the browser form",
                "tags": "browser, shared",
            },
        )
        self.assertEqual(created.status_code, 302)
        original_slug = created.headers["Location"].rsplit("/", 1)[-1]
        self.assertTrue(app.document_path(original_slug).exists())
        self.assertTrue(app.sidecar_path(original_slug).exists())

        duplicate = self.client.post(
            "/new",
            data={"title": "Browser shared flow", "content": "duplicate", "tags": "browser, shared"},
        )
        self.assertEqual(duplicate.status_code, 200)
        self.assertIn("같은 제목의 문서가 이미 있습니다.", duplicate.get_data(as_text=True))

        tag_warning = self.client.post(
            "/new",
            data={"title": "Browser tag confirmation", "content": "body", "tags": "only"},
        )
        self.assertEqual(tag_warning.status_code, 200)
        self.assertIn("태그를 2개 이상 등록하면", tag_warning.get_data(as_text=True))

        edited = self.client.post(
            f"/edit/{original_slug}",
            data={
                "title": "Browser shared flow revised",
                "content": "Updated by the browser form",
                "tags": "browser, updated",
            },
        )
        self.assertEqual(edited.status_code, 302)
        revised_slug = edited.headers["Location"].rsplit("/", 1)[-1]
        self.assertFalse(app.document_path(original_slug).exists())
        self.assertTrue(app.document_path(revised_slug).exists())
        self.assertEqual(
            self.client.get(f"/api/client/search?q=Updated").get_json()["documents"][0]["slug"],
            revised_slug,
        )

        deleted = self.client.post(f"/delete/{revised_slug}")
        self.assertEqual(deleted.status_code, 302)
        self.assertFalse(app.document_path(revised_slug).exists())

    def test_render_endpoint_reports_unavailable_ast_explicitly(self) -> None:
        with patch.object(app.markdown_engine, "render_ast", None):
            bootstrap = self.client.get("/api/client/bootstrap")
            self.assertFalse(bootstrap.get_json()["ast"]["available"])
            rendered = self.client.post("/api/client/render", json={"content": "# heading"})
            self.assertEqual(rendered.status_code, 503)
            self.assertEqual(rendered.get_json()["error_code"], "ast_unavailable")

    def test_render_endpoint_uses_the_real_native_ast_with_wiki_context(self) -> None:
        self.ast_patch.stop()
        try:
            self.create_document("Linked document")
            rendered = self.client.post(
                "/api/client/render",
                json={"content": "# Heading\n\n[[Linked document]]\n\n!!! note context"},
            )
        finally:
            self.ast_patch.start()

        self.assertEqual(rendered.status_code, 200, rendered.get_data(as_text=True))
        ast = [node for node in rendered.get_json()["ast"] if node["type"] != "blank_line"]
        self.assertEqual(ast[0]["type"], "heading")
        self.assertEqual(ast[1]["type"], "paragraph")
        self.assertEqual(ast[1]["children"][0]["type"], "link")
        self.assertEqual(ast[1]["children"][0]["attrs"]["url"], "/doc/linked-document")
        self.assertEqual(ast[2]["type"], "callout")

    def test_tag_results_batch_their_tag_lookups(self) -> None:
        expected_titles = {"Batched tag 1", "Batched tag 2", "Batched tag 3"}
        for index in range(1, 4):
            self.create_document(
                f"Batched tag {index}",
                content=f"ordinary body {index}",
                tags=["sharedtag", f"uniquetag{index}"],
            )

        # Both routes used to call list_doc_tags once per result.  The batch
        # map keeps a large shared tag result to a fixed number of queries.
        with patch.object(
            app,
            "list_doc_tags",
            side_effect=AssertionError("per-document tag lookup"),
        ):
            tag_response = self.client.get("/api/client/tags/sharedtag")
            search_response = self.client.get("/api/client/search?q=sharedtag")

        self.assertEqual(tag_response.status_code, 200)
        tag_documents = tag_response.get_json()["documents"]
        self.assertEqual({item["title"] for item in tag_documents}, expected_titles)
        self.assertTrue(all("sharedtag" in item["tags"] for item in tag_documents))

        self.assertEqual(search_response.status_code, 200)
        search_documents = search_response.get_json()["documents"]
        self.assertEqual({item["title"] for item in search_documents}, expected_titles)
        self.assertTrue(all("sharedtag" in item["tags"] for item in search_documents))
        self.assertTrue(all(item["created_at"] for item in search_documents))
        self.assertTrue(all(item["updated_at"] for item in search_documents))

    def test_tag_suggestion_input_limits_apply_to_browser_and_native_routes(self) -> None:
        payload = {
            "title": "Bounded suggestions",
            "content": "ordinary content",
            "tags": ["client", "api"],
        }
        accepted = self.client.post("/api/client/tag-suggestions", json=payload)
        self.assertEqual(accepted.status_code, 200, accepted.get_data(as_text=True))

        with patch.object(app, "TAG_SUGGESTION_MAX_CONTENT_CHARS", 5):
            for endpoint in ("/api/tag-suggestions", "/api/client/tag-suggestions"):
                with self.subTest(endpoint=endpoint):
                    rejected = self.client.post(
                        endpoint,
                        json={**payload, "content": "123456"},
                    )
                    self.assertEqual(rejected.status_code, 413)
                    self.assertEqual(rejected.get_json()["error_code"], "tag_suggestion_too_large")

        with patch.object(app, "TAG_SUGGESTION_MAX_TAGS", 2):
            rejected_tags = self.client.post(
                "/api/client/tag-suggestions",
                json={**payload, "tags": ["one", "two", "three"]},
            )
            self.assertEqual(rejected_tags.status_code, 413)
            self.assertEqual(rejected_tags.get_json()["error_code"], "tag_suggestion_too_large")

        # This guard runs before request.get_json(), so an oversized body is
        # rejected before Flask builds a payload object for the recommender.
        with patch.object(app, "TAG_SUGGESTION_MAX_REQUEST_BYTES", 1):
            rejected_body = self.client.post("/api/client/tag-suggestions", json=payload)
            self.assertEqual(rejected_body.status_code, 413)
            self.assertEqual(rejected_body.get_json()["error_code"], "tag_suggestion_payload_too_large")

    def test_invalid_json_payload_is_a_json_error(self) -> None:
        response = self.client.post("/api/client/documents", data="[]", content_type="application/json")
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.get_json()["error_code"], "invalid_json")


if __name__ == "__main__":
    unittest.main()
