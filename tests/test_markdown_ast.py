from __future__ import annotations

import json
import unittest

from markdown_engine import MarkdownEngine, PERSONAL_WIKI_AST_VERSION


def walk_nodes(nodes: list[dict[str, object]]):
    for node in nodes:
        yield node
        children = node["children"]
        if isinstance(children, list):
            yield from walk_nodes([child for child in children if isinstance(child, dict)])


def nodes_of_type(nodes: list[dict[str, object]], node_type: str) -> list[dict[str, object]]:
    return [node for node in walk_nodes(nodes) if node["type"] == node_type]


class MarkdownAstTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = MarkdownEngine()

    @staticmethod
    def resolve_doc_reference(ref: str) -> str | None:
        return {"Known": "known", "Template": "template"}.get(ref)

    @staticmethod
    def read_document(slug: str) -> str | None:
        if slug == "template":
            return "## Template heading\n\nTemplate **body**"
        return None

    @staticmethod
    def list_tag_documents(tag_name: str) -> list[dict[str, object]]:
        if tag_name == "important":
            return [
                {"title": "Known", "slug": "known"},
                {"title": "Another", "slug": "another"},
            ]
        return []

    def render_ast(self, text: str) -> list[dict[str, object]]:
        return self.engine.render_ast(
            text,
            resolve_doc_reference=self.resolve_doc_reference,
            read_document=self.read_document,
            list_tag_documents=self.list_tag_documents,
        )

    def test_render_ast_covers_markdown_and_personal_wiki_nodes(self) -> None:
        source = """{{TOC3}}

# Main *heading*

Text **strong** ~~strike~~ `code` ==highlight== ||spoiler|| [web](https://example.test).
[[Known|Wiki]] and [[file/report.pdf|Download]].

![Plain alt](plain.png)
![[diagram.png|Diagram, width=320, height=180]]
![[youtube(abc123def, width=320, start=1:02)]]
![[tag(important)]]

!!! note <unsafe>& text
!!! note second line

- [x] complete
- [ ] open

> Quote

```python
print('code')
```

| A | B |
| :--- | ---: |
| one | two |
| ----- | --- |
| wide | cell |

Footnote[^one].

[^one]: Footnote text

{{Template}}

||{{Template}}||

Inline {{Template}} template.
"""
        tree = self.render_ast(source)

        self.assertEqual(PERSONAL_WIKI_AST_VERSION, 1)
        self.assertEqual(json.loads(json.dumps(tree, ensure_ascii=False)), tree)
        for node in walk_nodes(tree):
            self.assertEqual(set(node), {"type", "text", "children", "attrs"})

        heading = nodes_of_type(tree, "heading")[0]
        self.assertEqual(heading["attrs"]["level"], 1)
        self.assertEqual(heading["attrs"]["anchor"], "main-heading")
        toc = nodes_of_type(tree, "toc")[0]
        self.assertEqual(toc["attrs"]["max_level"], 3)
        self.assertIn(
            {"level": 1, "title": "Main heading", "anchor": "main-heading"},
            toc["attrs"]["headings"],
        )
        self.assertIn("template-heading", {item["anchor"] for item in toc["attrs"]["headings"]})

        paragraph_types = {
            child["type"]
            for node in nodes_of_type(tree, "paragraph")
            for child in node["children"]
            if isinstance(child, dict)
        }
        self.assertTrue({"strong", "strikethrough", "codespan", "highlight", "spoiler"} <= paragraph_types)

        links = nodes_of_type(tree, "link")
        self.assertIn(
            {"url": "/doc/known", "kind": "wiki", "target": "Known"},
            [node["attrs"] for node in links],
        )
        self.assertIn(
            {"url": "/file/report.pdf", "kind": "file", "target": "file/report.pdf"},
            [node["attrs"] for node in links],
        )

        images = nodes_of_type(tree, "image")
        self.assertIn(
            {"url": "/img/diagram.png", "alt": "Diagram", "width": 320, "height": 180},
            [node["attrs"] for node in images],
        )
        self.assertIn("Plain alt", [node["attrs"]["alt"] for node in images])

        youtube = nodes_of_type(tree, "youtube")[0]
        self.assertEqual(
            youtube["attrs"],
            {
                "video_id": "abc123def",
                "url": "https://www.youtube.com/embed/abc123def?start=62",
                "width": 320,
                "height": 180,
                "start": 62,
            },
        )
        tag_embed = nodes_of_type(tree, "tag_embed")[0]
        self.assertEqual(tag_embed["attrs"]["tag"], "important")
        self.assertEqual([node["text"] for node in tag_embed["children"]], ["Known", "Another"])

        callout = nodes_of_type(tree, "callout")[0]
        self.assertEqual(callout["text"], "<unsafe>& text\nsecond line")
        self.assertEqual(callout["attrs"]["level"], "note")

        tasks = nodes_of_type(tree, "task_list_item")
        self.assertEqual([task["attrs"]["checked"] for task in tasks], [True, False])
        table_cells = nodes_of_type(tree, "table_cell")
        self.assertIn("left", [cell["attrs"]["align"] for cell in table_cells])
        self.assertIn("right", [cell["attrs"]["align"] for cell in table_cells])
        self.assertTrue(any("colspan" in cell["attrs"] for cell in table_cells))
        self.assertEqual(len(nodes_of_type(tree, "footnote_ref")), 1)
        self.assertEqual(len(nodes_of_type(tree, "footnotes")), 1)

        templates = nodes_of_type(tree, "template")
        self.assertIn(
            {"ref": "Template", "inline": False, "folded": False},
            [template["attrs"] for template in templates],
        )
        self.assertIn(
            {"ref": "Template", "inline": False, "folded": True},
            [template["attrs"] for template in templates],
        )
        self.assertIn(
            {"ref": "Template", "inline": True, "folded": False},
            [template["attrs"] for template in templates],
        )

    def test_ast_treats_raw_html_as_literal_and_sanitizes_harmful_urls(self) -> None:
        tree = self.render_ast(
            "[js](javascript:alert%281%29) [settings](ms-settings:display) "
            "[shell](shell:AppsFolder) [hidden](java%0ascript:alert%281%29) "
            "[good](https://example.test/path) <script>alert(1)</script>"
        )
        links = nodes_of_type(tree, "link")
        self.assertEqual(
            [link["attrs"]["url"] for link in links[:-1]],
            ["#harmful-link"] * 4,
        )
        self.assertEqual(links[-1]["attrs"]["url"], "https://example.test/path")
        html_nodes = nodes_of_type(tree, "html")
        self.assertEqual(len(html_nodes), 2)
        self.assertTrue(all(node["attrs"]["trusted"] is False for node in html_nodes))
        self.assertTrue(all(node["attrs"]["render_mode"] == "literal" for node in html_nodes))

    def test_ast_keeps_missing_template_and_template_html_as_safe_source_text(self) -> None:
        tree = self.engine.render_ast(
            "{{Missing <&>}}\n\n{{Raw}}",
            resolve_doc_reference=lambda ref: "raw" if ref == "Raw" else None,
            read_document=lambda slug: "<script>alert(1)</script>" if slug == "raw" else None,
            list_tag_documents=self.list_tag_documents,
        )

        templates = nodes_of_type(tree, "template")
        self.assertEqual(templates[0]["attrs"]["ref"], "Missing <&>")
        self.assertIn(
            "[Missing template: Missing <&>]",
            [node["text"] for node in nodes_of_type(templates[0]["children"], "text")],
        )
        template_html = nodes_of_type(templates[1]["children"], "html")[0]
        self.assertEqual(template_html["text"], "<script>alert(1)</script>\n")
        self.assertEqual(
            template_html["attrs"],
            {"inline": False, "trusted": False, "render_mode": "literal"},
        )

    def test_ast_maps_documented_kbd_and_br_to_safe_native_nodes(self) -> None:
        tree = self.render_ast(
            "<kbd>Ctrl</kbd> + <kbd>S</kbd>\n\n"
            "| A | B |\n| --- | --- |\n| value | first<br>second |"
        )
        keys = nodes_of_type(tree, "kbd")
        self.assertEqual([node["attrs"] for node in keys], [{"source": "html_kbd"}] * 2)
        self.assertEqual(
            ["".join(child["text"] for child in node["children"]) for node in keys],
            ["Ctrl", "S"],
        )
        linebreaks = nodes_of_type(tree, "linebreak")
        self.assertEqual(len(linebreaks), 1)
        self.assertEqual(linebreaks[0]["attrs"], {"source": "html_br"})

    def test_existing_html_renderer_keeps_personal_wiki_features(self) -> None:
        rendered = self.engine.render(
            """# Title

!!! note <unsafe>& text

[[Known|Wiki]] [[file/report.pdf|Download]] ==mark== ||hide||

![[youtube(abc123def, width=320, start=1:02)]]
![[tag(important)]]
{{Template}}
||{{Template}}||
{{TOC3}}
""",
            resolve_doc_reference=self.resolve_doc_reference,
            read_document=self.read_document,
            list_tag_documents=self.list_tag_documents,
        )
        self.assertIn('&lt;unsafe&gt;&amp; text', rendered)
        self.assertIn('<a href="/doc/known">Wiki</a>', rendered)
        self.assertIn('<a href="/file/report.pdf">Download</a>', rendered)
        self.assertIn("<mark>mark</mark>", rendered)
        self.assertIn('class="spoiler"', rendered)
        self.assertIn('src="https://www.youtube.com/embed/abc123def?start=62"', rendered)
        self.assertIn('class="tag-document-list"', rendered)
        self.assertIn('class="template-fold"', rendered)
        self.assertIn('class="wiki-toc"', rendered)


if __name__ == "__main__":
    unittest.main()
