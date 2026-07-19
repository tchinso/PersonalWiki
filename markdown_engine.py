from __future__ import annotations

import html
import re
from fractions import Fraction
from pathlib import Path
from typing import Callable, Match, MutableMapping
from urllib.parse import quote

import mistune
from mistune import BlockState, InlineState, Markdown
from mistune.plugins import table as mistune_table
from mistune.util import escape_url

WIKI_LINK_RE = re.compile(r"(?<!\!)\[\[([^\[\]]+)\]\]")
IMAGE_SHORTCUT_RE = re.compile(r"!\[\[([^\[\]]+)\]\]")
TAG_EMBED_RE = re.compile(r"^tag\((?P<tag>.*)\)$", flags=re.IGNORECASE)
TEMPLATE_RE = re.compile(r"\{\{([^{}]+)\}\}")
FOLDED_TEMPLATE_RE = re.compile(r"\|\|\s*\{\{([^{}]+)\}\}\s*\|\|?")
YOUTUBE_RE = re.compile(r"^youtube\((.*)\)$", flags=re.IGNORECASE)
YOUTUBE_ID_RE = re.compile(r"^[A-Za-z0-9_-]{6,20}$")
EXTERNAL_WIKI_TARGET_RE = re.compile(r"^(https?|file)://", flags=re.IGNORECASE)
FILE_WIKI_TARGET_RE = re.compile(r"^file/", flags=re.IGNORECASE)
TABLE_SEPARATOR_CELL_RE = re.compile(r"^:?(?P<dashes>-{3,}):?$")
CALLOUT_LINE_RE = re.compile(
    r"^[ \t]*!!![ \t]*(?P<callout_level>note|info|warn|danger)[ \t]+(?P<callout_body>[^\n]*)$",
    flags=re.IGNORECASE,
)
TOC_REF_RE = re.compile(r"^TOC(?P<level>[1-6])?$", flags=re.IGNORECASE)
HTML_TAG_RE = re.compile(r"<[^>]+>")

WIKI_CONTEXT_KEY = "personal_wiki_context"
TEMPLATE_DISABLED_KEY = "personal_wiki_template_disabled"
TOC_RESERVED_TITLES = ("TOC", "TOC1", "TOC2", "TOC3", "TOC4", "TOC5", "TOC6")
TOC_DEFAULT_MAX_LEVEL = 2
CALLOUT_ICONS = {
    "info": ("i", "정보"),
    "note": ("💡", "노트"),
    "warn": ("!", "경고"),
    "danger": ("!", "위험"),
}


def toc_max_level_from_ref(ref: str) -> int | None:
    match = TOC_REF_RE.fullmatch(ref.strip())
    if not match:
        return None
    level = match.group("level")
    return int(level) if level else TOC_DEFAULT_MAX_LEVEL


def _is_external_wiki_target(target: str) -> bool:
    return EXTERNAL_WIKI_TARGET_RE.match(target.strip()) is not None


def _is_file_wiki_target(target: str) -> bool:
    return FILE_WIKI_TARGET_RE.match(target.strip()) is not None


def _parse_dimension_option(value: str) -> int | None:
    cleaned = value.strip().rstrip(")")
    if not cleaned.isdigit():
        return None
    parsed = int(cleaned)
    if parsed < 120:
        return 120
    if parsed > 3840:
        return 3840
    return parsed


def _parse_timestamp(value: str) -> int | None:
    cleaned = value.strip().lower()
    if not cleaned:
        return None
    if cleaned.isdigit():
        return int(cleaned)

    parts = cleaned.split(":")
    if 2 <= len(parts) <= 3 and all(part.isdigit() for part in parts):
        seconds = 0
        for part in parts:
            seconds = seconds * 60 + int(part)
        return seconds
    return None


def _heading_text_from_html(rendered: str) -> str:
    text = HTML_TAG_RE.sub("", rendered)
    text = html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def _heading_anchor_base(text: str) -> str:
    lowered = text.strip().casefold()
    slug = re.sub(r"[^\w\s\-가-힣]", "", lowered, flags=re.UNICODE)
    slug = re.sub(r"[\s_]+", "-", slug, flags=re.UNICODE)
    return slug.strip("-") or "section"


def _youtube_height_from_width(width: int) -> int:
    return max(1, round(width * 9 / 16))


def _youtube_width_from_height(height: int) -> int:
    return max(1, round(height * 16 / 9))


def _table_separator_layout(cells: list[str]) -> tuple[list[int], list[str | None]] | None:
    if not cells:
        return None

    widths: list[int] = []
    aligns: list[str | None] = []
    for cell in cells:
        value = cell.strip()
        match = TABLE_SEPARATOR_CELL_RE.fullmatch(value)
        if not match:
            return None
        widths.append(len(match.group("dashes")))

        align_left = value.startswith(":")
        align_right = value.endswith(":")
        if align_left and align_right:
            aligns.append("center")
        elif align_left:
            aligns.append("left")
        elif align_right:
            aligns.append("right")
        else:
            aligns.append(None)
    return widths, aligns


def _split_table_cells(text: str) -> list[str]:
    cells: list[str] = []
    start = 0
    pos = 0
    while pos < len(text):
        if text[pos] == "|" and not _is_escaped_pipe(text, pos):
            cells.append(text[start:pos].strip())
            start = pos + 1
        pos += 1
    cells.append(text[start:].strip())
    return cells


def _is_escaped_pipe(text: str, pos: int) -> bool:
    backslashes = 0
    pos -= 1
    while pos >= 0 and text[pos] == "\\":
        backslashes += 1
        pos -= 1
    return backslashes % 2 == 1


def _strip_pipe_table_row(line: str) -> str | None:
    text = line.rstrip("\n").rstrip(" \t")
    if not text.startswith("|") and text.startswith((" ", "\t")):
        text = text.lstrip(" ")
    if not text.startswith("|") or not text.endswith("|"):
        return None
    return text[1:-1]


def _strip_table_line(line: str) -> str | None:
    text = line.rstrip("\n").rstrip(" \t")
    if not text or "|" not in text:
        return None
    return text


def _parse_invalid_pipe_table(state: BlockState, pos: int) -> int:
    while pos < state.cursor_max:
        line = state.get_line(pos)
        if _strip_pipe_table_row(line) is None:
            break
        pos += len(line)
    state.add_paragraph(state.src[state.cursor : pos])
    return pos


def _table_boundaries(widths: tuple[int, ...]) -> list[Fraction]:
    total = sum(widths)
    cursor = 0
    boundaries = [Fraction(0, 1)]
    for width in widths:
        cursor += width
        boundaries.append(Fraction(cursor, total))
    return boundaries


def _table_grid(
    layouts: list[tuple[int, ...]],
) -> tuple[list[float] | None, dict[tuple[int, ...], list[int]]]:
    unique_layouts = list(dict.fromkeys(layouts))
    if not unique_layouts:
        return None, {}

    if len(unique_layouts) == 1:
        layout = unique_layouts[0]
        column_widths = None if all(width == 3 for width in layout) else [float(width) for width in layout]
        return column_widths, {layout: [1] * len(layout)}

    boundary_set = {Fraction(0, 1), Fraction(1, 1)}
    layout_boundaries: dict[tuple[int, ...], list[Fraction]] = {}
    for layout in unique_layouts:
        boundaries = _table_boundaries(layout)
        layout_boundaries[layout] = boundaries
        boundary_set.update(boundaries)

    grid_boundaries = sorted(boundary_set)
    boundary_indexes = {boundary: index for index, boundary in enumerate(grid_boundaries)}
    column_widths = [
        float(grid_boundaries[index + 1] - grid_boundaries[index])
        for index in range(len(grid_boundaries) - 1)
    ]

    spans: dict[tuple[int, ...], list[int]] = {}
    for layout, boundaries in layout_boundaries.items():
        layout_spans: list[int] = []
        for index in range(len(layout)):
            start = boundary_indexes[boundaries[index]]
            end = boundary_indexes[boundaries[index + 1]]
            layout_spans.append(end - start)
        spans[layout] = layout_spans
    return column_widths, spans


def _make_table_cells(
    cells: list[str],
    aligns: list[str | None],
    spans: list[int],
    *,
    head: bool,
) -> list[dict[str, object]]:
    children: list[dict[str, object]] = []
    for index, text in enumerate(cells):
        attrs: dict[str, object] = {"align": aligns[index], "head": head}
        if spans[index] > 1:
            attrs["colspan"] = spans[index]
        children.append({"type": "table_cell", "text": text.strip(), "attrs": attrs})
    return children


def extract_reference_targets(text: str) -> tuple[list[str], list[str]]:
    wiki_targets: list[str] = []
    template_targets: list[str] = []

    for match in WIKI_LINK_RE.finditer(text):
        raw = match.group(1).strip()
        if not raw:
            continue
        target = raw.split("|", 1)[0].strip()
        if target and not _is_external_wiki_target(target) and not _is_file_wiki_target(target):
            wiki_targets.append(target)

    for match in TEMPLATE_RE.finditer(text):
        target = match.group(1).strip()
        if target and toc_max_level_from_ref(target) is None:
            template_targets.append(target)

    return wiki_targets, template_targets


class WikiRenderContext:
    def __init__(
        self,
        *,
        resolve_doc_reference: Callable[[str], str | None],
        read_document: Callable[[str], str | None],
        list_tag_documents: Callable[[str], list[dict[str, object]]],
    ) -> None:
        self.resolve_doc_reference = resolve_doc_reference
        self.read_document = read_document
        self.list_tag_documents = list_tag_documents
        self.template_cache: dict[str, str] = {}


class PersonalWikiRenderer(mistune.HTMLRenderer):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._toc_headings: list[dict[str, object]] = []
        self._heading_id_counts: dict[str, int] = {}
        self._toc_placeholders: list[tuple[str, int]] = []

    def heading(self, text: str, level: int, **attrs) -> str:
        title = _heading_text_from_html(text)
        requested_id = str(attrs.get("id") or "").strip()
        anchor = requested_id or self._unique_heading_anchor(title)
        if title:
            self._toc_headings.append({"level": level, "title": title, "anchor": anchor})
        safe_anchor = html.escape(anchor, quote=True)
        return f'<h{level} id="{safe_anchor}">{text}</h{level}>\n'

    def table(self, text: str, column_widths: list[float] | None = None) -> str:
        colgroup = ""
        if column_widths:
            total = sum(column_widths)
            if total > 0:
                cols = "".join(
                    f'  <col style="width: {width / total * 100:.6g}%">\n'
                    for width in column_widths
                )
                colgroup = f"<colgroup>\n{cols}</colgroup>\n"
        return "<table>\n" + colgroup + text + "</table>\n"

    def table_cell(
        self,
        text: str,
        align: str | None = None,
        head: bool = False,
        colspan: int | None = None,
    ) -> str:
        tag = "th" if head else "td"
        attrs = ""
        if align:
            attrs += f' style="text-align:{align}"'
        if colspan and colspan > 1:
            attrs += f' colspan="{int(colspan)}"'
        return f"  <{tag}{attrs}>{text}</{tag}>\n"

    def callout(self, text: str, level: str) -> str:
        icon, label = CALLOUT_ICONS.get(level, ("i", level))
        safe_label = html.escape(label, quote=True)
        return (
            f'<div class="callout callout-{level}">'
            f'<span class="callout-icon" aria-label="{safe_label}" title="{safe_label}">{icon}</span>'
            f'<span class="callout-content">{text}</span>'
            "</div>\n"
        )

    def template_block(self, text: str) -> str:
        return text

    def folded_template_block(self, text: str, ref: str) -> str:
        safe_ref = html.escape(ref)
        return (
            '<details class="template-fold">'
            f"<summary>Template: {safe_ref}</summary>\n\n"
            f"{text}"
            "</details>\n"
        )

    def highlight(self, text: str) -> str:
        return f"<mark>{text}</mark>"

    def spoiler(self, text: str) -> str:
        return (
            '<span class="spoiler" role="button" tabindex="0" aria-pressed="false">'
            f"{text}"
            "</span>"
        )

    def raw_embed(self, text: str) -> str:
        return text

    def template_inline(self, text: str) -> str:
        return text

    def toc(self, max_level: int) -> str:
        placeholder = f"<!--PERSONALWIKI_TOC:{len(self._toc_placeholders)}-->"
        self._toc_placeholders.append((placeholder, max_level))
        return placeholder

    def render_toc_placeholders(self, rendered: str) -> str:
        for placeholder, max_level in self._toc_placeholders:
            rendered = rendered.replace(placeholder, self._toc_html(max_level), 1)
        return rendered

    def _unique_heading_anchor(self, title: str) -> str:
        base = _heading_anchor_base(title)
        count = self._heading_id_counts.get(base, 0) + 1
        self._heading_id_counts[base] = count
        if count == 1:
            return base
        return f"{base}-{count}"

    def _toc_html(self, max_level: int) -> str:
        items = [
            heading
            for heading in self._toc_headings
            if 1 <= int(heading["level"]) <= max_level
        ]
        if not items:
            return (
                '<nav class="wiki-toc wiki-toc-empty" aria-label="Table of contents">'
                '<strong class="wiki-toc-title">Table of Contents</strong>'
                "<p>표시할 heading이 없습니다.</p>"
                "</nav>"
            )

        rendered_items = []
        for heading in items:
            level = int(heading["level"])
            title = html.escape(str(heading["title"]))
            anchor = html.escape(str(heading["anchor"]), quote=True)
            rendered_items.append(
                f'<li class="wiki-toc-level-{level}"><a href="#{anchor}">{title}</a></li>'
            )
        return (
            '<nav class="wiki-toc" aria-label="Table of contents">'
            '<strong class="wiki-toc-title">Table of Contents</strong>'
            "<ol>"
            + "".join(rendered_items)
            + "</ol></nav>"
        )


def _get_context(env: MutableMapping[str, object]) -> WikiRenderContext:
    context = env.get(WIKI_CONTEXT_KEY)
    if not isinstance(context, WikiRenderContext):
        raise RuntimeError("personal wiki render context is missing")
    return context


def _load_template_content(ref: str, context: WikiRenderContext) -> str:
    slug = context.resolve_doc_reference(ref)
    if not slug:
        return f"\n\n> [Missing template: {ref}]\n\n"
    if slug in context.template_cache:
        return context.template_cache[slug]

    template_content = context.read_document(slug)
    if template_content is None:
        return f"\n\n> [Missing template file: {ref}]\n\n"
    context.template_cache[slug] = template_content
    return template_content


def _render_template_content(md: Markdown, env: MutableMapping[str, object], content: str) -> str:
    child = md.block.state_cls()
    child.env = env
    child.process(content if content.endswith("\n") else f"{content}\n")

    previous = env.get(TEMPLATE_DISABLED_KEY, False)
    env[TEMPLATE_DISABLED_KEY] = True
    try:
        md.block.parse(child)
        return str(md.render_state(child))
    finally:
        if previous:
            env[TEMPLATE_DISABLED_KEY] = previous
        else:
            env.pop(TEMPLATE_DISABLED_KEY, None)


def _render_template_ref(md: Markdown, env: MutableMapping[str, object], ref: str) -> str:
    context = _get_context(env)
    content = _load_template_content(ref, context)
    return _render_template_content(md, env, content)


def _blockquote_message(message: str) -> str:
    return f"<blockquote>\n<p>[{message}]</p>\n</blockquote>\n"


def _wiki_url(target: str, context: WikiRenderContext) -> str | None:
    if _is_file_wiki_target(target):
        relative = FILE_WIKI_TARGET_RE.sub("", target.strip(), count=1)
        relative = relative.lstrip("/\\").replace("\\", "/")
        if not relative:
            return None
        return f"/file/{quote(relative)}"
    if _is_external_wiki_target(target):
        return escape_url(target)
    slug = context.resolve_doc_reference(target)
    if slug:
        return f"/doc/{quote(slug)}"
    return f"/new?title={quote(target)}"


def _render_youtube(raw: str) -> str | None:
    match = YOUTUBE_RE.match(raw)
    if not match:
        return None

    parts = [part.strip() for part in match.group(1).split(",") if part.strip()]
    if not parts:
        return _blockquote_message("Invalid youtube embed: missing video id")

    video_id = parts[0]
    if not YOUTUBE_ID_RE.fullmatch(video_id):
        safe_id = html.escape(video_id)
        return _blockquote_message(f"Invalid youtube video id: {safe_id}")

    width: int | None = None
    height: int | None = None
    start: int | None = None
    for option in parts[1:]:
        if "=" not in option:
            continue
        key, value = [item.strip().lower() for item in option.split("=", 1)]
        parsed_dimension = _parse_dimension_option(value)
        if key == "width":
            if parsed_dimension is not None:
                width = parsed_dimension
        elif key == "height":
            if parsed_dimension is not None:
                height = parsed_dimension
        elif key == "start":
            parsed_start = _parse_timestamp(value)
            if parsed_start is not None and parsed_start >= 0:
                start = parsed_start

    if width is None and height is None:
        width = 560
        height = 315
    elif width is None:
        width = _youtube_width_from_height(height)
    elif height is None:
        height = _youtube_height_from_width(width)

    src = f"https://www.youtube.com/embed/{quote(video_id)}"
    if start is not None:
        src += f"?start={start}"
    return (
        '<div class="youtube-embed">'
        f'<iframe width="{width}" height="{height}" src="{src}" '
        'title="YouTube video player" loading="lazy" '
        'referrerpolicy="strict-origin-when-cross-origin" '
        'allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" '
        "allowfullscreen></iframe></div>"
    )


def _tag_name_from_embed(raw: str) -> str | None:
    match = TAG_EMBED_RE.fullmatch(raw.strip())
    if match is None:
        return None
    tag_name = match.group("tag").strip()
    return tag_name or None


def _render_tag_embed(raw: str, context: WikiRenderContext) -> str | None:
    tag_name = _tag_name_from_embed(raw)
    if tag_name is None:
        return None

    links = []
    for document in context.list_tag_documents(tag_name):
        title = html.escape(str(document["title"]))
        slug = quote(str(document["slug"]))
        links.append(f'<a href="/doc/{slug}">{title}</a>')
    return '<span class="tag-document-list">' + " ・ ".join(links) + "</span>"


def _parse_image_shortcut(raw: str) -> dict[str, object]:
    youtube = _render_youtube(raw)
    if youtube is not None:
        return {"type": "raw", "html": youtube}

    segments = [part.strip() for part in raw.split(",")]
    main = segments[0] if segments else ""
    options = segments[1:]

    if "|" in main:
        filename, alt = [part.strip() for part in main.split("|", 1)]
    else:
        filename = main
        alt = Path(filename).stem or "image"

    if not filename:
        return {"type": "raw", "html": _blockquote_message("Invalid image embed: missing file name")}

    if not alt:
        alt = Path(filename).stem or "image"

    width: int | None = None
    height: int | None = None
    for option in options:
        if "=" not in option:
            continue
        key, value = option.split("=", 1)
        key = key.strip().lower()
        parsed = _parse_dimension_option(value)
        if parsed is None:
            continue
        if key == "width":
            width = parsed
        elif key == "height":
            height = parsed

    safe = quote(filename.replace("\\", "/"))
    if width is None and height is None:
        return {"type": "image", "url": f"/img/{safe}", "alt": alt}

    safe_alt = html.escape(alt, quote=True)
    attrs = ""
    if width is not None:
        attrs += f' width="{width}"'
    if height is not None:
        attrs += f' height="{height}"'
    return {"type": "raw", "html": f'<img src="/img/{safe}" alt="{safe_alt}" loading="lazy"{attrs}>'}


def _parse_callout(block: mistune.BlockParser, match: Match[str], state: BlockState) -> int | None:
    raw_lines = [line for line in match.group(0).splitlines() if line.strip()]
    bodies: list[str] = []
    level = ""
    for line in raw_lines:
        line_match = CALLOUT_LINE_RE.match(line)
        if line_match is None:
            return None
        current_level = line_match.group("callout_level").lower()
        if level and current_level != level:
            return None
        level = current_level
        bodies.append(html.escape(line_match.group("callout_body").strip()))
    if not level or not bodies:
        return None

    state.append_token({"type": "callout", "raw": "<br>\n".join(bodies), "attrs": {"level": level}})
    return match.end()


def _parse_template_block(md: Markdown, block: mistune.BlockParser, match: Match[str], state: BlockState) -> int | None:
    if state.env.get(TEMPLATE_DISABLED_KEY):
        return None
    ref = match.group("template_block_ref").strip()
    if not ref:
        return None
    toc_max_level = toc_max_level_from_ref(ref)
    if toc_max_level is not None:
        state.append_token({"type": "toc", "attrs": {"max_level": toc_max_level}})
        return state.find_line_end()
    rendered = _render_template_ref(md, state.env, ref)
    state.append_token({"type": "template_block", "raw": rendered})
    return state.find_line_end()


def _parse_folded_template_block(
    md: Markdown,
    block: mistune.BlockParser,
    match: Match[str],
    state: BlockState,
) -> int | None:
    if state.env.get(TEMPLATE_DISABLED_KEY):
        return None
    ref = match.group("folded_template_block_ref").strip()
    if not ref:
        return None
    rendered = _render_template_ref(md, state.env, ref)
    state.append_token({"type": "folded_template_block", "raw": rendered, "attrs": {"ref": ref}})
    return state.find_line_end()


def _parse_raw_image_block(block: mistune.BlockParser, match: Match[str], state: BlockState) -> int | None:
    raw = match.group("raw_image_block_value").strip()
    tag_embed = _render_tag_embed(raw, _get_context(state.env))
    if tag_embed is not None:
        state.append_token({"type": "raw_embed", "raw": tag_embed})
        return state.find_line_end()
    parsed = _parse_image_shortcut(raw)
    if parsed["type"] != "raw":
        return None
    state.append_token({"type": "raw_embed", "raw": str(parsed["html"])})
    return state.find_line_end()


def _parse_to_end(
    inline: mistune.InlineParser,
    match: Match[str],
    state: InlineState,
    *,
    token_type: str,
    marker: str,
) -> int | None:
    pos = match.end()
    end_match = re.search(rf"(?<=\S){re.escape(marker)}", state.src[pos:])
    if not end_match:
        return None
    end_start = pos + end_match.start()
    end_pos = pos + end_match.end()
    child = state.copy()
    child.src = state.src[pos:end_start]
    children = inline.render(child)
    state.append_token({"type": token_type, "children": children})
    return end_pos


def _parse_highlight(inline: mistune.InlineParser, match: Match[str], state: InlineState) -> int | None:
    return _parse_to_end(inline, match, state, token_type="highlight", marker="==")


def _parse_spoiler(inline: mistune.InlineParser, match: Match[str], state: InlineState) -> int | None:
    return _parse_to_end(inline, match, state, token_type="spoiler", marker="||")


def _parse_wiki_link(inline: mistune.InlineParser, match: Match[str], state: InlineState) -> int:
    raw = match.group("wiki_link_value").strip()
    if "|" in raw:
        target, label = [part.strip() for part in raw.split("|", 1)]
    else:
        target, label = raw, raw

    context = _get_context(state.env)
    url = _wiki_url(target, context)
    if url is None:
        inline.process_text(label, state)
        return match.end()

    child = state.copy()
    child.src = label
    state.append_token(
        {
            "type": "link",
            "children": inline.render(child),
            "attrs": {"url": url},
        }
    )
    return match.end()


def _parse_image_shortcut_inline(inline: mistune.InlineParser, match: Match[str], state: InlineState) -> int:
    raw = match.group("image_shortcut_value").strip()
    tag_embed = _render_tag_embed(raw, _get_context(state.env))
    if tag_embed is not None:
        state.append_token({"type": "raw_embed", "raw": tag_embed})
        return match.end()
    parsed = _parse_image_shortcut(raw)
    if parsed["type"] == "image":
        state.append_token(
            {
                "type": "image",
                "children": [{"type": "text", "raw": str(parsed["alt"])}],
                "attrs": {"url": str(parsed["url"])},
            }
        )
    else:
        state.append_token({"type": "raw_embed", "raw": str(parsed["html"])})
    return match.end()


def _parse_template_inline(md: Markdown, inline: mistune.InlineParser, match: Match[str], state: InlineState) -> int | None:
    if state.env.get(TEMPLATE_DISABLED_KEY):
        return None
    ref = match.group("template_inline_ref").strip()
    if not ref:
        return None
    toc_max_level = toc_max_level_from_ref(ref)
    if toc_max_level is not None:
        state.append_token({"type": "toc", "attrs": {"max_level": toc_max_level}})
        return match.end()
    rendered = _render_template_ref(md, state.env, ref)
    state.append_token({"type": "template_inline", "raw": rendered})
    return match.end()


def _parse_folded_template_inline(
    md: Markdown,
    inline: mistune.InlineParser,
    match: Match[str],
    state: InlineState,
) -> int | None:
    if state.env.get(TEMPLATE_DISABLED_KEY):
        return None
    ref = match.group("folded_template_inline_ref").strip()
    if not ref:
        return None
    rendered = _render_template_ref(md, state.env, ref)
    state.append_token({"type": "folded_template_block", "raw": rendered, "attrs": {"ref": ref}})
    return match.end()


def _parse_custom_table(
    match: Match[str],
    state: BlockState,
    *,
    strip_row: Callable[[str], str | None],
    invalid_pipe_table_as_paragraph: bool,
) -> int | None:
    pos = match.end()
    header = strip_row(match.group(0))
    if header is None:
        return None

    align_line = state.get_line(pos)
    align = strip_row(align_line)
    if align is None:
        return None

    header_cells = _split_table_cells(header)
    layout = _table_separator_layout(_split_table_cells(align))
    if layout is None:
        if invalid_pipe_table_as_paragraph:
            return _parse_invalid_pipe_table(state, pos + len(align_line))
        return None
    active_widths, active_aligns = layout
    if len(header_cells) != len(active_aligns):
        if invalid_pipe_table_as_paragraph:
            return _parse_invalid_pipe_table(state, pos + len(align_line))
        return None

    pos += len(align_line)
    layouts = [tuple(active_widths)]
    body_rows: list[tuple[list[str], list[str | None], tuple[int, ...]]] = []

    while pos < state.cursor_max:
        line = state.get_line(pos)
        text = strip_row(line)
        if text is None:
            break

        cells = _split_table_cells(text)
        next_layout = _table_separator_layout(cells)
        if next_layout is not None:
            active_widths, active_aligns = next_layout
            layouts.append(tuple(active_widths))
            pos += len(line)
            continue
        if len(cells) != len(active_aligns):
            if invalid_pipe_table_as_paragraph:
                return _parse_invalid_pipe_table(state, pos + len(line))
            return None
        body_rows.append((cells, list(active_aligns), tuple(active_widths)))
        pos += len(line)

    column_widths, layout_spans = _table_grid(layouts)
    header_layout = tuple(layout[0])
    thead = {
        "type": "table_head",
        "children": _make_table_cells(
            header_cells,
            layout[1],
            layout_spans[header_layout],
            head=True,
        ),
    }
    rows = [
        {
            "type": "table_row",
            "children": _make_table_cells(cells, aligns, layout_spans[widths], head=False),
        }
        for cells, aligns, widths in body_rows
    ]

    token: dict[str, object] = {
        "type": "table",
        "children": [thead, {"type": "table_body", "children": rows}],
    }
    if column_widths:
        token["attrs"] = {"column_widths": column_widths}
    state.append_token(token)
    return pos


def _parse_table(block: mistune.BlockParser, match: Match[str], state: BlockState) -> int | None:
    return _parse_custom_table(
        match,
        state,
        strip_row=_strip_pipe_table_row,
        invalid_pipe_table_as_paragraph=True,
    )


def _parse_nptable(block: mistune.BlockParser, match: Match[str], state: BlockState) -> int | None:
    return _parse_custom_table(
        match,
        state,
        strip_row=_strip_table_line,
        invalid_pipe_table_as_paragraph=False,
    )


def personal_wiki_table(md: Markdown) -> None:
    md.block.register("table", mistune_table.TABLE_PATTERN, _parse_table, before="paragraph")
    md.block.register("nptable", mistune_table.NP_TABLE_PATTERN, _parse_nptable, before="paragraph")


def personal_wiki_syntax(md: Markdown) -> None:
    md.block.register(
        "folded_template_block",
        r"^[ \t]*\|\|[ \t]*\{\{(?P<folded_template_block_ref>[^{}\n]+)\}\}[ \t]*\|\|?[ \t]*(?=\n|$)",
        lambda block, match, state: _parse_folded_template_block(md, block, match, state),
        before="fenced_code",
    )
    md.block.register(
        "template_block",
        r"^[ \t]*\{\{(?P<template_block_ref>[^{}\n]+)\}\}[ \t]*(?=\n|$)",
        lambda block, match, state: _parse_template_block(md, block, match, state),
        before="fenced_code",
    )
    md.block.register(
        "raw_image_block",
        r"^[ \t]*!\[\[(?P<raw_image_block_value>[^\[\]\n]+)\]\][ \t]*(?=\n|$)",
        _parse_raw_image_block,
        before="fenced_code",
    )
    md.block.register(
        "callout",
        (
            r"^[ \t]*!!![ \t]*(?P<callout_level>note|info|warn|danger)[ \t]+[^\n]*"
            r"(?:\n[ \t]*!!![ \t]*(?P=callout_level)[ \t]+[^\n]*)*(?:\n|$)"
        ),
        _parse_callout,
        before="fenced_code",
    )

    md.inline.register(
        "image_shortcut",
        r"!\[\[(?P<image_shortcut_value>[^\[\]\n]+)\]\]",
        _parse_image_shortcut_inline,
        before="link",
    )
    md.inline.register(
        "wiki_link",
        r"(?<!!)\[\[(?P<wiki_link_value>[^\[\]\n]+)\]\]",
        _parse_wiki_link,
        before="link",
    )
    md.inline.register(
        "folded_template_inline",
        r"\|\|[ \t]*\{\{(?P<folded_template_inline_ref>[^{}\n]+)\}\}[ \t]*\|\|?",
        lambda inline, match, state: _parse_folded_template_inline(md, inline, match, state),
        before="link",
    )
    md.inline.register(
        "template_inline",
        r"\{\{(?P<template_inline_ref>[^{}\n]+)\}\}",
        lambda inline, match, state: _parse_template_inline(md, inline, match, state),
        before="link",
    )
    md.inline.register(
        "highlight",
        r"==(?=\S)",
        _parse_highlight,
        before="link",
    )
    md.inline.register(
        "spoiler",
        r"\|\|(?=\S)",
        _parse_spoiler,
        before="link",
    )


class MarkdownEngine:
    def __init__(self) -> None:
        pass

    def _create_markdown(self, renderer: PersonalWikiRenderer) -> Markdown:
        return mistune.create_markdown(
            escape=False,
            renderer=renderer,
            plugins=[
                "strikethrough",
                "table",
                personal_wiki_table,
                "task_lists",
                "url",
                "footnotes",
                personal_wiki_syntax,
            ],
        )

    def render(
        self,
        text: str,
        *,
        resolve_doc_reference: Callable[[str], str | None],
        read_document: Callable[[str], str | None],
        list_tag_documents: Callable[[str], list[dict[str, object]]],
    ) -> str:
        renderer = PersonalWikiRenderer(escape=False)
        markdown = self._create_markdown(renderer)
        state = markdown.block.state_cls()
        state.env[WIKI_CONTEXT_KEY] = WikiRenderContext(
            resolve_doc_reference=resolve_doc_reference,
            read_document=read_document,
            list_tag_documents=list_tag_documents,
        )
        rendered, _ = markdown.parse(text, state)
        return renderer.render_toc_placeholders(str(rendered))
