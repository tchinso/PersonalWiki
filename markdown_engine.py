from __future__ import annotations

import html
import re
from pathlib import Path
from typing import Callable, Match, MutableMapping
from urllib.parse import quote

import mistune
from mistune import BlockState, InlineState, Markdown
from mistune.util import escape_url

WIKI_LINK_RE = re.compile(r"(?<!\!)\[\[([^\[\]]+)\]\]")
IMAGE_SHORTCUT_RE = re.compile(r"!\[\[([^\[\]]+)\]\]")
TEMPLATE_RE = re.compile(r"\{\{([^{}]+)\}\}")
FOLDED_TEMPLATE_RE = re.compile(r"\|\|\s*\{\{([^{}]+)\}\}\s*\|\|?")
YOUTUBE_RE = re.compile(r"^youtube\((.*)\)$", flags=re.IGNORECASE)
YOUTUBE_ID_RE = re.compile(r"^[A-Za-z0-9_-]{6,20}$")
EXTERNAL_WIKI_TARGET_RE = re.compile(r"^(https?|file)://", flags=re.IGNORECASE)
FILE_WIKI_TARGET_RE = re.compile(r"^file/", flags=re.IGNORECASE)

WIKI_CONTEXT_KEY = "personal_wiki_context"
TEMPLATE_DISABLED_KEY = "personal_wiki_template_disabled"


def _is_external_wiki_target(target: str) -> bool:
    return EXTERNAL_WIKI_TARGET_RE.match(target.strip()) is not None


def _is_file_wiki_target(target: str) -> bool:
    return FILE_WIKI_TARGET_RE.match(target.strip()) is not None


def _parse_dimension(value: str, default: int) -> int:
    if not value.isdigit():
        return default
    parsed = int(value)
    if parsed < 120:
        return 120
    if parsed > 3840:
        return 3840
    return parsed


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
        if target:
            template_targets.append(target)

    return wiki_targets, template_targets


class WikiRenderContext:
    def __init__(
        self,
        *,
        resolve_doc_reference: Callable[[str], str | None],
        read_document: Callable[[str], str | None],
    ) -> None:
        self.resolve_doc_reference = resolve_doc_reference
        self.read_document = read_document
        self.template_cache: dict[str, str] = {}


class PersonalWikiRenderer(mistune.HTMLRenderer):
    def callout(self, text: str, level: str) -> str:
        return f'<div class="callout callout-{level}"><strong>{level.upper()}</strong> {text}</div>\n'

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

    width = 560
    height = 315
    for option in parts[1:]:
        if "=" not in option:
            continue
        key, value = [item.strip().lower() for item in option.split("=", 1)]
        if key == "width":
            width = _parse_dimension(value, width)
        elif key == "height":
            height = _parse_dimension(value, height)

    src = f"https://www.youtube.com/embed/{quote(video_id)}"
    return (
        '<div class="youtube-embed">'
        f'<iframe width="{width}" height="{height}" src="{src}" '
        'title="YouTube video player" loading="lazy" '
        'referrerpolicy="strict-origin-when-cross-origin" '
        'allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" '
        "allowfullscreen></iframe></div>"
    )


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


def _parse_callout(block: mistune.BlockParser, match: Match[str], state: BlockState) -> int:
    level = match.group("callout_level").lower()
    body = html.escape(match.group("callout_body").strip())
    state.append_token({"type": "callout", "raw": body, "attrs": {"level": level}})
    return state.find_line_end()


def _parse_template_block(md: Markdown, block: mistune.BlockParser, match: Match[str], state: BlockState) -> int | None:
    if state.env.get(TEMPLATE_DISABLED_KEY):
        return None
    ref = match.group("template_block_ref").strip()
    if not ref:
        return None
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
        r"^[ \t]*!!![ \t]*(?P<callout_level>note|info|warn|danger)[ \t]+(?P<callout_body>[^\n]*)",
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
        self.markdown = mistune.create_markdown(
            escape=False,
            renderer=PersonalWikiRenderer(escape=False),
            plugins=["strikethrough", "table", "task_lists", "url", "footnotes", personal_wiki_syntax],
        )

    def render(
        self,
        text: str,
        *,
        resolve_doc_reference: Callable[[str], str | None],
        read_document: Callable[[str], str | None],
    ) -> str:
        state = self.markdown.block.state_cls()
        state.env[WIKI_CONTEXT_KEY] = WikiRenderContext(
            resolve_doc_reference=resolve_doc_reference,
            read_document=read_document,
        )
        rendered, _ = self.markdown.parse(text, state)
        return str(rendered)
