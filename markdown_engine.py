from __future__ import annotations

import html
import re
from pathlib import Path
from typing import Callable
from urllib.parse import quote

import mistune

WIKI_LINK_RE = re.compile(r"(?<!\!)\[\[([^\[\]]+)\]\]")
IMAGE_SHORTCUT_RE = re.compile(r"!\[\[([^\[\]]+)\]\]")
TEMPLATE_RE = re.compile(r"\{\{([^{}]+)\}\}")
HIGHLIGHT_RE = re.compile(r"==(?=\S)(.+?)(?<=\S)==")
SPOILER_RE = re.compile(r"\|\|(?=\S)(.+?)(?<=\S)\|\|")
CALLOUT_RE = re.compile(r"^\s*!!!\s*(note|info|warn|danger)\s+(.*)$", flags=re.IGNORECASE)
YOUTUBE_RE = re.compile(r"^youtube\((.*)\)$", flags=re.IGNORECASE)
YOUTUBE_ID_RE = re.compile(r"^[A-Za-z0-9_-]{6,20}$")
EXTERNAL_WIKI_TARGET_RE = re.compile(r"^(https?|file)://", flags=re.IGNORECASE)


def _is_external_wiki_target(target: str) -> bool:
    return EXTERNAL_WIKI_TARGET_RE.match(target.strip()) is not None


def _parse_dimension(value: str, default: int) -> int:
    if not value.isdigit():
        return default
    parsed = int(value)
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
        if target and not _is_external_wiki_target(target):
            wiki_targets.append(target)

    for match in TEMPLATE_RE.finditer(text):
        target = match.group(1).strip()
        if target:
            template_targets.append(target)

    return wiki_targets, template_targets


class MarkdownEngine:
    def __init__(self) -> None:
        self.renderer = mistune.create_markdown(
            escape=False,
            plugins=["strikethrough", "table", "task_lists", "url", "footnotes"],
        )

    def render(
        self,
        text: str,
        *,
        resolve_doc_reference: Callable[[str], str | None],
        read_document: Callable[[str], str | None],
    ) -> str:
        processed = self.preprocess(
            text,
            resolve_doc_reference=resolve_doc_reference,
            read_document=read_document,
        )
        return self.renderer(processed)

    def preprocess(
        self,
        text: str,
        *,
        resolve_doc_reference: Callable[[str], str | None],
        read_document: Callable[[str], str | None],
    ) -> str:
        processed = self._expand_templates_once(
            text,
            resolve_doc_reference=resolve_doc_reference,
            read_document=read_document,
        )
        processed = self._replace_callouts(processed)
        processed = self._replace_highlights(processed)
        processed = self._replace_spoilers(processed)
        processed = self._replace_image_shortcuts(processed)
        processed = self._replace_wiki_links(processed, resolve_doc_reference=resolve_doc_reference)
        return processed

    def _expand_templates_once(
        self,
        text: str,
        *,
        resolve_doc_reference: Callable[[str], str | None],
        read_document: Callable[[str], str | None],
    ) -> str:
        # Expand only one pass so nested templates are never expanded recursively.
        cache: dict[str, str] = {}

        def repl(match: re.Match[str]) -> str:
            ref = match.group(1).strip()
            slug = resolve_doc_reference(ref)
            if not slug:
                return f"\n\n> [Missing template: {ref}]\n\n"
            if slug in cache:
                return cache[slug]
            template_content = read_document(slug)
            if template_content is None:
                return f"\n\n> [Missing template file: {ref}]\n\n"
            cache[slug] = template_content
            return template_content

        return TEMPLATE_RE.sub(repl, text)

    def _replace_callouts(self, text: str) -> str:
        lines: list[str] = []
        for line in text.splitlines():
            match = CALLOUT_RE.match(line)
            if not match:
                lines.append(line)
                continue
            level = match.group(1).lower()
            body = html.escape(match.group(2).strip())
            lines.append(f'<div class="callout callout-{level}"><strong>{level.upper()}</strong> {body}</div>')
        return "\n".join(lines)

    def _replace_highlights(self, text: str) -> str:
        return HIGHLIGHT_RE.sub(lambda m: f"<mark>{m.group(1)}</mark>", text)

    def _replace_spoilers(self, text: str) -> str:
        return SPOILER_RE.sub(
            lambda m: (
                '<span class="spoiler" role="button" tabindex="0" aria-pressed="false">'
                f"{m.group(1)}"
                "</span>"
            ),
            text,
        )

    def _render_youtube(self, raw: str) -> str | None:
        match = YOUTUBE_RE.match(raw)
        if not match:
            return None

        parts = [part.strip() for part in match.group(1).split(",") if part.strip()]
        if not parts:
            return "\n\n> [Invalid youtube embed: missing video id]\n\n"

        video_id = parts[0]
        if not YOUTUBE_ID_RE.fullmatch(video_id):
            safe_id = html.escape(video_id)
            return f"\n\n> [Invalid youtube video id: {safe_id}]\n\n"

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

    def _replace_image_shortcuts(self, text: str) -> str:
        def repl(match: re.Match[str]) -> str:
            raw = match.group(1).strip()
            youtube = self._render_youtube(raw)
            if youtube is not None:
                return youtube

            if "|" in raw:
                filename, alt = [part.strip() for part in raw.split("|", 1)]
            else:
                filename = raw
                alt = Path(filename).stem or "image"
            safe = quote(filename.replace("\\", "/"))
            return f"![{alt}](/img/{safe})"

        return IMAGE_SHORTCUT_RE.sub(repl, text)

    def _replace_wiki_links(self, text: str, *, resolve_doc_reference: Callable[[str], str | None]) -> str:
        def repl(match: re.Match[str]) -> str:
            raw = match.group(1).strip()
            if "|" in raw:
                target, label = [part.strip() for part in raw.split("|", 1)]
            else:
                target, label = raw, raw
            if _is_external_wiki_target(target):
                return f"[{label}](<{target}>)"
            slug = resolve_doc_reference(target)
            if slug:
                return f"[{label}](/doc/{quote(slug)})"
            return f"[{label}](/new?title={quote(target)})"

        return WIKI_LINK_RE.sub(repl, text)
