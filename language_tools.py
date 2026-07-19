from __future__ import annotations

import hashlib
import math
import os
import re
import sqlite3
import threading
from collections import Counter, OrderedDict, defaultdict, deque
from datetime import datetime
from functools import lru_cache

ENGLISH_TOKEN_RE = re.compile(r"(?<![A-Za-z0-9])[A-Za-z0-9]{2,}(?![A-Za-z0-9])")
KOREAN_TOKEN_RE = re.compile(r"[가-힣]{2,}")
KOREAN_CHAR_RE = re.compile(r"[가-힣]")
ENGLISH_STOPWORDS = {
    "the",
    "a",
    "an",
    "and",
    "or",
    "not",
    "with",
    "from",
    "into",
    "about",
    "that",
    "this",
    "there",
    "their",
    "your",
    "you",
    "for",
    "are",
    "was",
    "were",
    "been",
    "will",
    "shall",
    "have",
    "has",
    "had",
    "can",
    "could",
    "would",
    "should",
    "to",
    "of",
    "in",
    "on",
    "at",
    "as",
    "is",
    "it",
    "its",
    "by",
    "be",
    "if",
    "else",
    "when",
    "where",
    "which",
    "who",
    "what",
    "why",
    "how",
    "img",
    "file",
    "http",
    "https",
    "br",
    "youtube",
    "tag",
    "width",
    "height",
    "info",
    "warn",
    "danger",
    "note",
    "png",
    "jpg",
    "jpeg",
    "webp",
    "avif",
    "gif",
    "zip",
    "rar",
    "7z",
    "mp3",
    "mp4",
    "m4a",
    "mkv",
    "bat",
    "ps1",
    "txt",
    "html",
    "mhtml",
    "htm",
    "com",
    "net",
    "org",
    "www",
    "co",
    "kr",
    "exe",
    "jp",
    "gov",
    "edu",
}
KOREAN_STOPWORDS = {
    "문서",
    "내용",
    "그리고",
    "또는",
    "에서",
    "으로",
    "입니다",
    "있는",
    "하는",
    "합니다",
    "대한",
    "통해",
    "관련",
    "사용",
    "기능",
    "추가",
    "그것",
    "이것",
    "저것",
    "여기",
    "거기",
    "저기",
    "같은",
    "다른",
    "모든",
    "각각",
    "해당",
    "경우",
    "정도",
    "위해",
    "위한",
    "때문",
    "중에서",
    "대해서",
    "대하여",
    "또한",
    "이미",
    "먼저",
    "나중",
    "이번",
    "다음",
    "아래",
    "위의",
    "하지만",
    "그러나",
    "그러면",
    "즉시",
    "아주",
    "매우",
    "정말",
    "조금",
    "많이",
    "있습니다",
    "없습니다",
    "같습니다",
    "이런",
    "그런",
    "저런",
    "어떤",
    "누구",
    "무엇",
    "어디",
    "언제",
    "그래서",
    "그래도",
    "따라서",
    "예를",
    "예시",
    "일단",
    "계속",
    "현재",
    "이후",
    "이전",
    "전부",
    "전체",
    "부분",
    "대부분",
    "주요",
    "기본",
    "직접",
    "간접",
    "가능",
    "불가능",
    "필요",
    "필요한",
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
KOREAN_STOPWORDS_LONGEST_FIRST = tuple(sorted(KOREAN_STOPWORDS, key=lambda word: (-len(word), word)))
KOREAN_STOPWORD_RE = re.compile("|".join(re.escape(word) for word in KOREAN_STOPWORDS_LONGEST_FIRST))


MARKDOWN_EMBED_IGNORE_SPAN_RE = re.compile(
    r"!\[\[[^\]\r\n]*\]\]|!\[[^\]\r\n]*\]\([^\r\n)]*\)"
)

TAG_RECOMMEND_TOKENIZER_VERSION = "tag-token-v4-ignore-markdown-embeds-and-tag"
TAG_RECOMMEND_IDF_EXPONENT = 2.0
TAG_RECOMMEND_SIMILAR_DOC_LIMIT = 30
TAG_RECOMMEND_LIMIT = 25
TAG_RECOMMEND_RANK_WEIGHT_EXPONENT = 1.7
TAG_RECOMMEND_MIN_TOKENS_PER_DOC = 512
TAG_RECOMMEND_MID_TOKENS_PER_DOC = 768
TAG_RECOMMEND_MAX_TOKENS_PER_DOC = 1024
TAG_RECOMMEND_ADAPTIVE_MID_INDEX = 300
TAG_RECOMMEND_ADAPTIVE_MAX_INDEX = 400
TAG_RECOMMEND_ADAPTIVE_MIN_TF = 2
TAG_RECOMMEND_FTS_CANDIDATE_LIMIT = 250
TAG_RECOMMEND_FTS_MAX_QUERY_TERMS = 50
TAG_RECOMMEND_TOKEN_DB_MAX_QUERY_TERMS = 50
TAG_RECOMMEND_TOKEN_DB_CANDIDATE_LIMIT = 500
TAG_RECOMMEND_TOKEN_DB_MAX_DF_RATIO = 0.20
TAG_RECOMMEND_MIN_CANDIDATE_FALLBACK = 50
TAG_RECOMMEND_FULL_SCAN_MAX_DOCS = 1000
TAG_RECOMMEND_QUERY_VECTOR_MAX_TOKENS = 256
TAG_RECOMMEND_SCORE_DOC_LIMIT = 750
TAG_RECOMMEND_CACHE_MAX_ENTRIES = 128
TAG_RECOMMEND_DEBUG = os.environ.get("PERSONALWIKI_TAG_RECOMMEND_DEBUG") == "1"
SQLITE_IN_CLAUSE_CHUNK_SIZE = 400

# Keep the database longest-first even when new rules are appended later.
KOREAN_SPELL_REPLACE_DB: tuple[tuple[str, str], ...] = tuple(sorted((
    ("컨텐츠", "콘텐츠"),
    ("다던지", "다든지"),
    ("다던가", "다든가"),
    ("나뉘어져", "나뉘어"),
    ("나눠져", "나뉘어"),
    ("이떄문에", "이 때문에"),
    ("그떄문에", "그 때문에"),
    ("는것", "는 것"),
    ("는게", "는 게"),
    ("인게", "인 게"),
    ("여러가지", "여러 가지"),
    ("여러개", "여러 개"),
    ("을때", "을 때"),
    ("을떄", "을 때"),
    ("일때", "일 때"),
    ("일떄", "일 때"),
    ("할때", "할 때"),
    ("할떄", "할 때"),
    ("된때", "된 때"),
    ("된떄", "된 때"),
    ("던때", "던 때"),
    ("던떄", "던 때"),
    ("이 때", "이때"),
    ("이 떄", "이때"),
    ("그 때", "그때"),
    ("그 떄", "그때"),
    ("한 때", "한때"),
    ("한 떄", "한때"),
    ("을것", "을 것"),
    ("일것", "일 것"),
    ("할것", "할 것"),
    ("한것", "한 것"),
    ("된것", "된 것"),
    ("던것", "던 것"),
    ("린것", "린 것"),
    ("치뤘다", "치렀다"),
    ("기때문", "기 때문"),
    ("기떄문", "기 때문"),
    ("한가지", "한 가지"),
    ("번번히", "번번이"),
    ("또 다시", "또다시"),
    ("맞은 편", "맞은편"),
    ("이로서", "이로써"),
    ("일려", "이려"),
    ("않는이상", "않는 이상"),
    ("얼만큼", "얼마만큼"),
    ("뇌졸증", "뇌졸중"),
    ("째째하", "쩨쩨하"),
    ("째째한", "쩨쩨한"),
    ("돋보적", "독보적"),
    ("이때문에", "이 때문에"),
    ("그때문에", "그 때문에"),
    ("제 때", "제때"),
    ("제 떄", "제때"),
    ("그 것", "그것"),
    ("그 날", "그날"),
    ("저 것", "저것"),
    ("다름아", "다름 아"),
    ("이제와서", "이제 와서"),
    ("오래동안", "오랫동안"),
    ("오랜동안", "오랫동안"),
    ("다행이도", "다행히도"),
    ("받아드리", "받아들이"),
    ("뒤집혀졌", "뒤집혔"),
    ("그럴 수 밖에", "그럴 수밖에"),
    ("어느정도", "어느 정도"),
    ("이를 테면", "이를테면"),
    ("등장 인물", "등장인물"),
    ("못지 않다", "못지않다"),
    ("아무 것", "아무것"),
    ("오래 전", "오래전"),
    ("갯수", "개수"),
    ("곰곰히", "곰곰이"),
    ("기여코", "기어코"),
    ("깨끗히", "깨끗이"),
    ("나날히", "나날이"),
    ("다행이", "다행히"),
    ("누누히", "누누이"),
    ("일일히", "일일이"),
    ("줄줄히", "줄줄이"),
    ("넉넉치", "넉넉지"),
    ("녹록치", "녹록지"),
    ("익숙치", "익숙지"),
    ("짐작케", "짐작게"),
    ("탐탁찮", "탐탁잖"),
    ("탐탁치", "탐탁지"),
    ("노랑색", "노란색"),
    ("빨강색", "빨간색"),
    ("파랑색", "파란색"),
    ("검정색", "검은색"),
    ("높혀", "높여"),
    ("높혔", "높였"),
    ("높힐", "높일"),
    ("붙혀", "붙여"),
    ("붙혔", "붙였"),
    ("붙힐", "붙일"),
    ("다싶이", "다시피"),
    ("대려가", "데려가"),
    ("대리고", "데리고"),
    ("댓가", "대가"),
    ("되야", "돼야"),
    ("되버", "돼 버"),
    ("되있", "돼있"),
    ("되서", "돼서"),
    ("바꼈", "바뀌었"),
    ("보여지", "보이"),
    ("불리우", "불리"),
    ("불리운", "불린"),
    ("불리웠", "불렸"),
    ("불리울", "불릴"),
    ("불리워", "불려"),
    ("본따", "본떠"),
    ("본딴", "본뜬"),
    ("실날", "실낱"),
    ("스폐셜", "스페셜"),
    ("알맞는", "알맞은"),
    ("여러므로", "여러모로"),
    ("주서", "주워"),
    ("쯤음", "즈음"),
    ("쓸대", "쓸데"),
    ("쓸떄", "쓸데"),
    ("치루는", "치르는"),
    ("치룰", "치를"),
    ("치뤘", "치렀"),
    ("치뤄", "치러"),
    ("치룸", "치름"),
    ("치루게", "치르게"),
    ("치루고", "치르고"),
    ("치루기", "치르기"),
    ("치루지", "치르지"),
    ("치루며", "치르며"),
    ("치루면", "치르면"),
    ("치루던", "치르던"),
    ("치루려", "치르려"),
    ("치루었", "치렀"),
    ("치루어", "치러"),
    ("치뤄져", "치러져"),
    ("표효", "포효"),
    ("헛점", "허점"),
    ("프롬포트", "프롬프트"),
    ("옳바", "올바"),
    ("확율", "확률"),
    ("유렵", "유럽"),
    ("할려", "하려"),
    ("죽을려", "죽으려"),
    ("패쇄", "폐쇄"),
    ("폐쇠", "폐쇄"),
    ("아니였", "아니었"),
    ("햇갈", "헷갈"),
    ("쓸때", "쓸데"),
    ("들어나는", "드러나는"),
    ("들어나면", "드러나면"),
    ("든줄", "든 줄"),
    ("따음표", "따옴표"),
    ("떄", "때"),
    ("왠만", "웬만"),
    ("걸맞는", "걸맞은"),
    ("건내다", "건네다"),
    ("과부화", "과부하"),
    ("꺼려하다", "꺼리다"),
    ("대체제", "대체재"),
    ("말빨", "말발"),
    ("화장빨", "화장발"),
    ("약빨", "약발"),
    ("배끼다", "베끼다"),
    ("배풀다", "베풀다"),
    ("잇점", "이점"),
    ("가디건", "카디건"),
    ("나레이션", "내레이션"),
    ("넌센스", "난센스"),
    ("데미지", "대미지"),
    ("라이센스", "라이선스"),
    ("레포트", "리포트"),
    ("메뉴얼", "매뉴얼"),
    ("메세지", "메시지"),
    ("네비게이", "내비게이"),
    ("패널티", "페널티"),
    ("샤베트", "셔벗"),
    ("세레모니", "세리머니"),
    ("알콜", "알코올"),
    ("앙케이트", "앙케트"),
    ("앵콜", "앙코르"),
    ("어플", "앱"),
    ("스케쥴", "스케줄"),
    ("엘레베이터", "엘리베이터"),
    ("타겟", "타깃"),
    ("타란튤라", "타란툴라"),
    ("헐리우드", "할리우드"),
    ("헐리웃", "할리우드"),
    ("랍퍼", "래퍼"),
    ("런닝", "러닝"),
    ("쎄다", "세다"),
    ("쎄고", "세고"),
    ("쎄지", "세지"),
    ("쎄진", "세진"),
    ("쎄게", "세게"),
    ("썸머", "서머"),
), key=lambda pair: len(pair[0]), reverse=True))
KOREAN_SPELL_SAMPLE_LIMIT = 8
KOREAN_SPELL_EXCLUDED_SUFFIXES: dict[str, tuple[str, ...]] = {
    "이 때": ("문",),
    "그 때": ("문",),
}

_TAG_RECOMMENDATION_CACHE: OrderedDict[tuple[object, ...], tuple[str, ...]] = OrderedDict()
_TAG_RECOMMENDATION_CACHE_GENERATION = 0
_TAG_RECOMMENDATION_CACHE_LOCK = threading.RLock()


def invalidate_tag_recommendation_cache() -> None:
    """Discard results derived from the mutable document/tag indexes."""
    global _TAG_RECOMMENDATION_CACHE_GENERATION
    with _TAG_RECOMMENDATION_CACHE_LOCK:
        _TAG_RECOMMENDATION_CACHE_GENERATION += 1
        _TAG_RECOMMENDATION_CACHE.clear()


def _tag_recommendation_cache_key(
    *,
    title: str,
    content: str,
    current_slug: str | None,
    exclude_tags: list[str] | None,
    limit: int,
) -> tuple[object, ...]:
    digest = hashlib.blake2b(digest_size=20)
    digest.update(title.encode("utf-8"))
    digest.update(b"\0")
    digest.update(content.encode("utf-8"))
    excluded = tuple(sorted({tag.casefold() for tag in (exclude_tags or [])}))
    return (digest.digest(), current_slug or "", excluded, limit)


def _get_cached_tag_recommendations(key: tuple[object, ...]) -> tuple[int, list[str] | None]:
    with _TAG_RECOMMENDATION_CACHE_LOCK:
        generation = _TAG_RECOMMENDATION_CACHE_GENERATION
        cached = _TAG_RECOMMENDATION_CACHE.get(key)
        if cached is None:
            return generation, None
        _TAG_RECOMMENDATION_CACHE.move_to_end(key)
        return generation, list(cached)


def _cache_tag_recommendations(
    key: tuple[object, ...],
    generation: int,
    recommendations: list[str],
) -> None:
    with _TAG_RECOMMENDATION_CACHE_LOCK:
        if generation != _TAG_RECOMMENDATION_CACHE_GENERATION:
            return
        _TAG_RECOMMENDATION_CACHE[key] = tuple(recommendations)
        _TAG_RECOMMENDATION_CACHE.move_to_end(key)
        while len(_TAG_RECOMMENDATION_CACHE) > TAG_RECOMMEND_CACHE_MAX_ENTRIES:
            _TAG_RECOMMENDATION_CACHE.popitem(last=False)


KOREAN_SPELL_REPLACEMENTS = dict(KOREAN_SPELL_REPLACE_DB)
KOREAN_SPELL_MAX_PATTERN_LENGTH = max((len(wrong) for wrong, _replace in KOREAN_SPELL_REPLACE_DB), default=0)


def build_korean_spell_automaton(
    pairs: tuple[tuple[str, str], ...],
) -> tuple[list[dict[str, int]], list[int], list[list[str]]]:
    transitions: list[dict[str, int]] = [{}]
    failure_links: list[int] = [0]
    outputs: list[list[str]] = [[]]

    for wrong, _replace in pairs:
        if not wrong:
            continue
        state = 0
        for char in wrong:
            next_state = transitions[state].get(char)
            if next_state is None:
                next_state = len(transitions)
                transitions[state][char] = next_state
                transitions.append({})
                failure_links.append(0)
                outputs.append([])
            state = next_state
        outputs[state].append(wrong)

    queue: deque[int] = deque(transitions[0].values())
    while queue:
        state = queue.popleft()
        for char, next_state in transitions[state].items():
            queue.append(next_state)
            fallback = failure_links[state]
            while fallback and char not in transitions[fallback]:
                fallback = failure_links[fallback]
            failure_links[next_state] = transitions[fallback].get(char, 0)
            outputs[next_state].extend(outputs[failure_links[next_state]])

    return transitions, failure_links, outputs


@lru_cache(maxsize=1)
def get_korean_spell_automaton() -> tuple[list[dict[str, int]], list[int], list[list[str]]]:
    return build_korean_spell_automaton(KOREAN_SPELL_REPLACE_DB)


def iter_korean_spell_matches(text: str):
    if not text:
        return

    transitions, failure_links, outputs = get_korean_spell_automaton()
    state = 0
    for index, char in enumerate(text):
        while state and char not in transitions[state]:
            state = failure_links[state]
        state = transitions[state].get(char, 0)
        for wrong in outputs[state]:
            start = index - len(wrong) + 1
            excluded_suffixes = KOREAN_SPELL_EXCLUDED_SUFFIXES.get(wrong, ())
            if any(text.startswith(suffix, index + 1) for suffix in excluded_suffixes):
                continue
            yield start, index + 1, wrong, KOREAN_SPELL_REPLACEMENTS[wrong]


def _best_korean_spell_match(matches: list[tuple[int, int, str, str]]) -> tuple[int, int, str, str]:
    best_index = 0
    best = matches[0]
    best_key = (best[0], -(best[1] - best[0]))
    for index, match in enumerate(matches[1:], start=1):
        key = (match[0], -(match[1] - match[0]))
        if key < best_key:
            best_index = index
            best = match
            best_key = key
    return matches.pop(best_index)


def iter_selected_korean_spell_replacements(text: str):
    if not text or KOREAN_SPELL_MAX_PATTERN_LENGTH <= 0:
        return

    cursor = 0
    pending: list[tuple[int, int, str, str]] = []

    for start, end, wrong, replace in iter_korean_spell_matches(text):
        if start < cursor:
            continue

        pending.append((start, end, wrong, replace))
        safe_start = end - KOREAN_SPELL_MAX_PATTERN_LENGTH
        while pending:
            pending = [match for match in pending if match[0] >= cursor]
            if not pending:
                break
            earliest_start = min(match[0] for match in pending)
            if earliest_start > safe_start:
                break
            selected = _best_korean_spell_match(pending)
            yield selected
            cursor = selected[1]

    while pending:
        pending = [match for match in pending if match[0] >= cursor]
        if not pending:
            break
        selected = _best_korean_spell_match(pending)
        yield selected
        cursor = selected[1]


def select_korean_spell_replacements(text: str) -> list[tuple[int, int, str, str]]:
    matches = list(iter_selected_korean_spell_replacements(text))
    if not matches:
        return []
    return matches


def collect_korean_spell_issues(title: str, content: str) -> dict[str, object] | None:
    count = 0
    samples: list[dict[str, str]] = []
    sampled: set[tuple[str, str, str]] = set()

    for field_label, text in (("제목", title), ("본문", content)):
        for _start, _end, wrong, replace in iter_selected_korean_spell_replacements(text):
            count += 1
            sample_key = (field_label, wrong, replace)
            if len(samples) >= KOREAN_SPELL_SAMPLE_LIMIT or sample_key in sampled:
                continue
            sampled.add(sample_key)
            samples.append(
                {
                    "field": field_label,
                    "wrong": wrong,
                    "replace": replace,
                }
            )

    if count == 0:
        return None
    return {
        "count": count,
        "samples": samples,
    }


def apply_korean_spell_replacements(text: str) -> str:
    parts: list[str] = []
    cursor = 0
    changed = False
    for start, end, _wrong, replace in iter_selected_korean_spell_replacements(text):
        changed = True
        parts.append(text[cursor:start])
        parts.append(replace)
        cursor = end
    if not changed:
        return text
    parts.append(text[cursor:])
    return "".join(parts)


def apply_korean_spell_autofix(title: str, content: str) -> tuple[str, str]:
    return (
        apply_korean_spell_replacements(title).strip(),
        apply_korean_spell_replacements(content),
    )


def korean_spell_warning_message(issue_count: object) -> str:
    return (
        f"맞춤법 자동교정 후보 {issue_count}곳을 찾았습니다. "
        "자동수정하고 저장하거나, 수정하지 않고 이대로 저장할 수 있습니다."
    )


def _build_doc_tag_map(conn: sqlite3.Connection, doc_ids: list[int]) -> dict[int, list[str]]:
    unique_doc_ids: list[int] = []
    seen: set[int] = set()
    for doc_id in doc_ids:
        if doc_id in seen:
            continue
        seen.add(doc_id)
        unique_doc_ids.append(doc_id)
    if not unique_doc_ids:
        return {}

    mapping: dict[int, list[str]] = defaultdict(list)
    for chunk in _chunked(unique_doc_ids):
        placeholders = ",".join("?" for _ in chunk)
        rows = conn.execute(
            f"""
            SELECT dt.doc_id, t.name
            FROM doc_tags dt
            JOIN tags t ON t.id = dt.tag_id
            WHERE dt.doc_id IN ({placeholders})
            ORDER BY dt.doc_id, t.name COLLATE NOCASE
            """,
            list(chunk),
        ).fetchall()
        for row in rows:
            mapping[int(row["doc_id"])].append(str(row["name"]))
    return dict(mapping)

@lru_cache(maxsize=8192)
def singularize_token(token: str) -> str:
    if not token.isascii() or not token.isalpha():
        return token
    if token.endswith("ies") and len(token) > 4:
        return token[:-3] + "y"
    if token.endswith(("sses", "xes", "zes", "ches", "shes")) and len(token) > 4:
        return token[:-2]
    if token.endswith("uses") and len(token) > 4:
        return token[:-2]
    if token.endswith("s") and len(token) > 3 and not token.endswith("ss"):
        return token[:-1]
    return token


def remove_korean_stopwords_aggressively(text: str) -> str:
    return KOREAN_STOPWORD_RE.sub("", text)


def is_low_value_numeric_token(token: str) -> bool:
    return token.isdigit() and len(token) <= 4


def is_korean_token(token: str) -> bool:
    return KOREAN_CHAR_RE.search(token) is not None


def iter_tokenizable_segments(text: str):
    start = 0
    for match in MARKDOWN_EMBED_IGNORE_SPAN_RE.finditer(text):
        if match.start() > start:
            yield text[start : match.start()]
        start = max(start, match.end())
    if start < len(text):
        yield text[start:]


def iter_tokens_from_segment(segment: str):
    lowered = segment.lower()
    for match in ENGLISH_TOKEN_RE.finditer(lowered):
        raw = match.group(0)
        token = singularize_token(raw)
        if is_low_value_numeric_token(token):
            continue
        if token in ENGLISH_STOPWORDS:
            continue
        if len(token) < 2:
            continue
        yield token

    korean_cleaned = remove_korean_stopwords_aggressively(lowered)
    for match in KOREAN_TOKEN_RE.finditer(korean_cleaned):
        token = match.group(0)
        if len(token) < 2:
            continue
        yield token


def iter_text_tokens(text: str):
    for segment in iter_tokenizable_segments(text):
        yield from iter_tokens_from_segment(segment)


def append_tokens_from_segment(tokens: list[str], segment: str) -> None:
    """Compatibility helper for callers that require a materialized token list."""
    tokens.extend(iter_tokens_from_segment(segment))


def tokenize_text(text: str) -> list[str]:
    return list(iter_text_tokens(text))


def compute_tag_recommendation_idf(total_docs: int, df: int) -> float:
    if total_docs <= 0:
        return 0.0
    idf_base = math.log((total_docs + 1) / (max(df, 0) + 1)) + 1
    return idf_base ** TAG_RECOMMEND_IDF_EXPONENT


def tag_recommend_token_sort_key(item: tuple[str, int]) -> tuple[int, int, str]:
    token, tf = item
    korean_priority = 0 if is_korean_token(token) else 1
    return (-int(tf), korean_priority, token)


def sorted_tf_items(tf_counter: Counter[str], limit: int | None = None) -> list[tuple[str, int]]:
    items = sorted(tf_counter.items(), key=tag_recommend_token_sort_key)
    if limit is not None:
        return items[:limit]
    return items


def limit_tf_counter(tf_counter: Counter[str], max_tokens: int) -> Counter[str]:
    if max_tokens <= 0 or len(tf_counter) <= max_tokens:
        return tf_counter
    return Counter(dict(sorted_tf_items(tf_counter, max_tokens)))


def choose_content_token_limit(tf_counter: Counter[str]) -> int:
    if not tf_counter:
        return TAG_RECOMMEND_MIN_TOKENS_PER_DOC

    ranked = sorted_tf_items(tf_counter, TAG_RECOMMEND_MAX_TOKENS_PER_DOC)

    max_index = TAG_RECOMMEND_ADAPTIVE_MAX_INDEX - 1
    if (
        len(ranked) > max_index
        and ranked[max_index][1] >= TAG_RECOMMEND_ADAPTIVE_MIN_TF
    ):
        return TAG_RECOMMEND_MAX_TOKENS_PER_DOC

    mid_index = TAG_RECOMMEND_ADAPTIVE_MID_INDEX - 1
    if (
        len(ranked) > mid_index
        and ranked[mid_index][1] >= TAG_RECOMMEND_ADAPTIVE_MIN_TF
    ):
        return TAG_RECOMMEND_MID_TOKENS_PER_DOC

    return TAG_RECOMMEND_MIN_TOKENS_PER_DOC


def limit_tf_counter_adaptive(tf_counter: Counter[str]) -> Counter[str]:
    if not tf_counter:
        return tf_counter

    limit = choose_content_token_limit(tf_counter)
    if limit <= 0 or len(tf_counter) <= limit:
        return tf_counter

    return Counter(dict(sorted_tf_items(tf_counter, limit)))


def compute_doc_token_counters(title: str, content: str) -> dict[str, Counter[str]]:
    title_counter = Counter(iter_text_tokens(title))
    content_counter = limit_tf_counter_adaptive(Counter(iter_text_tokens(content)))
    return {
        "title": title_counter,
        "content": content_counter,
    }


def ensure_language_token_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS language_doc_tokens (
            doc_id INTEGER NOT NULL,
            token TEXT NOT NULL,
            tf INTEGER NOT NULL,
            field TEXT NOT NULL DEFAULT 'content',
            PRIMARY KEY (doc_id, token, field)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_language_doc_tokens_token
        ON language_doc_tokens(token)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_language_doc_tokens_token_tf
        ON language_doc_tokens(token, tf DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_language_doc_tokens_token_doc
        ON language_doc_tokens(token, doc_id)
        """
    )
    # The primary key starts with doc_id, so this legacy secondary index only
    # adds B-tree work during every token update.
    conn.execute("DROP INDEX IF EXISTS idx_language_doc_tokens_doc_id")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS language_doc_norms (
            doc_id INTEGER PRIMARY KEY,
            norm REAL NOT NULL,
            token_count INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS language_token_stats (
            token TEXT PRIMARY KEY,
            df INTEGER NOT NULL,
            idf REAL NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS language_index_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """
    )


def _get_language_meta(conn: sqlite3.Connection, key: str) -> str | None:
    row = conn.execute(
        "SELECT value FROM language_index_meta WHERE key = ?",
        (key,),
    ).fetchone()
    return str(row["value"]) if row is not None else None


def _set_language_meta(conn: sqlite3.Connection, key: str, value: object) -> None:
    conn.execute(
        """
        INSERT INTO language_index_meta (key, value)
        VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        (key, str(value)),
    )


def _get_stored_language_total_docs(conn: sqlite3.Connection) -> int | None:
    value = _get_language_meta(conn, "total_docs")
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _get_main_doc_count(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT COUNT(*) AS c FROM docs").fetchone()
    return int(row["c"]) if row is not None else 0


def build_language_index_source_signature(conn: sqlite3.Connection) -> str:
    """Return the O(1) corpus revision, with a legacy-safe fallback.

    Older databases have no ``wiki_meta`` table.  Keeping the previous aggregate
    signature as a fallback makes the upgrade path safe; current databases use
    the revision bumped inside each document mutation transaction instead.
    """
    try:
        row = conn.execute(
            "SELECT value FROM wiki_meta WHERE key = 'corpus_revision'"
        ).fetchone()
    except sqlite3.OperationalError:
        row = None
    if row is not None:
        return f"corpus-v1|{row['value']}"

    legacy_row = conn.execute(
        """
        SELECT
            COUNT(*) AS docs_count,
            COALESCE(MAX(updated_at), '') AS docs_max_updated_at,
            COALESCE(SUM(id), 0) AS docs_id_sum,
            COALESCE(SUM(LENGTH(title)), 0) AS title_length_sum,
            COALESCE(SUM(LENGTH(slug)), 0) AS slug_length_sum
        FROM docs
        """
    ).fetchone()
    if legacy_row is None:
        return "docs-v1|0||||"
    return "|".join(
        (
            "docs-v1",
            str(int(legacy_row["docs_count"])),
            str(legacy_row["docs_max_updated_at"]),
            str(int(legacy_row["docs_id_sum"])),
            str(int(legacy_row["title_length_sum"])),
            str(int(legacy_row["slug_length_sum"])),
        )
    )


def _set_language_total_docs_and_version(conn: sqlite3.Connection, total_docs: int) -> None:
    _set_language_meta(conn, "total_docs", total_docs)
    _set_language_meta(conn, "tokenizer_version", TAG_RECOMMEND_TOKENIZER_VERSION)


def _set_language_idf_total_docs(conn: sqlite3.Connection, total_docs: int) -> None:
    _set_language_meta(conn, "idf_total_docs", total_docs)


def language_index_idf_is_current(conn: sqlite3.Connection, total_docs: int) -> bool:
    value = _get_language_meta(conn, "idf_total_docs")
    try:
        return value is not None and int(value) == total_docs
    except ValueError:
        return False


def _set_language_doc_norm_count(conn: sqlite3.Connection) -> None:
    row = conn.execute("SELECT COUNT(*) AS c FROM language_doc_norms").fetchone()
    count = int(row["c"]) if row is not None else 0
    _set_language_meta(conn, "doc_norm_count", count)


def _set_language_source_signature(token_conn: sqlite3.Connection, main_conn: sqlite3.Connection) -> None:
    _set_language_meta(
        token_conn,
        "source_signature",
        build_language_index_source_signature(main_conn),
    )


def finalize_language_token_batch(token_conn: sqlite3.Connection, main_conn: sqlite3.Connection) -> None:
    ensure_language_token_tables(token_conn)
    total_docs = _get_main_doc_count(main_conn)
    _recompute_language_token_idfs(token_conn, total_docs)
    _recompute_all_language_doc_norms(token_conn)
    _set_language_total_docs_and_version(token_conn, total_docs)
    _set_language_idf_total_docs(token_conn, total_docs)
    _set_language_source_signature(token_conn, main_conn)
    invalidate_tag_recommendation_cache()


def get_existing_doc_token_set(conn: sqlite3.Connection, doc_id: int) -> set[str]:
    rows = conn.execute(
        """
        SELECT DISTINCT token
        FROM language_doc_tokens
        WHERE doc_id = ?
        """,
        (doc_id,),
    ).fetchall()
    return {str(row["token"]) for row in rows}


def _doc_token_rows(doc_id: int, counters: dict[str, Counter[str]]) -> list[tuple[int, str, int, str]]:
    rows: list[tuple[int, str, int, str]] = []
    for field in ("title", "content"):
        for token, tf in counters.get(field, Counter()).items():
            if tf > 0:
                rows.append((doc_id, token, int(tf), field))
    return rows


def _doc_token_set(counters: dict[str, Counter[str]]) -> set[str]:
    tokens: set[str] = set()
    for counter in counters.values():
        tokens.update(counter.keys())
    return tokens


def _chunked(values: list[object], size: int = SQLITE_IN_CLAUSE_CHUNK_SIZE):
    for index in range(0, len(values), size):
        yield values[index : index + size]


def _increment_language_token_dfs(conn: sqlite3.Connection, tokens: set[str]) -> None:
    if not tokens:
        return
    conn.executemany(
        """
        INSERT INTO language_token_stats (token, df, idf)
        VALUES (?, 1, 0.0)
        ON CONFLICT(token) DO UPDATE SET df = df + 1
        """,
        [(token,) for token in sorted(tokens)],
    )


def _decrement_language_token_dfs(conn: sqlite3.Connection, tokens: set[str]) -> None:
    if not tokens:
        return
    conn.executemany(
        "UPDATE language_token_stats SET df = df - 1 WHERE token = ?",
        [(token,) for token in sorted(tokens)],
    )
    conn.execute("DELETE FROM language_token_stats WHERE df <= 0")


def _recompute_language_token_idfs(
    conn: sqlite3.Connection,
    total_docs: int,
    tokens: set[str] | None = None,
) -> None:
    def update_rows(rows: list[sqlite3.Row]) -> None:
        updates = [
            (compute_tag_recommendation_idf(total_docs, int(row["df"])), str(row["token"]))
            for row in rows
        ]
        if updates:
            conn.executemany(
                "UPDATE language_token_stats SET idf = ? WHERE token = ?",
                updates,
            )

    if tokens is None:
        cursor = conn.execute("SELECT token, df FROM language_token_stats")
        while rows := cursor.fetchmany(1000):
            update_rows(rows)
        return

    token_list = sorted(tokens)
    if not token_list:
        return
    for chunk in _chunked(token_list):
        placeholders = ",".join("?" for _ in chunk)
        rows = conn.execute(
            f"SELECT token, df FROM language_token_stats WHERE token IN ({placeholders})",
            list(chunk),
        ).fetchall()
        update_rows(rows)


def _get_doc_ids_for_tokens(conn: sqlite3.Connection, tokens: set[str]) -> set[int]:
    if not tokens:
        return set()
    doc_ids: set[int] = set()
    for chunk in _chunked(sorted(tokens)):
        placeholders = ",".join("?" for _ in chunk)
        rows = conn.execute(
            f"""
            SELECT DISTINCT doc_id
            FROM language_doc_tokens
            WHERE token IN ({placeholders})
            """,
            list(chunk),
        ).fetchall()
        doc_ids.update(int(row["doc_id"]) for row in rows)
    return doc_ids


def _recompute_language_doc_norms_for_docs(
    conn: sqlite3.Connection,
    doc_ids: set[int] | list[int],
) -> None:
    unique_doc_ids = sorted(set(int(doc_id) for doc_id in doc_ids))
    if not unique_doc_ids:
        return

    seen_with_tokens: set[int] = set()
    norm_rows: list[tuple[int, float, int]] = []
    for chunk in _chunked(unique_doc_ids):
        placeholders = ",".join("?" for _ in chunk)
        rows = conn.execute(
            f"""
            SELECT
                weighted.doc_id,
                SUM(weighted.value * weighted.value) AS norm_sq,
                COUNT(*) AS token_count
            FROM (
                SELECT
                    l.doc_id,
                    l.token,
                    SUM(l.tf) * s.idf AS value
                FROM language_doc_tokens l
                JOIN language_token_stats s ON s.token = l.token
                WHERE l.doc_id IN ({placeholders})
                GROUP BY l.doc_id, l.token
            ) weighted
            GROUP BY weighted.doc_id
            """,
            list(chunk),
        ).fetchall()
        for row in rows:
            doc_id = int(row["doc_id"])
            seen_with_tokens.add(doc_id)
            norm_sq = float(row["norm_sq"] or 0.0)
            token_count = int(row["token_count"] or 0)
            norm_rows.append((doc_id, math.sqrt(norm_sq), token_count))

    conn.executemany(
        """
        INSERT INTO language_doc_norms (doc_id, norm, token_count)
        VALUES (?, ?, ?)
        ON CONFLICT(doc_id) DO UPDATE SET
            norm = excluded.norm,
            token_count = excluded.token_count
        """,
        norm_rows,
    )
    missing_doc_ids = set(unique_doc_ids) - seen_with_tokens
    if missing_doc_ids:
        for chunk in _chunked(sorted(missing_doc_ids)):
            placeholders = ",".join("?" for _ in chunk)
            conn.execute(
                f"DELETE FROM language_doc_norms WHERE doc_id IN ({placeholders})",
                list(chunk),
            )
    _set_language_doc_norm_count(conn)


def _recompute_all_language_doc_norms(conn: sqlite3.Connection) -> None:
    rows = conn.execute("SELECT DISTINCT doc_id FROM language_doc_tokens ORDER BY doc_id").fetchall()
    doc_ids = [int(row["doc_id"]) for row in rows]
    conn.execute("DELETE FROM language_doc_norms")
    _recompute_language_doc_norms_for_docs(conn, doc_ids)
    _set_language_doc_norm_count(conn)


def language_doc_norms_need_refresh(conn: sqlite3.Connection, *, thorough: bool = False) -> bool:
    ensure_language_token_tables(conn)
    norm_row = conn.execute("SELECT COUNT(*) AS c FROM language_doc_norms").fetchone()
    norm_docs = int(norm_row["c"]) if norm_row is not None else 0
    stored_norm_count = _get_language_meta(conn, "doc_norm_count")
    try:
        if stored_norm_count is None or int(stored_norm_count) != norm_docs:
            return True
    except ValueError:
        return True
    if not thorough:
        return False

    row = conn.execute(
        """
        SELECT
            (SELECT COUNT(DISTINCT doc_id) FROM language_doc_tokens) AS token_docs,
            (SELECT COUNT(*) FROM language_doc_norms) AS norm_docs
        """
    ).fetchone()
    if row is None:
        return False
    return int(row["token_docs"] or 0) != int(row["norm_docs"] or 0)


def upsert_language_doc_tokens(
    token_conn: sqlite3.Connection,
    main_conn: sqlite3.Connection,
    doc_id: int,
    title: str,
    content: str,
    *,
    refresh_idf: bool = True,
) -> None:
    old_token_set = get_existing_doc_token_set(token_conn, doc_id)
    counters = compute_doc_token_counters(title, content)
    new_token_set = _doc_token_set(counters)
    total_docs = _get_main_doc_count(main_conn)
    stored_total_docs = _get_stored_language_total_docs(token_conn)

    removed_tokens = old_token_set - new_token_set
    added_tokens = new_token_set - old_token_set
    _decrement_language_token_dfs(token_conn, removed_tokens)
    _increment_language_token_dfs(token_conn, added_tokens)

    token_conn.execute("DELETE FROM language_doc_tokens WHERE doc_id = ?", (doc_id,))
    rows = _doc_token_rows(doc_id, counters)
    if rows:
        token_conn.executemany(
            """
            INSERT INTO language_doc_tokens (doc_id, token, tf, field)
            VALUES (?, ?, ?, ?)
            """,
            rows,
        )

    if refresh_idf:
        if stored_total_docs != total_docs:
            # A document-count change affects every IDF.  Rebuilding every
            # document norm here turns ordinary create/delete operations into
            # O(corpus) writes.  Keep the persisted index structurally current
            # and let the bounded candidate scorer use current IDFs lazily until
            # the next explicit batch finalization.
            _recompute_language_doc_norms_for_docs(token_conn, {doc_id})
        else:
            affected_tokens = removed_tokens | added_tokens
            if language_index_idf_is_current(token_conn, total_docs):
                _recompute_language_token_idfs(token_conn, total_docs, affected_tokens)
                affected_doc_ids = _get_doc_ids_for_tokens(token_conn, affected_tokens)
                affected_doc_ids.add(doc_id)
                _recompute_language_doc_norms_for_docs(token_conn, affected_doc_ids)
            else:
                _recompute_language_doc_norms_for_docs(token_conn, {doc_id})
        _set_language_total_docs_and_version(token_conn, total_docs)
        _set_language_source_signature(token_conn, main_conn)
        invalidate_tag_recommendation_cache()


def delete_language_doc_tokens(
    token_conn: sqlite3.Connection,
    main_conn: sqlite3.Connection,
    doc_id: int,
    *,
    refresh_idf: bool = True,
) -> None:
    old_token_set = get_existing_doc_token_set(token_conn, doc_id)
    stored_total_docs = _get_stored_language_total_docs(token_conn)
    total_docs = _get_main_doc_count(main_conn)

    _decrement_language_token_dfs(token_conn, old_token_set)
    token_conn.execute("DELETE FROM language_doc_tokens WHERE doc_id = ?", (doc_id,))
    if refresh_idf:
        if stored_total_docs != total_docs:
            _recompute_language_doc_norms_for_docs(token_conn, {doc_id})
        else:
            if language_index_idf_is_current(token_conn, total_docs):
                _recompute_language_token_idfs(token_conn, total_docs, old_token_set)
                affected_doc_ids = _get_doc_ids_for_tokens(token_conn, old_token_set)
                affected_doc_ids.add(doc_id)
                _recompute_language_doc_norms_for_docs(token_conn, affected_doc_ids)
            else:
                _recompute_language_doc_norms_for_docs(token_conn, {doc_id})
        _set_language_total_docs_and_version(token_conn, total_docs)
        _set_language_source_signature(token_conn, main_conn)
        invalidate_tag_recommendation_cache()


def rebuild_language_token_index(
    token_conn: sqlite3.Connection,
    main_conn: sqlite3.Connection,
    fts_conn: sqlite3.Connection,
) -> tuple[int, int]:
    ensure_language_token_tables(token_conn)
    token_conn.execute("DELETE FROM language_doc_tokens")
    token_conn.execute("DELETE FROM language_token_stats")

    doc_rows = main_conn.execute("SELECT id, title FROM docs ORDER BY id").fetchall()
    total_docs = len(doc_rows)
    pending_rows: list[tuple[int, str, int, str]] = []

    def flush_pending_rows() -> None:
        nonlocal pending_rows
        if not pending_rows:
            return
        token_conn.executemany(
            """
            INSERT INTO language_doc_tokens (doc_id, token, tf, field)
            VALUES (?, ?, ?, ?)
            """,
            pending_rows,
        )
        pending_rows = []

    for chunk in _chunked(doc_rows):
        doc_ids = [int(row["id"]) for row in chunk]
        placeholders = ",".join("?" for _ in doc_ids)
        content_by_id: dict[int, str] = {}
        if doc_ids:
            for fts_row in fts_conn.execute(
                f"SELECT rowid AS doc_id, content FROM docs_fts WHERE rowid IN ({placeholders})",
                doc_ids,
            ):
                content_by_id[int(fts_row["doc_id"])] = str(fts_row["content"] or "")

        for row in chunk:
            doc_id = int(row["id"])
            counters = compute_doc_token_counters(
                str(row["title"] or ""),
                content_by_id.get(doc_id, ""),
            )
            pending_rows.extend(_doc_token_rows(doc_id, counters))
            if len(pending_rows) >= 5000:
                flush_pending_rows()
    flush_pending_rows()

    # Let SQLite aggregate document frequency from the persisted rows instead
    # of retaining the complete vocabulary in Python during large rebuilds.
    token_conn.execute(
        """
        INSERT INTO language_token_stats (token, df, idf)
        SELECT token, COUNT(DISTINCT doc_id), 0.0
        FROM language_doc_tokens
        GROUP BY token
        """
    )
    _recompute_language_token_idfs(token_conn, total_docs)
    _recompute_all_language_doc_norms(token_conn)
    _set_language_total_docs_and_version(token_conn, total_docs)
    _set_language_idf_total_docs(token_conn, total_docs)
    _set_language_source_signature(token_conn, main_conn)
    _set_language_meta(token_conn, "last_rebuild_at", datetime.now().isoformat(timespec="seconds"))
    invalidate_tag_recommendation_cache()
    token_count_row = token_conn.execute("SELECT COUNT(*) AS c FROM language_token_stats").fetchone()
    token_count = int(token_count_row["c"]) if token_count_row is not None else 0
    return total_docs, token_count


def language_token_index_needs_rebuild(
    token_conn: sqlite3.Connection,
    main_conn: sqlite3.Connection,
) -> bool:
    ensure_language_token_tables(token_conn)
    total_docs = _get_main_doc_count(main_conn)
    tokenizer_version = _get_language_meta(token_conn, "tokenizer_version")
    stored_total_docs = _get_stored_language_total_docs(token_conn)
    stored_source_signature = _get_language_meta(token_conn, "source_signature")
    current_source_signature = build_language_index_source_signature(main_conn)
    if tokenizer_version != TAG_RECOMMEND_TOKENIZER_VERSION:
        return True
    if stored_total_docs != total_docs:
        return True
    if stored_source_signature != current_source_signature:
        return True
    return False


def ensure_language_token_index_current(
    token_conn: sqlite3.Connection,
    main_conn: sqlite3.Connection,
    fts_conn: sqlite3.Connection,
) -> tuple[bool, int, int]:
    if language_token_index_needs_rebuild(token_conn, main_conn):
        total_docs, token_count = rebuild_language_token_index(token_conn, main_conn, fts_conn)
        return True, total_docs, token_count
    norm_refreshed = False
    if language_doc_norms_need_refresh(token_conn):
        _recompute_all_language_doc_norms(token_conn)
        norm_refreshed = True
    total_docs = _get_main_doc_count(main_conn)
    row = token_conn.execute("SELECT COUNT(*) AS c FROM language_token_stats").fetchone()
    token_count = int(row["c"]) if row is not None else 0
    return norm_refreshed, total_docs, token_count


def get_language_token_stats_for_tokens(
    conn: sqlite3.Connection,
    tokens: list[str],
    *,
    total_docs: int | None = None,
) -> tuple[dict[str, int], dict[str, float]]:
    unique_tokens = sorted(set(tokens))
    if not unique_tokens:
        return {}, {}

    df_by_token: dict[str, int] = {}
    idf_by_token: dict[str, float] = {}
    for chunk in _chunked(unique_tokens):
        placeholders = ",".join("?" for _ in chunk)
        rows = conn.execute(
            f"""
            SELECT token, df, idf
            FROM language_token_stats
            WHERE token IN ({placeholders})
            """,
            list(chunk),
        ).fetchall()
        for row in rows:
            token = str(row["token"])
            df = int(row["df"])
            df_by_token[token] = df
            idf_by_token[token] = (
                compute_tag_recommendation_idf(total_docs, df)
                if total_docs is not None
                else float(row["idf"])
            )
    return df_by_token, idf_by_token


def build_tfidf_vector_from_idf(
    tf_counter: Counter[str],
    idf_by_token: dict[str, float],
) -> dict[str, float]:
    vec: dict[str, float] = {}
    for token, tf in tf_counter.items():
        if tf <= 0:
            continue
        idf = idf_by_token.get(token)
        if idf is None:
            continue
        vec[token] = float(tf) * idf
    return vec


def limit_recommendation_query_counter(
    query_counter: Counter[str],
    idf_by_token: dict[str, float],
) -> Counter[str]:
    weighted_items = [
        (token, int(tf), float(idf_by_token.get(token, 0.0)))
        for token, tf in query_counter.items()
        if tf > 0 and idf_by_token.get(token, 0.0) > 0
    ]
    weighted_items.sort(
        key=lambda item: (
            -(item[1] * item[2]),
            -item[2],
            -item[1],
            item[0],
        )
    )
    return Counter(
        {
            token: tf
            for token, tf, _idf in weighted_items[:TAG_RECOMMEND_QUERY_VECTOR_MAX_TOKENS]
        }
    )


def vector_norm(vec: dict[str, float]) -> float:
    return math.sqrt(sum(v * v for v in vec.values()))


def tag_recommend_rank_weight(rank: int) -> float:
    base = TAG_RECOMMEND_SIMILAR_DOC_LIMIT + 1 - rank
    if base <= 0:
        return 0.0
    return float(base) ** TAG_RECOMMEND_RANK_WEIGHT_EXPONENT


def escape_fts5_phrase_token(token: str) -> str:
    return '"' + token.replace('"', '""') + '"'


def select_tag_recommendation_fts_query_tokens(
    query_counter: Counter[str],
    df_by_token: dict[str, int] | Counter[str],
    total_docs: int,
) -> list[str]:
    first_positions = {token: index for index, token in enumerate(query_counter)}

    def token_idf(token: str) -> float:
        df = df_by_token.get(token, 0)
        return math.log((total_docs + 1) / (df + 1)) + 1

    ranked_tokens = sorted(
        first_positions.keys(),
        key=lambda token: (-token_idf(token), -query_counter[token], first_positions[token]),
    )
    return ranked_tokens[:TAG_RECOMMEND_FTS_MAX_QUERY_TERMS]


def find_tag_recommendation_candidate_doc_ids(
    fts_conn: sqlite3.Connection,
    query_counter: Counter[str],
    df_by_token: dict[str, int] | Counter[str],
    total_docs: int,
    *,
    limit: int = TAG_RECOMMEND_FTS_CANDIDATE_LIMIT,
    selected_tokens: list[str] | None = None,
) -> set[int]:
    if not query_counter or total_docs <= 0 or limit <= 0:
        return set()

    if selected_tokens is None:
        selected_tokens = select_tag_recommendation_fts_query_tokens(
            query_counter,
            df_by_token,
            total_docs,
        )
    if not selected_tokens:
        return set()

    match_query = " OR ".join(escape_fts5_phrase_token(token) for token in selected_tokens)
    try:
        rows = fts_conn.execute(
            """
            SELECT rowid AS doc_id
            FROM docs_fts
            WHERE docs_fts MATCH ?
            ORDER BY bm25(docs_fts)
            LIMIT ?
            """,
            (match_query, limit),
        ).fetchall()
    except sqlite3.OperationalError:
        return set()

    return {int(row["doc_id"]) for row in rows}


def select_tag_recommendation_language_query_tokens(
    query_counter: Counter[str],
    df_by_token: dict[str, int],
    idf_by_token: dict[str, float],
    total_docs: int,
) -> list[str]:
    if total_docs <= 0:
        return []
    first_positions = {token: index for index, token in enumerate(query_counter)}

    usable_tokens = [
        token
        for token in first_positions.keys()
        if df_by_token.get(token, 0) > 0
        and (df_by_token[token] / max(total_docs, 1)) <= TAG_RECOMMEND_TOKEN_DB_MAX_DF_RATIO
    ]
    usable_tokens.sort(
        key=lambda token: (
            -idf_by_token.get(token, 0.0),
            -query_counter[token],
            first_positions[token],
        )
    )
    return usable_tokens[:TAG_RECOMMEND_TOKEN_DB_MAX_QUERY_TERMS]


def find_tag_recommendation_language_candidate_doc_ids(
    conn: sqlite3.Connection,
    query_counter: Counter[str],
    total_docs: int,
    *,
    limit: int = TAG_RECOMMEND_TOKEN_DB_CANDIDATE_LIMIT,
    df_by_token: dict[str, int] | None = None,
    idf_by_token: dict[str, float] | None = None,
    selected_tokens: list[str] | None = None,
) -> set[int]:
    if not query_counter or total_docs <= 0 or limit <= 0:
        return set()

    if df_by_token is None or idf_by_token is None:
        df_by_token, idf_by_token = get_language_token_stats_for_tokens(
            conn,
            list(query_counter),
            total_docs=total_docs,
        )
    if selected_tokens is None:
        selected_tokens = select_tag_recommendation_language_query_tokens(
            query_counter,
            df_by_token,
            idf_by_token,
            total_docs,
        )
    if not selected_tokens:
        return set()

    values_sql = ",".join("(?, ?)" for _ in selected_tokens)
    params: list[object] = []
    for token in selected_tokens:
        params.extend((token, float(idf_by_token.get(token, 0.0))))
    rows = conn.execute(
        f"""
        WITH query_terms(token, idf) AS (
            VALUES {values_sql}
        )
        SELECT
            l.doc_id,
            SUM(l.tf * q.idf) AS token_score,
            COUNT(DISTINCT l.token) AS overlap_count
        FROM language_doc_tokens l
        JOIN query_terms q ON q.token = l.token
        GROUP BY l.doc_id
        ORDER BY token_score DESC, overlap_count DESC
        LIMIT ?
        """,
        [*params, limit],
    ).fetchall()
    return {int(row["doc_id"]) for row in rows}


def _fetch_recommendation_doc_rows(
    conn: sqlite3.Connection,
    *,
    doc_ids: set[int] | None,
    current_slug: str | None,
) -> list[sqlite3.Row]:
    params: list[object] = []
    where_parts: list[str] = []
    if current_slug:
        where_parts.append("slug != ?")
        params.append(current_slug)

    if doc_ids is None:
        where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
        return conn.execute(
            f"SELECT id, slug FROM docs {where_sql} ORDER BY id",
            params,
        ).fetchall()

    if not doc_ids:
        return []

    rows: list[sqlite3.Row] = []
    for chunk in _chunked(sorted(doc_ids)):
        chunk_where = list(where_parts)
        placeholders = ",".join("?" for _ in chunk)
        chunk_where.append(f"id IN ({placeholders})")
        where_sql = " AND ".join(chunk_where)
        rows.extend(
            conn.execute(
                f"SELECT id, slug FROM docs WHERE {where_sql} ORDER BY id",
                [*params, *chunk],
            ).fetchall()
        )
    return rows


def score_recommendation_doc_ids_from_language_tokens(
    conn: sqlite3.Connection,
    *,
    query_vec: dict[str, float],
    query_norm: float,
    doc_ids: list[int],
    limit: int = TAG_RECOMMEND_SCORE_DOC_LIMIT,
) -> list[tuple[int, float]]:
    if not query_vec or query_norm <= 0 or not doc_ids or limit <= 0:
        return []

    query_items = sorted(query_vec.items(), key=lambda item: (-item[1], item[0]))
    values_sql = ",".join("(?, ?)" for _token, _weight in query_items)
    query_params: list[object] = []
    for token, weight in query_items:
        query_params.extend((token, float(weight)))

    scored: dict[int, float] = {}
    for chunk in _chunked(sorted(set(int(doc_id) for doc_id in doc_ids))):
        placeholders = ",".join("?" for _ in chunk)
        rows = conn.execute(
            f"""
            WITH query_terms(token, weight) AS (
                VALUES {values_sql}
            )
            SELECT
                l.doc_id,
                SUM(l.tf * s.idf * q.weight) AS dot,
                n.norm AS doc_norm
            FROM language_doc_tokens l
            JOIN query_terms q ON q.token = l.token
            JOIN language_token_stats s ON s.token = l.token
            JOIN language_doc_norms n ON n.doc_id = l.doc_id
            WHERE l.doc_id IN ({placeholders})
            GROUP BY l.doc_id, n.norm
            HAVING dot > 0 AND n.norm > 0
            ORDER BY (dot / n.norm) DESC
            LIMIT ?
            """,
            [*query_params, *chunk, limit],
        ).fetchall()
        for row in rows:
            doc_id = int(row["doc_id"])
            dot = float(row["dot"] or 0.0)
            doc_norm = float(row["doc_norm"] or 0.0)
            if dot <= 0 or doc_norm <= 0:
                continue
            similarity = dot / (query_norm * doc_norm)
            if similarity > scored.get(doc_id, 0.0):
                scored[doc_id] = similarity

    return sorted(scored.items(), key=lambda item: item[1], reverse=True)[:limit]


def score_recommendation_doc_ids_with_current_idf(
    conn: sqlite3.Connection,
    *,
    query_vec: dict[str, float],
    query_norm: float,
    doc_ids: list[int],
    total_docs: int,
    limit: int = TAG_RECOMMEND_SCORE_DOC_LIMIT,
) -> list[tuple[int, float]]:
    """Score a bounded candidate set without rewriting global document norms.

    Document-count changes alter every IDF.  Persisted norms are intentionally
    left stale until a batch maintenance pass; calculating norms for at most the
    candidate set keeps single-document saves fast while preserving cosine-score
    correctness for the recommendations returned by this request.
    """
    if not query_vec or query_norm <= 0 or not doc_ids or total_docs <= 0 or limit <= 0:
        return []

    dots: defaultdict[int, float] = defaultdict(float)
    norm_squares: defaultdict[int, float] = defaultdict(float)
    for chunk in _chunked(sorted(set(int(doc_id) for doc_id in doc_ids))):
        placeholders = ",".join("?" for _ in chunk)
        rows = conn.execute(
            f"""
            SELECT l.doc_id, l.token, SUM(l.tf) AS tf, s.df
            FROM language_doc_tokens l
            JOIN language_token_stats s ON s.token = l.token
            WHERE l.doc_id IN ({placeholders})
            GROUP BY l.doc_id, l.token, s.df
            """,
            list(chunk),
        ).fetchall()
        for row in rows:
            doc_id = int(row["doc_id"])
            token = str(row["token"])
            tf = int(row["tf"] or 0)
            if tf <= 0:
                continue
            weight = float(tf) * compute_tag_recommendation_idf(total_docs, int(row["df"]))
            norm_squares[doc_id] += weight * weight
            query_weight = query_vec.get(token)
            if query_weight is not None:
                dots[doc_id] += weight * query_weight

    scored: list[tuple[int, float]] = []
    for doc_id, dot in dots.items():
        norm_sq = norm_squares.get(doc_id, 0.0)
        if dot <= 0 or norm_sq <= 0:
            continue
        similarity = dot / (query_norm * math.sqrt(norm_sq))
        if similarity > 0:
            scored.append((doc_id, similarity))
    scored.sort(key=lambda item: (-item[1], item[0]))
    return scored[:limit]


def build_recommendation_query_counter(title: str, content: str) -> Counter[str]:
    counters = compute_doc_token_counters(title, content)
    combined: Counter[str] = Counter()
    for counter in counters.values():
        combined.update(counter)
    # Keep term frequencies in the Counter.  Expanding ``[token] * tf`` made a
    # repetitive long document allocate an unbounded list solely for ranking.
    return Counter(dict(sorted_tf_items(combined)))


def _log_tag_recommendation_debug(**values: object) -> None:
    if not TAG_RECOMMEND_DEBUG:
        return
    details = " ".join(f"{key}={value}" for key, value in values.items())
    print(f"[TAG_RECOMMEND] {details}")


def recommend_tags(
    conn: sqlite3.Connection,
    fts_conn: sqlite3.Connection,
    token_conn: sqlite3.Connection | None = None,
    *,
    title: str,
    content: str,
    current_slug: str | None = None,
    exclude_tags: list[str] | None = None,
    limit: int = TAG_RECOMMEND_LIMIT,
) -> list[str]:
    if limit <= 0:
        return []

    cache_key = _tag_recommendation_cache_key(
        title=title,
        content=content,
        current_slug=current_slug,
        exclude_tags=exclude_tags,
        limit=limit,
    )
    cache_generation, cached = _get_cached_tag_recommendations(cache_key)
    if cached is not None:
        return cached

    def finish(recommendations: list[str]) -> list[str]:
        _cache_tag_recommendations(cache_key, cache_generation, recommendations)
        return recommendations

    raw_query_counter = build_recommendation_query_counter(title, content)
    if not raw_query_counter:
        return finish([])

    token_conn = token_conn or conn
    total_docs = _get_main_doc_count(conn)
    if total_docs <= 0:
        return finish([])

    # Recommendations must be read-only.  Startup and mutation transactions own
    # index repair; a GET/API call must never start an uncommitted global rebuild.
    df_by_token, idf_by_token = get_language_token_stats_for_tokens(
        token_conn,
        list(raw_query_counter),
        total_docs=total_docs,
    )
    query_counter = limit_recommendation_query_counter(raw_query_counter, idf_by_token)
    query_vec = build_tfidf_vector_from_idf(query_counter, idf_by_token)
    if not query_vec:
        return finish([])
    query_norm = vector_norm(query_vec)
    if query_norm == 0:
        return finish([])

    candidate_query_counter = Counter(
        {
            token: tf
            for token, tf in query_counter.items()
            if token in query_vec and df_by_token.get(token, 0) > 0
        }
    )
    selected_fts_tokens = select_tag_recommendation_fts_query_tokens(
        candidate_query_counter,
        df_by_token,
        total_docs,
    )
    fts_candidate_doc_ids = find_tag_recommendation_candidate_doc_ids(
        fts_conn,
        candidate_query_counter,
        df_by_token,
        total_docs,
        selected_tokens=selected_fts_tokens,
    )
    selected_token_db_tokens = select_tag_recommendation_language_query_tokens(
        candidate_query_counter,
        df_by_token,
        idf_by_token,
        total_docs,
    )
    token_candidate_doc_ids = find_tag_recommendation_language_candidate_doc_ids(
        token_conn,
        candidate_query_counter,
        total_docs,
        df_by_token=df_by_token,
        idf_by_token=idf_by_token,
        selected_tokens=selected_token_db_tokens,
    )
    candidate_doc_ids = fts_candidate_doc_ids | token_candidate_doc_ids
    fallback_requested = len(candidate_doc_ids) < TAG_RECOMMEND_MIN_CANDIDATE_FALLBACK
    fallback_used = fallback_requested and total_docs <= TAG_RECOMMEND_FULL_SCAN_MAX_DOCS

    doc_rows = _fetch_recommendation_doc_rows(
        conn,
        doc_ids=None if fallback_used else candidate_doc_ids,
        current_slug=current_slug,
    )
    if not doc_rows:
        _log_tag_recommendation_debug(
            query_tokens=sum(raw_query_counter.values()),
            selected_fts_terms=len(selected_fts_tokens),
            selected_token_db_terms=len(selected_token_db_tokens),
            fts_candidates=len(fts_candidate_doc_ids),
            token_db_candidates=len(token_candidate_doc_ids),
            merged_candidates=len(candidate_doc_ids),
            fallback_used=fallback_used,
            fallback_skipped=fallback_requested and not fallback_used,
            scored_docs=0,
            final_tags=0,
        )
        return finish([])

    doc_ids = [int(row["id"]) for row in doc_rows]
    tag_map = _build_doc_tag_map(conn, doc_ids)
    score_limit = min(
        max(TAG_RECOMMEND_SIMILAR_DOC_LIMIT * 10, limit * 10),
        TAG_RECOMMEND_SCORE_DOC_LIMIT,
        len(doc_ids),
    )

    scored_docs: list[tuple[float, list[str]]] = []
    if language_index_idf_is_current(token_conn, total_docs):
        scored_doc_ids = score_recommendation_doc_ids_from_language_tokens(
            token_conn,
            query_vec=query_vec,
            query_norm=query_norm,
            doc_ids=doc_ids,
            limit=score_limit,
        )
    else:
        scored_doc_ids = score_recommendation_doc_ids_with_current_idf(
            token_conn,
            query_vec=query_vec,
            query_norm=query_norm,
            doc_ids=doc_ids,
            total_docs=total_docs,
            limit=score_limit,
        )
    for doc_id, similarity in scored_doc_ids:
        tags = tag_map.get(doc_id, [])
        if not tags:
            continue
        if similarity > 0:
            scored_docs.append((similarity, tags))
    if not scored_docs:
        _log_tag_recommendation_debug(
            query_tokens=sum(raw_query_counter.values()),
            query_vector_terms=len(query_vec),
            selected_fts_terms=len(selected_fts_tokens),
            selected_token_db_terms=len(selected_token_db_tokens),
            fts_candidates=len(fts_candidate_doc_ids),
            token_db_candidates=len(token_candidate_doc_ids),
            merged_candidates=len(candidate_doc_ids),
            fallback_used=fallback_used,
            fallback_skipped=fallback_requested and not fallback_used,
            scored_docs=0,
            final_tags=0,
        )
        return finish([])

    scored_docs.sort(key=lambda item: item[0], reverse=True)
    similar_docs = scored_docs[:TAG_RECOMMEND_SIMILAR_DOC_LIMIT]

    excluded = {tag.casefold() for tag in (exclude_tags or [])}
    tag_counts: Counter[str] = Counter()
    tag_scores: defaultdict[str, float] = defaultdict(float)
    display_names: dict[str, str] = {}

    for index, (similarity, tags) in enumerate(similar_docs):
        rank = index + 1
        score = similarity * tag_recommend_rank_weight(rank)
        seen_in_doc: set[str] = set()
        for tag in tags:
            key = tag.casefold()
            if key in excluded or key in seen_in_doc:
                continue
            seen_in_doc.add(key)
            display_names.setdefault(key, tag)
            tag_counts[key] += 1
            tag_scores[key] += score

    ordered = sorted(
        tag_counts.keys(),
        key=lambda key: (-tag_scores[key], -tag_counts[key], display_names[key].casefold()),
    )
    recommendations = [display_names[key] for key in ordered[:limit]]
    _log_tag_recommendation_debug(
        query_tokens=sum(raw_query_counter.values()),
        query_vector_terms=len(query_vec),
        selected_fts_terms=len(selected_fts_tokens),
        selected_token_db_terms=len(selected_token_db_tokens),
        fts_candidates=len(fts_candidate_doc_ids),
        token_db_candidates=len(token_candidate_doc_ids),
        merged_candidates=len(candidate_doc_ids),
        fallback_used=fallback_used,
        fallback_skipped=fallback_requested and not fallback_used,
        scored_docs=len(scored_docs),
        final_tags=len(recommendations),
    )
    return finish(recommendations)
