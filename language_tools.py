from __future__ import annotations

import math
import re
import sqlite3
import threading
from collections import Counter, defaultdict, deque
from functools import lru_cache

ENGLISH_TOKEN_RE = re.compile(r"(?<![A-Za-z0-9])[A-Za-z0-9]{2,}(?![A-Za-z0-9])")
KOREAN_TOKEN_RE = re.compile(r"[가-힣]{2,}")
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


_TAG_RECOMMEND_CACHE_LOCK = threading.Lock()
_TAG_RECOMMEND_CACHE: dict[str, object] = {
    "signature": None,
    "corpus": [],
    "entries_by_id": {},
    "doc_states": {},
    "df_counter": Counter(),
    "total_docs": 0,
    "content_cutoff": None,
}
TAG_RECOMMEND_IDF_EXPONENT = 2.0
TAG_RECOMMEND_SIMILAR_DOC_LIMIT = 30
TAG_RECOMMEND_LIMIT = 25
TAG_RECOMMEND_RANK_WEIGHT_EXPONENT = 1.7
TAG_RECOMMEND_FTS_CANDIDATE_LIMIT = 200
TAG_RECOMMEND_FTS_MAX_QUERY_TERMS = 30
TAG_RECOMMEND_MIN_CANDIDATE_FALLBACK = 30
SQLITE_IN_CLAUSE_CHUNK_SIZE = 400
TAG_RECOMMEND_CUTOFF_START_DOCS = 100
TAG_RECOMMEND_CUTOFF_FULL_BUDGET_DOCS = 6_000
TAG_RECOMMEND_CUTOFF_STEP = 100
TAG_RECOMMEND_CUTOFF_START_BUDGET = 120_000_000
TAG_RECOMMEND_CUTOFF_FLOOR_BUDGET = 60_000_000


def _tag_recommend_budget_for_doc_count(doc_count: int) -> int:
    if doc_count <= TAG_RECOMMEND_CUTOFF_START_DOCS:
        return TAG_RECOMMEND_CUTOFF_START_BUDGET
    if doc_count >= TAG_RECOMMEND_CUTOFF_FULL_BUDGET_DOCS:
        return TAG_RECOMMEND_CUTOFF_FLOOR_BUDGET

    span = TAG_RECOMMEND_CUTOFF_FULL_BUDGET_DOCS - TAG_RECOMMEND_CUTOFF_START_DOCS
    used = doc_count - TAG_RECOMMEND_CUTOFF_START_DOCS
    budget_drop = TAG_RECOMMEND_CUTOFF_START_BUDGET - TAG_RECOMMEND_CUTOFF_FLOOR_BUDGET
    return TAG_RECOMMEND_CUTOFF_START_BUDGET - ((budget_drop * used) // span)


def _tag_recommend_cutoff_for_doc_count(doc_count: int) -> int:
    return max(1, _tag_recommend_budget_for_doc_count(doc_count) // max(doc_count, 1))


TAG_RECOMMEND_CORPUS_CONTENT_CUTOFFS: tuple[tuple[int, int], ...] = (
    (1, _tag_recommend_cutoff_for_doc_count(100)),
    *(
        (_doc_count, _tag_recommend_cutoff_for_doc_count(_doc_count))
        for _doc_count in range(
            TAG_RECOMMEND_CUTOFF_START_DOCS,
            TAG_RECOMMEND_CUTOFF_FULL_BUDGET_DOCS,
            TAG_RECOMMEND_CUTOFF_STEP,
        )
    ),
    (6_000, 10_000),
    (8_000, 7_500),
    (10_000, 6_000),
    (15_000, 4_000),
    (20_000, 3_000),
    (25_000, 2_400),
    (30_000, 2_000),
    (40_000, 1_500),
    (50_000, 1_200),
    (60_000, 1_000),
    (80_000, 750),
    (100_000, 600),
    (150_000, 400),
    (200_000, 300),
)

KOREAN_SPELL_REPLACE_DB: tuple[tuple[str, str], ...] = (
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
)
KOREAN_SPELL_SAMPLE_LIMIT = 8


def invalidate_tag_recommendation_cache() -> None:
    with _TAG_RECOMMEND_CACHE_LOCK:
        _TAG_RECOMMEND_CACHE["signature"] = None
        _TAG_RECOMMEND_CACHE["corpus"] = []
        _TAG_RECOMMEND_CACHE["entries_by_id"] = {}
        _TAG_RECOMMEND_CACHE["doc_states"] = {}
        _TAG_RECOMMEND_CACHE["df_counter"] = Counter()
        _TAG_RECOMMEND_CACHE["total_docs"] = 0
        _TAG_RECOMMEND_CACHE["content_cutoff"] = None


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

    placeholders = ",".join("?" for _ in unique_doc_ids)
    rows = conn.execute(
        f"""
        SELECT dt.doc_id, t.name
        FROM doc_tags dt
        JOIN tags t ON t.id = dt.tag_id
        WHERE dt.doc_id IN ({placeholders})
        ORDER BY dt.doc_id, t.name COLLATE NOCASE
        """,
        unique_doc_ids,
    ).fetchall()

    mapping: dict[int, list[str]] = defaultdict(list)
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
    for stopword in KOREAN_STOPWORDS_LONGEST_FIRST:
        text = text.replace(stopword, "")
    return text


def tokenize_text(text: str) -> list[str]:
    tokens: list[str] = []
    lowered = text.lower()

    for raw in ENGLISH_TOKEN_RE.findall(lowered):
        token = singularize_token(raw)
        if token in ENGLISH_STOPWORDS:
            continue
        if len(token) < 2:
            continue
        tokens.append(token)

    korean_cleaned = remove_korean_stopwords_aggressively(lowered)
    for token in KOREAN_TOKEN_RE.findall(korean_cleaned):
        if len(token) < 2:
            continue
        tokens.append(token)
    return tokens


def build_tfidf_vector(tf_counter: Counter[str], df_counter: Counter[str], total_docs: int) -> dict[str, float]:
    if total_docs <= 0:
        return {}
    vec: dict[str, float] = {}
    for token, tf in tf_counter.items():
        if tf <= 0:
            continue
        df = df_counter.get(token, 0)
        idf_base = math.log((total_docs + 1) / (df + 1)) + 1
        idf = idf_base ** TAG_RECOMMEND_IDF_EXPONENT
        vec[token] = float(tf) * idf
    return vec


def vector_norm(vec: dict[str, float]) -> float:
    return math.sqrt(sum(v * v for v in vec.values()))


def cosine_similarity(
    vec_a: dict[str, float],
    vec_b: dict[str, float],
    *,
    norm_a: float | None = None,
    norm_b: float | None = None,
) -> float:
    if not vec_a or not vec_b:
        return 0.0
    if len(vec_a) <= len(vec_b):
        numerator = sum(value * vec_b.get(token, 0.0) for token, value in vec_a.items())
    else:
        numerator = sum(value * vec_a.get(token, 0.0) for token, value in vec_b.items())
    if numerator <= 0:
        return 0.0
    norm_a = vector_norm(vec_a) if norm_a is None else norm_a
    norm_b = vector_norm(vec_b) if norm_b is None else norm_b
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return numerator / (norm_a * norm_b)


def tag_recommend_rank_weight(rank: int) -> float:
    base = TAG_RECOMMEND_SIMILAR_DOC_LIMIT + 1 - rank
    if base <= 0:
        return 0.0
    return float(base) ** TAG_RECOMMEND_RANK_WEIGHT_EXPONENT


def get_tag_recommend_corpus_content_cutoff(doc_count: int) -> int | None:
    cutoff: int | None = None
    for min_docs, char_limit in TAG_RECOMMEND_CORPUS_CONTENT_CUTOFFS:
        if doc_count < min_docs:
            break
        cutoff = char_limit
    return cutoff


def escape_fts5_phrase_token(token: str) -> str:
    return '"' + token.replace('"', '""') + '"'


def find_tag_recommendation_candidate_doc_ids(
    fts_conn: sqlite3.Connection,
    query_tokens: list[str],
    df_counter: Counter[str],
    total_docs: int,
    *,
    limit: int = TAG_RECOMMEND_FTS_CANDIDATE_LIMIT,
) -> set[int]:
    if not query_tokens or total_docs <= 0 or limit <= 0:
        return set()

    token_counts = Counter(query_tokens)
    first_positions: dict[str, int] = {}
    for index, token in enumerate(query_tokens):
        first_positions.setdefault(token, index)

    def token_idf(token: str) -> float:
        df = df_counter.get(token, 0)
        return math.log((total_docs + 1) / (df + 1)) + 1

    ranked_tokens = sorted(
        first_positions.keys(),
        key=lambda token: (-token_idf(token), -token_counts[token], first_positions[token]),
    )
    selected_tokens = ranked_tokens[:TAG_RECOMMEND_FTS_MAX_QUERY_TERMS]
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


def build_tag_recommendation_signature(
    conn: sqlite3.Connection,
    fts_conn: sqlite3.Connection,
) -> tuple[int, str, int, int]:
    row = conn.execute(
        """
        SELECT
            (SELECT COUNT(*) FROM docs) AS docs_count,
            (SELECT COALESCE(MAX(updated_at), '') FROM docs) AS docs_max_updated_at,
            (SELECT COUNT(*) FROM doc_tags) AS doc_tag_count
        """
    ).fetchone()
    if row is None:
        return (0, "", 0, 0)
    fts_count_row = fts_conn.execute("SELECT COUNT(*) AS c FROM docs_fts").fetchone()
    fts_count = int(fts_count_row["c"]) if fts_count_row is not None else 0
    return (
        int(row["docs_count"]),
        str(row["docs_max_updated_at"]),
        int(row["doc_tag_count"]),
        fts_count,
    )


def build_tag_recommendation_corpus(
    conn: sqlite3.Connection,
    fts_conn: sqlite3.Connection,
) -> list[dict[str, object]]:
    signature = build_tag_recommendation_signature(conn, fts_conn)
    with _TAG_RECOMMEND_CACHE_LOCK:
        cached_signature = _TAG_RECOMMEND_CACHE.get("signature")
        cached_corpus = _TAG_RECOMMEND_CACHE.get("corpus")
        if cached_signature == signature and isinstance(cached_corpus, list):
            return cached_corpus

    rows = conn.execute("SELECT id, title, slug, updated_at FROM docs ORDER BY id").fetchall()
    if not rows:
        with _TAG_RECOMMEND_CACHE_LOCK:
            _TAG_RECOMMEND_CACHE["signature"] = signature
            _TAG_RECOMMEND_CACHE["corpus"] = []
            _TAG_RECOMMEND_CACHE["entries_by_id"] = {}
            _TAG_RECOMMEND_CACHE["doc_states"] = {}
            _TAG_RECOMMEND_CACHE["df_counter"] = Counter()
            _TAG_RECOMMEND_CACHE["total_docs"] = 0
            _TAG_RECOMMEND_CACHE["content_cutoff"] = None
        return []

    doc_meta_by_id = {int(row["id"]): row for row in rows}
    doc_ids = list(doc_meta_by_id.keys())
    tag_map = _build_doc_tag_map(conn, doc_ids)
    content_cutoff = get_tag_recommend_corpus_content_cutoff(len(rows))
    doc_states: dict[int, tuple[str, str, str, tuple[str, ...]]] = {
        doc_id: (
            str(row["title"]),
            str(row["slug"]),
            str(row["updated_at"]),
            tuple(tag_map.get(doc_id, [])),
        )
        for doc_id, row in doc_meta_by_id.items()
    }

    with _TAG_RECOMMEND_CACHE_LOCK:
        cached_entries_raw = _TAG_RECOMMEND_CACHE.get("entries_by_id")
        cached_states_raw = _TAG_RECOMMEND_CACHE.get("doc_states")
        cached_cutoff = _TAG_RECOMMEND_CACHE.get("content_cutoff")
    cached_entries = cached_entries_raw if isinstance(cached_entries_raw, dict) else {}
    cached_states = cached_states_raw if isinstance(cached_states_raw, dict) else {}
    can_reuse_cached_entries = bool(cached_entries) and cached_cutoff == content_cutoff

    entries_by_id: dict[int, dict[str, object]] = {}
    changed_doc_ids: list[int] = []
    if can_reuse_cached_entries:
        for doc_id in doc_ids:
            if cached_states.get(doc_id) == doc_states[doc_id] and doc_id in cached_entries:
                entries_by_id[doc_id] = dict(cached_entries[doc_id])
            else:
                changed_doc_ids.append(doc_id)
    else:
        changed_doc_ids = doc_ids

    def make_corpus_entry(doc_id: int, content: str) -> dict[str, object] | None:
        row = doc_meta_by_id.get(doc_id)
        if row is None:
            return None
        doc_tokens = tokenize_text(f"{row['title']}\n{content}")
        if not doc_tokens:
            return None
        tf_counter = Counter(doc_tokens)
        return {
            "doc_id": doc_id,
            "slug": str(row["slug"]),
            "title": str(row["title"]),
            "tags": tag_map.get(doc_id, []),
            "tf_counter": tf_counter,
            "token_set": set(tf_counter.keys()),
        }

    fts_content_by_id: dict[int, str] = {}
    for chunk_start in range(0, len(changed_doc_ids), 400):
        chunk = changed_doc_ids[chunk_start : chunk_start + 400]
        if not chunk:
            continue
        placeholders = ",".join("?" for _ in chunk)
        if content_cutoff is None:
            rows_sql = f"SELECT rowid AS doc_id, content FROM docs_fts WHERE rowid IN ({placeholders})"
            params: list[object] = list(chunk)
        else:
            rows_sql = (
                f"SELECT rowid AS doc_id, substr(content, 1, ?) AS content "
                f"FROM docs_fts WHERE rowid IN ({placeholders})"
            )
            params = [content_cutoff, *chunk]
        for fts_row in fts_conn.execute(rows_sql, params):
            fts_content_by_id[int(fts_row["doc_id"])] = str(fts_row["content"] or "")

    for doc_id in changed_doc_ids:
        entry = make_corpus_entry(doc_id, fts_content_by_id.get(doc_id, ""))
        if entry is not None:
            entries_by_id[doc_id] = entry

    corpus: list[dict[str, object]] = [entries_by_id[doc_id] for doc_id in doc_ids if doc_id in entries_by_id]
    df_counter: Counter[str] = Counter()
    for doc in corpus:
        for token in doc["token_set"]:
            df_counter[token] += 1

    total_docs = len(corpus)
    for doc in corpus:
        doc_vec = build_tfidf_vector(doc["tf_counter"], df_counter, total_docs)
        doc["tfidf_vector"] = doc_vec
        doc["tfidf_norm"] = vector_norm(doc_vec)

    with _TAG_RECOMMEND_CACHE_LOCK:
        _TAG_RECOMMEND_CACHE["signature"] = signature
        _TAG_RECOMMEND_CACHE["corpus"] = corpus
        _TAG_RECOMMEND_CACHE["entries_by_id"] = entries_by_id
        _TAG_RECOMMEND_CACHE["doc_states"] = doc_states
        _TAG_RECOMMEND_CACHE["df_counter"] = df_counter
        _TAG_RECOMMEND_CACHE["total_docs"] = total_docs
        _TAG_RECOMMEND_CACHE["content_cutoff"] = content_cutoff
    return corpus


def recommend_tags(
    conn: sqlite3.Connection,
    fts_conn: sqlite3.Connection,
    *,
    title: str,
    content: str,
    current_slug: str | None = None,
    exclude_tags: list[str] | None = None,
    limit: int = TAG_RECOMMEND_LIMIT,
) -> list[str]:
    query_tokens = tokenize_text(f"{title}\n{content}")
    if not query_tokens:
        return []

    base_corpus = build_tag_recommendation_corpus(conn, fts_conn)
    full_corpus = [
        doc
        for doc in base_corpus
        if not current_slug or str(doc["slug"]) != current_slug
    ]
    if not full_corpus:
        return []

    with _TAG_RECOMMEND_CACHE_LOCK:
        cached_df_counter = _TAG_RECOMMEND_CACHE.get("df_counter")
        cached_total_docs = _TAG_RECOMMEND_CACHE.get("total_docs")
    df_counter = cached_df_counter if isinstance(cached_df_counter, Counter) else Counter()
    total_docs = cached_total_docs if isinstance(cached_total_docs, int) else len(base_corpus)
    candidate_query_tokens = [
        token
        for token in query_tokens
        if df_counter.get(token, 0) > 0
    ]
    candidate_doc_ids = find_tag_recommendation_candidate_doc_ids(
        fts_conn,
        candidate_query_tokens,
        df_counter,
        total_docs,
    )
    corpus = full_corpus
    if len(candidate_doc_ids) >= TAG_RECOMMEND_MIN_CANDIDATE_FALLBACK:
        candidate_corpus = [
            doc
            for doc in full_corpus
            if isinstance(doc.get("doc_id"), int) and int(doc["doc_id"]) in candidate_doc_ids
        ]
        if candidate_corpus:
            corpus = candidate_corpus

    query_vec = build_tfidf_vector(Counter(query_tokens), df_counter, total_docs)
    if not query_vec:
        return []
    query_norm = vector_norm(query_vec)
    if query_norm == 0:
        return []
    query_token_set = set(query_vec.keys())

    scored_docs: list[tuple[float, list[str]]] = []
    for doc in corpus:
        tags = list(doc["tags"])
        if not tags:
            continue
        doc_vec = doc.get("tfidf_vector")
        if not isinstance(doc_vec, dict):
            continue
        if not query_token_set.intersection(doc_vec.keys()):
            continue
        doc_norm = doc.get("tfidf_norm")
        similarity = cosine_similarity(
            query_vec,
            doc_vec,
            norm_a=query_norm,
            norm_b=doc_norm if isinstance(doc_norm, float) else None,
        )
        if similarity > 0:
            scored_docs.append((similarity, tags))
    if not scored_docs:
        return []

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
    return [display_names[key] for key in ordered[:limit]]
