from __future__ import annotations

import os
import math
import re
import sqlite3
import threading
from collections import Counter, defaultdict, deque
from datetime import datetime
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
    "total_docs": 0,
    "idf_by_token": {},
    "df_by_token": {},
}
TAG_RECOMMEND_TOKENIZER_VERSION = "tag-token-v1"
TAG_RECOMMEND_IDF_EXPONENT = 2.0
TAG_RECOMMEND_SIMILAR_DOC_LIMIT = 30
TAG_RECOMMEND_LIMIT = 25
TAG_RECOMMEND_RANK_WEIGHT_EXPONENT = 1.7
TAG_RECOMMEND_MAX_TOKENS_PER_DOC = 512
TAG_RECOMMEND_FTS_CANDIDATE_LIMIT = 250
TAG_RECOMMEND_FTS_MAX_QUERY_TERMS = 50
TAG_RECOMMEND_TOKEN_DB_MAX_QUERY_TERMS = 50
TAG_RECOMMEND_TOKEN_DB_CANDIDATE_LIMIT = 500
TAG_RECOMMEND_TOKEN_DB_MAX_DF_RATIO = 0.20
TAG_RECOMMEND_MIN_CANDIDATE_FALLBACK = 80
TAG_RECOMMEND_DEBUG = os.environ.get("PERSONALWIKI_TAG_RECOMMEND_DEBUG") == "1"
SQLITE_IN_CLAUSE_CHUNK_SIZE = 400

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
        _TAG_RECOMMEND_CACHE["total_docs"] = 0
        _TAG_RECOMMEND_CACHE["idf_by_token"] = {}
        _TAG_RECOMMEND_CACHE["df_by_token"] = {}


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
        vec[token] = float(tf) * compute_tag_recommendation_idf(total_docs, df)
    return vec


def compute_tag_recommendation_idf(total_docs: int, df: int) -> float:
    if total_docs <= 0:
        return 0.0
    idf_base = math.log((total_docs + 1) / (max(df, 0) + 1)) + 1
    return idf_base ** TAG_RECOMMEND_IDF_EXPONENT


def limit_tf_counter(tf_counter: Counter[str], max_tokens: int) -> Counter[str]:
    if max_tokens <= 0 or len(tf_counter) <= max_tokens:
        return tf_counter
    return Counter(dict(tf_counter.most_common(max_tokens)))


def compute_doc_token_counters(title: str, content: str) -> dict[str, Counter[str]]:
    title_counter = Counter(tokenize_text(title))
    content_counter = limit_tf_counter(
        Counter(tokenize_text(content)),
        TAG_RECOMMEND_MAX_TOKENS_PER_DOC,
    )
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
        CREATE INDEX IF NOT EXISTS idx_language_doc_tokens_doc_id
        ON language_doc_tokens(doc_id)
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


def _set_language_total_docs_and_version(conn: sqlite3.Connection, total_docs: int) -> None:
    _set_language_meta(conn, "total_docs", total_docs)
    _set_language_meta(conn, "tokenizer_version", TAG_RECOMMEND_TOKENIZER_VERSION)


def get_existing_doc_token_set(conn: sqlite3.Connection, doc_id: int) -> set[str]:
    ensure_language_token_tables(conn)
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
    if tokens is None:
        rows = conn.execute("SELECT token, df FROM language_token_stats").fetchall()
    else:
        token_list = sorted(tokens)
        if not token_list:
            return
        rows = []
        for chunk in _chunked(token_list):
            placeholders = ",".join("?" for _ in chunk)
            rows.extend(
                conn.execute(
                    f"SELECT token, df FROM language_token_stats WHERE token IN ({placeholders})",
                    list(chunk),
                ).fetchall()
            )
    updates = [
        (compute_tag_recommendation_idf(total_docs, int(row["df"])), str(row["token"]))
        for row in rows
    ]
    if updates:
        conn.executemany(
            "UPDATE language_token_stats SET idf = ? WHERE token = ?",
            updates,
        )


def upsert_language_doc_tokens(
    token_conn: sqlite3.Connection,
    main_conn: sqlite3.Connection,
    doc_id: int,
    title: str,
    content: str,
) -> None:
    ensure_language_token_tables(token_conn)
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

    if stored_total_docs != total_docs:
        _recompute_language_token_idfs(token_conn, total_docs)
    else:
        _recompute_language_token_idfs(token_conn, total_docs, removed_tokens | added_tokens)
    _set_language_total_docs_and_version(token_conn, total_docs)
    invalidate_tag_recommendation_cache()


def delete_language_doc_tokens(
    token_conn: sqlite3.Connection,
    main_conn: sqlite3.Connection,
    doc_id: int,
) -> None:
    ensure_language_token_tables(token_conn)
    old_token_set = get_existing_doc_token_set(token_conn, doc_id)
    stored_total_docs = _get_stored_language_total_docs(token_conn)
    total_docs = _get_main_doc_count(main_conn)

    _decrement_language_token_dfs(token_conn, old_token_set)
    token_conn.execute("DELETE FROM language_doc_tokens WHERE doc_id = ?", (doc_id,))
    if stored_total_docs != total_docs:
        _recompute_language_token_idfs(token_conn, total_docs)
    else:
        _recompute_language_token_idfs(token_conn, total_docs, old_token_set)
    _set_language_total_docs_and_version(token_conn, total_docs)
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
    df_counter: Counter[str] = Counter()
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
            token_set = _doc_token_set(counters)
            df_counter.update(token_set)
            pending_rows.extend(_doc_token_rows(doc_id, counters))
            if len(pending_rows) >= 5000:
                flush_pending_rows()
    flush_pending_rows()

    stats_rows = [
        (token, int(df), compute_tag_recommendation_idf(total_docs, int(df)))
        for token, df in sorted(df_counter.items())
    ]
    if stats_rows:
        token_conn.executemany(
            """
            INSERT INTO language_token_stats (token, df, idf)
            VALUES (?, ?, ?)
            """,
            stats_rows,
        )
    _set_language_total_docs_and_version(token_conn, total_docs)
    _set_language_meta(token_conn, "last_rebuild_at", datetime.now().isoformat(timespec="seconds"))
    invalidate_tag_recommendation_cache()
    return total_docs, len(stats_rows)


def language_token_index_needs_rebuild(
    token_conn: sqlite3.Connection,
    main_conn: sqlite3.Connection,
) -> bool:
    ensure_language_token_tables(token_conn)
    total_docs = _get_main_doc_count(main_conn)
    tokenizer_version = _get_language_meta(token_conn, "tokenizer_version")
    stored_total_docs = _get_stored_language_total_docs(token_conn)
    token_row = token_conn.execute("SELECT COUNT(*) AS c FROM language_doc_tokens").fetchone()
    token_row_count = int(token_row["c"]) if token_row is not None else 0
    if tokenizer_version != TAG_RECOMMEND_TOKENIZER_VERSION:
        return True
    if stored_total_docs != total_docs:
        return True
    if total_docs > 0 and token_row_count == 0:
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
    total_docs = _get_main_doc_count(main_conn)
    row = token_conn.execute("SELECT COUNT(*) AS c FROM language_token_stats").fetchone()
    token_count = int(row["c"]) if row is not None else 0
    return False, total_docs, token_count


def get_language_token_stats_for_tokens(
    conn: sqlite3.Connection,
    tokens: list[str],
) -> tuple[dict[str, int], dict[str, float]]:
    ensure_language_token_tables(conn)
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
            df_by_token[token] = int(row["df"])
            idf_by_token[token] = float(row["idf"])
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


def escape_fts5_phrase_token(token: str) -> str:
    return '"' + token.replace('"', '""') + '"'


def select_tag_recommendation_fts_query_tokens(
    query_tokens: list[str],
    df_by_token: dict[str, int] | Counter[str],
    total_docs: int,
) -> list[str]:
    token_counts = Counter(query_tokens)
    first_positions: dict[str, int] = {}
    for index, token in enumerate(query_tokens):
        first_positions.setdefault(token, index)

    def token_idf(token: str) -> float:
        df = df_by_token.get(token, 0)
        return math.log((total_docs + 1) / (df + 1)) + 1

    ranked_tokens = sorted(
        first_positions.keys(),
        key=lambda token: (-token_idf(token), -token_counts[token], first_positions[token]),
    )
    return ranked_tokens[:TAG_RECOMMEND_FTS_MAX_QUERY_TERMS]


def find_tag_recommendation_candidate_doc_ids(
    fts_conn: sqlite3.Connection,
    query_tokens: list[str],
    df_by_token: dict[str, int] | Counter[str],
    total_docs: int,
    *,
    limit: int = TAG_RECOMMEND_FTS_CANDIDATE_LIMIT,
) -> set[int]:
    if not query_tokens or total_docs <= 0 or limit <= 0:
        return set()

    selected_tokens = select_tag_recommendation_fts_query_tokens(
        query_tokens,
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
    query_tokens: list[str],
    df_by_token: dict[str, int],
    idf_by_token: dict[str, float],
    total_docs: int,
) -> list[str]:
    if total_docs <= 0:
        return []
    token_counts = Counter(query_tokens)
    first_positions: dict[str, int] = {}
    for index, token in enumerate(query_tokens):
        first_positions.setdefault(token, index)

    usable_tokens = [
        token
        for token in first_positions.keys()
        if df_by_token.get(token, 0) > 0
        and (df_by_token[token] / max(total_docs, 1)) <= TAG_RECOMMEND_TOKEN_DB_MAX_DF_RATIO
    ]
    usable_tokens.sort(
        key=lambda token: (
            -idf_by_token.get(token, 0.0),
            -token_counts[token],
            first_positions[token],
        )
    )
    return usable_tokens[:TAG_RECOMMEND_TOKEN_DB_MAX_QUERY_TERMS]


def find_tag_recommendation_language_candidate_doc_ids(
    conn: sqlite3.Connection,
    query_tokens: list[str],
    total_docs: int,
    *,
    limit: int = TAG_RECOMMEND_TOKEN_DB_CANDIDATE_LIMIT,
) -> set[int]:
    if not query_tokens or total_docs <= 0 or limit <= 0:
        return set()

    df_by_token, idf_by_token = get_language_token_stats_for_tokens(conn, query_tokens)
    selected_tokens = select_tag_recommendation_language_query_tokens(
        query_tokens,
        df_by_token,
        idf_by_token,
        total_docs,
    )
    if not selected_tokens:
        return set()

    placeholders = ",".join("?" for _ in selected_tokens)
    rows = conn.execute(
        f"""
        SELECT
            l.doc_id,
            SUM(l.tf * s.idf) AS token_score,
            COUNT(DISTINCT l.token) AS overlap_count
        FROM language_doc_tokens l
        JOIN language_token_stats s ON s.token = l.token
        WHERE l.token IN ({placeholders})
        GROUP BY l.doc_id
        ORDER BY token_score DESC, overlap_count DESC
        LIMIT ?
        """,
        [*selected_tokens, limit],
    ).fetchall()
    return {int(row["doc_id"]) for row in rows}


def build_doc_vectors_from_language_tokens(
    conn: sqlite3.Connection,
    doc_ids: list[int],
    idf_by_token: dict[str, float],
) -> dict[int, dict[str, float]]:
    unique_doc_ids = sorted(set(int(doc_id) for doc_id in doc_ids))
    if not unique_doc_ids:
        return {}

    vectors: dict[int, dict[str, float]] = defaultdict(dict)
    for chunk in _chunked(unique_doc_ids):
        placeholders = ",".join("?" for _ in chunk)
        if idf_by_token:
            rows = conn.execute(
                f"""
                SELECT doc_id, token, SUM(tf) AS tf
                FROM language_doc_tokens
                WHERE doc_id IN ({placeholders})
                GROUP BY doc_id, token
                """,
                list(chunk),
            ).fetchall()
            for row in rows:
                token = str(row["token"])
                idf = idf_by_token.get(token)
                if idf is None:
                    continue
                vectors[int(row["doc_id"])][token] = float(row["tf"]) * idf
        else:
            rows = conn.execute(
                f"""
                SELECT l.doc_id, l.token, SUM(l.tf * s.idf) AS value
                FROM language_doc_tokens l
                JOIN language_token_stats s ON s.token = l.token
                WHERE l.doc_id IN ({placeholders})
                GROUP BY l.doc_id, l.token
                """,
                list(chunk),
            ).fetchall()
            for row in rows:
                vectors[int(row["doc_id"])][str(row["token"])] = float(row["value"] or 0.0)
    return {doc_id: vec for doc_id, vec in vectors.items() if vec}


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


def _log_tag_recommendation_debug(**values: object) -> None:
    if not TAG_RECOMMEND_DEBUG:
        return
    details = " ".join(f"{key}={value}" for key, value in values.items())
    print(f"[TAG_RECOMMEND] {details}")


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
    rows = conn.execute("SELECT id, title, slug, updated_at FROM docs ORDER BY id").fetchall()
    if not rows:
        return []

    doc_meta_by_id = {int(row["id"]): row for row in rows}
    doc_ids = list(doc_meta_by_id.keys())
    tag_map = _build_doc_tag_map(conn, doc_ids)

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
    for chunk_start in range(0, len(doc_ids), 400):
        chunk = doc_ids[chunk_start : chunk_start + 400]
        if not chunk:
            continue
        placeholders = ",".join("?" for _ in chunk)
        rows_sql = f"SELECT rowid AS doc_id, content FROM docs_fts WHERE rowid IN ({placeholders})"
        params: list[object] = list(chunk)
        for fts_row in fts_conn.execute(rows_sql, params):
            fts_content_by_id[int(fts_row["doc_id"])] = str(fts_row["content"] or "")

    entries_by_id: dict[int, dict[str, object]] = {}
    for doc_id in doc_ids:
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

    return corpus


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
    query_tokens = tokenize_text(f"{title}\n{content}")
    if not query_tokens:
        return []

    token_conn = token_conn or conn
    ensure_language_token_tables(token_conn)
    total_docs = _get_main_doc_count(conn)
    if total_docs <= 0:
        return []

    df_by_token, idf_by_token = get_language_token_stats_for_tokens(token_conn, query_tokens)
    candidate_query_tokens = [
        token
        for token in query_tokens
        if df_by_token.get(token, 0) > 0
    ]
    selected_fts_tokens = select_tag_recommendation_fts_query_tokens(
        candidate_query_tokens,
        df_by_token,
        total_docs,
    )
    fts_candidate_doc_ids = find_tag_recommendation_candidate_doc_ids(
        fts_conn,
        candidate_query_tokens,
        df_by_token,
        total_docs,
    )
    selected_token_db_tokens = select_tag_recommendation_language_query_tokens(
        candidate_query_tokens,
        df_by_token,
        idf_by_token,
        total_docs,
    )
    token_candidate_doc_ids = find_tag_recommendation_language_candidate_doc_ids(
        token_conn,
        candidate_query_tokens,
        total_docs,
    )
    candidate_doc_ids = fts_candidate_doc_ids | token_candidate_doc_ids
    fallback_used = len(candidate_doc_ids) < TAG_RECOMMEND_MIN_CANDIDATE_FALLBACK

    doc_rows = _fetch_recommendation_doc_rows(
        conn,
        doc_ids=None if fallback_used else candidate_doc_ids,
        current_slug=current_slug,
    )
    if not doc_rows:
        _log_tag_recommendation_debug(
            query_tokens=len(query_tokens),
            selected_fts_terms=len(selected_fts_tokens),
            selected_token_db_terms=len(selected_token_db_tokens),
            fts_candidates=len(fts_candidate_doc_ids),
            token_db_candidates=len(token_candidate_doc_ids),
            merged_candidates=len(candidate_doc_ids),
            fallback_used=fallback_used,
            scored_docs=0,
            final_tags=0,
        )
        return []

    doc_ids = [int(row["id"]) for row in doc_rows]
    tag_map = _build_doc_tag_map(conn, doc_ids)
    doc_vectors = build_doc_vectors_from_language_tokens(token_conn, doc_ids, {})
    if not doc_vectors:
        _log_tag_recommendation_debug(
            query_tokens=len(query_tokens),
            selected_fts_terms=len(selected_fts_tokens),
            selected_token_db_terms=len(selected_token_db_tokens),
            fts_candidates=len(fts_candidate_doc_ids),
            token_db_candidates=len(token_candidate_doc_ids),
            merged_candidates=len(candidate_doc_ids),
            fallback_used=fallback_used,
            scored_docs=0,
            final_tags=0,
        )
        return []

    query_vec = build_tfidf_vector_from_idf(Counter(query_tokens), idf_by_token)
    if not query_vec:
        _log_tag_recommendation_debug(
            query_tokens=len(query_tokens),
            selected_fts_terms=len(selected_fts_tokens),
            selected_token_db_terms=len(selected_token_db_tokens),
            fts_candidates=len(fts_candidate_doc_ids),
            token_db_candidates=len(token_candidate_doc_ids),
            merged_candidates=len(candidate_doc_ids),
            fallback_used=fallback_used,
            scored_docs=0,
            final_tags=0,
        )
        return []
    query_norm = vector_norm(query_vec)
    if query_norm == 0:
        return []
    query_token_set = set(query_vec.keys())

    scored_docs: list[tuple[float, list[str]]] = []
    for doc_id in doc_ids:
        tags = tag_map.get(doc_id, [])
        if not tags:
            continue
        doc_vec = doc_vectors.get(doc_id)
        if not doc_vec:
            continue
        if not query_token_set.intersection(doc_vec.keys()):
            continue
        similarity = cosine_similarity(
            query_vec,
            doc_vec,
            norm_a=query_norm,
            norm_b=vector_norm(doc_vec),
        )
        if similarity > 0:
            scored_docs.append((similarity, tags))
    if not scored_docs:
        _log_tag_recommendation_debug(
            query_tokens=len(query_tokens),
            selected_fts_terms=len(selected_fts_tokens),
            selected_token_db_terms=len(selected_token_db_tokens),
            fts_candidates=len(fts_candidate_doc_ids),
            token_db_candidates=len(token_candidate_doc_ids),
            merged_candidates=len(candidate_doc_ids),
            fallback_used=fallback_used,
            scored_docs=0,
            final_tags=0,
        )
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
    recommendations = [display_names[key] for key in ordered[:limit]]
    _log_tag_recommendation_debug(
        query_tokens=len(query_tokens),
        selected_fts_terms=len(selected_fts_tokens),
        selected_token_db_terms=len(selected_token_db_tokens),
        fts_candidates=len(fts_candidate_doc_ids),
        token_db_candidates=len(token_candidate_doc_ids),
        merged_candidates=len(candidate_doc_ids),
        fallback_used=fallback_used,
        scored_docs=len(scored_docs),
        final_tags=len(recommendations),
    )
    return recommendations
