from __future__ import annotations

import json
import math
import atexit
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import threading
import unicodedata
from collections import Counter, defaultdict, deque
from contextlib import closing
from datetime import datetime
from functools import lru_cache
from pathlib import Path

from flask import (
    Flask,
    abort,
    g,
    jsonify,
    redirect,
    render_template,
    request,
    send_from_directory,
    url_for,
)
from markupsafe import Markup

from markdown_engine import MarkdownEngine, extract_reference_targets


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def runtime_paths() -> tuple[Path, Path]:
    if getattr(sys, "frozen", False):
        resource_dir = Path(getattr(sys, "_MEIPASS", Path(sys.executable).resolve().parent))
        data_dir = Path(sys.executable).resolve().parent
    else:
        resource_dir = Path(__file__).resolve().parent
        data_dir = resource_dir
    return resource_dir, data_dir


RESOURCE_DIR, DATA_DIR = runtime_paths()
DOC_DIR = DATA_DIR / "doc"
JSON_DIR = DOC_DIR / "json"
IMG_DIR = DATA_DIR / "img"
FILE_DIR = DATA_DIR / "file"
DB_PATH = DATA_DIR / "wiki.db"
FTS_DB_PATH = DATA_DIR / "wiki_fts.db"
DATA_LOCK_PATH = DATA_DIR / "wiki.lock"
TEMPLATE_DIR = RESOURCE_DIR / "templates"
STATIC_DIR = RESOURCE_DIR / "static"
_DATA_LOCK_FILE = None


app = Flask(
    __name__,
    template_folder=str(TEMPLATE_DIR),
    static_folder=str(STATIC_DIR),
)
app.config["JSON_AS_ASCII"] = False

markdown_engine = MarkdownEngine()

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
    "df_counter": Counter(),
    "total_docs": 0,
}
TAG_RECOMMEND_IDF_EXPONENT = 2.0
TAG_RECOMMEND_SIMILAR_DOC_LIMIT = 30
TAG_RECOMMEND_LIMIT = 25
TAG_RECOMMEND_RANK_WEIGHT_EXPONENT = 1.7
TAG_RECOMMEND_FTS_CANDIDATE_LIMIT = 200
TAG_RECOMMEND_FTS_MAX_QUERY_TERMS = 30
TAG_RECOMMEND_MIN_CANDIDATE_FALLBACK = 10
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
        _TAG_RECOMMEND_CACHE["df_counter"] = Counter()
        _TAG_RECOMMEND_CACHE["total_docs"] = 0


DIRTY_MTIME_GRACE_SECONDS = 5.0
SEVERE_DIFF_MIN_CHANGES = 50
SEVERE_DIFF_RATIO = 0.9
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 6885
UNLINKABLE_TITLE_PREFIXES = ("http://", "https://", "file/")
TITLE_LINK_LIMIT_WARNING = (
    "이 문서는 위키 엔진 한계상 링크가 제대로 동작하지 않을 수 있습니다. "
    "제목이 file/, http://, https:// 로 시작하면 위키 링크 해석이 충돌할 수 있습니다."
)


class StartupRecoveryNeeded(RuntimeError):
    pass


def _lock_file_handle(handle) -> None:
    handle.seek(0)
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        return

    import fcntl

    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)


def _unlock_file_handle(handle) -> None:
    handle.seek(0)
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        return

    import fcntl

    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def acquire_data_lock() -> None:
    global _DATA_LOCK_FILE
    if _DATA_LOCK_FILE is not None:
        return

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    handle = DATA_LOCK_PATH.open("a+b")
    try:
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"\0")
            handle.flush()
        _lock_file_handle(handle)
    except OSError as error:
        handle.close()
        raise RuntimeError(
            "PersonalWiki 데이터베이스가 이미 다른 프로세스에서 사용 중입니다. "
            "실행 중인 PersonalWiki 또는 DBFix를 종료한 뒤 다시 시작해 주세요."
        ) from error

    _DATA_LOCK_FILE = handle
    atexit.register(release_data_lock)


def release_data_lock() -> None:
    global _DATA_LOCK_FILE
    handle = _DATA_LOCK_FILE
    if handle is None:
        return
    _DATA_LOCK_FILE = None
    try:
        _unlock_file_handle(handle)
    except OSError:
        pass
    handle.close()


def configure_sqlite_connection(conn: sqlite3.Connection, *, foreign_keys: bool) -> None:
    conn.execute("PRAGMA busy_timeout = 5000")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA wal_autocheckpoint = 1000")
    conn.execute("PRAGMA temp_store = MEMORY")
    conn.execute("PRAGMA cache_size = -20000")
    if foreign_keys:
        conn.execute("PRAGMA foreign_keys = ON")


def connect_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    configure_sqlite_connection(conn, foreign_keys=True)
    return conn


def connect_fts_db() -> sqlite3.Connection:
    conn = sqlite3.connect(FTS_DB_PATH)
    conn.row_factory = sqlite3.Row
    configure_sqlite_connection(conn, foreign_keys=False)
    return conn


def get_db() -> sqlite3.Connection:
    if "db" not in g:
        g.db = connect_db()
    return g.db


def get_fts_db() -> sqlite3.Connection:
    if "fts_db" not in g:
        g.fts_db = connect_fts_db()
    return g.fts_db


@app.teardown_appcontext
def close_db(_error: BaseException | None) -> None:
    conn = g.pop("db", None)
    if conn is not None:
        conn.close()
    fts_conn = g.pop("fts_db", None)
    if fts_conn is not None:
        fts_conn.close()


def init_storage() -> None:
    DOC_DIR.mkdir(parents=True, exist_ok=True)
    JSON_DIR.mkdir(parents=True, exist_ok=True)
    IMG_DIR.mkdir(parents=True, exist_ok=True)
    FILE_DIR.mkdir(parents=True, exist_ok=True)
    ensure_default_favicon()


def ensure_default_favicon() -> None:
    source = RESOURCE_DIR / "img" / "icon.ico"
    target = IMG_DIR / "icon.ico"
    if target.exists() or not source.exists():
        return
    try:
        shutil.copyfile(source, target)
    except OSError as error:
        print(f"[WARN] failed to copy default favicon: {error}")


def init_db() -> None:
    with closing(connect_db()) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS docs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL COLLATE NOCASE UNIQUE,
                slug TEXT NOT NULL UNIQUE,
                file_path TEXT NOT NULL,
                meta_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tags (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL COLLATE NOCASE UNIQUE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS doc_tags (
                doc_id INTEGER NOT NULL,
                tag_id INTEGER NOT NULL,
                PRIMARY KEY (doc_id, tag_id),
                FOREIGN KEY (doc_id) REFERENCES docs (id) ON DELETE CASCADE,
                FOREIGN KEY (tag_id) REFERENCES tags (id) ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS doc_references (
                source_doc_id INTEGER NOT NULL,
                ref_type TEXT NOT NULL CHECK (ref_type IN ('link', 'template')),
                raw_target TEXT NOT NULL,
                target_title_key TEXT NOT NULL,
                target_slug_key TEXT NOT NULL,
                PRIMARY KEY (source_doc_id, ref_type, raw_target),
                FOREIGN KEY (source_doc_id) REFERENCES docs (id) ON DELETE CASCADE
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_docs_slug ON docs (slug)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_doc_tags_tag_id ON doc_tags (tag_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_doc_refs_title_key ON doc_references (target_title_key)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_doc_refs_slug_key ON doc_references (target_slug_key)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_doc_refs_source ON doc_references (source_doc_id)")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_doc_refs_title_lookup "
            "ON doc_references (target_title_key, source_doc_id, ref_type)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_doc_refs_slug_lookup "
            "ON doc_references (target_slug_key, source_doc_id, ref_type)"
        )
        # Legacy cleanup: older versions stored docs_fts in wiki.db.
        conn.execute("DROP TABLE IF EXISTS docs_fts")
        conn.commit()


def init_fts_db() -> None:
    with closing(connect_fts_db()) as conn:
        conn.execute(
            """
            CREATE VIRTUAL TABLE IF NOT EXISTS docs_fts
            USING fts5(title, content)
            """
        )
        conn.commit()


def safe_load_json(raw: str) -> dict:
    try:
        data = json.loads(raw)
        if isinstance(data, dict):
            return data
    except (TypeError, json.JSONDecodeError):
        pass
    return {}


def slugify(title: str) -> str:
    normalized = unicodedata.normalize("NFKC", title).strip()
    normalized = re.sub(r"[^\w\s\-가-힣]", "", normalized, flags=re.UNICODE)
    normalized = re.sub(r"[\s_]+", "-", normalized, flags=re.UNICODE)
    slug = normalized.strip("-").lower()
    return slug or "untitled"


def ensure_unique_title(conn: sqlite3.Connection, base_title: str, exclude_doc_id: int | None = None) -> str:
    title = base_title.strip() or "untitled"
    candidate = title
    suffix = 2
    while True:
        if exclude_doc_id is None:
            row = conn.execute("SELECT id FROM docs WHERE title = ? COLLATE NOCASE", (candidate,)).fetchone()
        else:
            row = conn.execute(
                "SELECT id FROM docs WHERE title = ? COLLATE NOCASE AND id != ?",
                (candidate, exclude_doc_id),
            ).fetchone()
        if row is None:
            return candidate
        candidate = f"{title} ({suffix})"
        suffix += 1


def ensure_unique_slug(conn: sqlite3.Connection, base_slug: str, exclude_doc_id: int | None = None) -> str:
    root = base_slug or "untitled"
    candidate = root
    idx = 2
    while True:
        row = conn.execute("SELECT id FROM docs WHERE slug = ?", (candidate,)).fetchone()
        if row is None or row["id"] == exclude_doc_id:
            return candidate
        candidate = f"{root}-{idx}"
        idx += 1


def document_path(slug: str) -> Path:
    return DOC_DIR / f"{slug}.md"


def sidecar_path(slug: str) -> Path:
    return JSON_DIR / f"{slug}.json"


def normalize_newlines(text: str) -> str:
    if not text:
        return ""
    # Repair malformed CRCRLF sequences caused by double newline translation.
    normalized = text.replace("\r\r\n", "\n")
    normalized = normalized.replace("\r\n", "\n").replace("\r", "\n")
    return normalized


def read_text_normalized(path: Path) -> str:
    with path.open("r", encoding="utf-8", newline="") as file:
        return normalize_newlines(file.read())


def read_document(slug: str) -> str:
    path = document_path(slug)
    if not path.exists():
        return ""
    return read_text_normalized(path)


def read_document_if_exists(slug: str) -> str | None:
    path = document_path(slug)
    if not path.exists():
        return None
    return read_text_normalized(path)


def write_document(slug: str, content: str) -> None:
    normalized = normalize_newlines(content)
    document_path(slug).write_text(normalized, encoding="utf-8", newline="\n")


def move_document_assets_for_slug_change(old_slug: str, new_slug: str) -> list[tuple[Path, Path]]:
    moved: list[tuple[Path, Path]] = []
    for old_path, new_path in (
        (document_path(old_slug), document_path(new_slug)),
        (sidecar_path(old_slug), sidecar_path(new_slug)),
    ):
        if not old_path.exists():
            continue
        if new_path.exists():
            raise FileExistsError(f"target document asset already exists: {new_path}")
        old_path.rename(new_path)
        moved.append((old_path, new_path))
    return moved


def rollback_document_asset_moves(moved: list[tuple[Path, Path]]) -> None:
    for old_path, new_path in reversed(moved):
        try:
            if new_path.exists() and not old_path.exists():
                new_path.rename(old_path)
        except OSError as error:
            print(f"[WARN] failed to rollback document asset move {new_path} -> {old_path}: {error}")


def load_sidecar(slug: str) -> dict:
    path = sidecar_path(slug)
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            return data
    except json.JSONDecodeError:
        return {}
    return {}


def normalize_reference_target(value: str) -> str:
    return unicodedata.normalize("NFKC", str(value)).strip()


def dedupe_reference_targets(values: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for raw in values:
        target = normalize_reference_target(raw)
        if not target:
            continue
        key = target.casefold()
        if key in seen:
            continue
        seen.add(key)
        result.append(target)
    return result


def extract_reference_payload(content: str) -> dict[str, list[str]]:
    wiki_refs, template_refs = extract_reference_targets(content)
    return {
        "links": dedupe_reference_targets(wiki_refs),
        "templates": dedupe_reference_targets(template_refs),
    }


def normalize_reference_payload(references: dict | None) -> dict[str, list[str]]:
    if not isinstance(references, dict):
        return {"links": [], "templates": []}

    links_raw = references.get("links")
    templates_raw = references.get("templates")

    links = dedupe_reference_targets([str(item) for item in links_raw]) if isinstance(links_raw, list) else []
    templates = (
        dedupe_reference_targets([str(item) for item in templates_raw]) if isinstance(templates_raw, list) else []
    )
    return {
        "links": links,
        "templates": templates,
    }


def reference_title_key(value: str) -> str:
    return normalize_reference_target(value).casefold()


def write_sidecar(
    *,
    slug: str,
    title: str,
    created_at: str,
    updated_at: str,
    tags: list[str],
    meta: dict,
    references: dict | None = None,
) -> None:
    normalized_references = normalize_reference_payload(references)
    payload = {
        "title": title,
        "slug": slug,
        "created_at": created_at,
        "updated_at": updated_at,
        "tags": tags,
        "meta": meta,
        "references": normalized_references,
    }
    sidecar_path(slug).write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
        newline="\n",
    )


def build_doc_tag_map(conn: sqlite3.Connection, doc_ids: list[int]) -> dict[int, list[str]]:
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


def list_doc_tags(conn: sqlite3.Connection, doc_id: int) -> list[str]:
    return build_doc_tag_map(conn, [doc_id]).get(doc_id, [])


def parse_tags(raw: str) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for part in raw.split(","):
        tag = part.strip()
        if not tag:
            continue
        lowered = tag.casefold()
        if lowered in seen:
            continue
        seen.add(lowered)
        result.append(tag)
    return result


def collect_sidecar_tags(sidecar: dict) -> list[str]:
    tags_value = sidecar.get("tags")
    if isinstance(tags_value, list):
        return parse_tags(",".join(str(item) for item in tags_value))
    if isinstance(tags_value, str):
        return parse_tags(tags_value)
    return []


def has_unlinkable_title_prefix(title: str) -> bool:
    lowered = title.strip().casefold()
    return any(lowered.startswith(prefix) for prefix in UNLINKABLE_TITLE_PREFIXES)


def title_prefix_warning(title: str) -> str | None:
    if not title:
        return None
    if has_unlinkable_title_prefix(title):
        return TITLE_LINK_LIMIT_WARNING
    return None


KOREAN_SPELL_REPLACEMENTS = dict(KOREAN_SPELL_REPLACE_DB)


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


def select_korean_spell_replacements(text: str) -> list[tuple[int, int, str, str]]:
    matches = list(iter_korean_spell_matches(text))
    if not matches:
        return []

    matches.sort(key=lambda item: (item[0], -(item[1] - item[0])))
    selected: list[tuple[int, int, str, str]] = []
    cursor = 0
    for start, end, wrong, replace in matches:
        if start < cursor:
            continue
        selected.append((start, end, wrong, replace))
        cursor = end
    return selected


def collect_korean_spell_issues(title: str, content: str) -> dict[str, object] | None:
    count = 0
    samples: list[dict[str, str]] = []
    sampled: set[tuple[str, str, str]] = set()

    for field_label, text in (("제목", title), ("본문", content)):
        for _start, _end, wrong, replace in select_korean_spell_replacements(text):
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
    matches = select_korean_spell_replacements(text)
    if not matches:
        return text

    parts: list[str] = []
    cursor = 0
    for start, end, _wrong, replace in matches:
        parts.append(text[cursor:start])
        parts.append(replace)
        cursor = end
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


def render_edit_form(**context):
    context.setdefault("spell_warning", None)
    context.setdefault("show_spellcheck_warning", False)
    context.setdefault("spellcheck_samples", [])
    context.setdefault("ignore_tag_warning", False)
    return render_template("edit.html", **context)


def set_doc_tags(conn: sqlite3.Connection, doc_id: int, tags: list[str]) -> None:
    old_tag_rows = conn.execute(
        "SELECT tag_id FROM doc_tags WHERE doc_id = ?",
        (doc_id,),
    ).fetchall()
    affected_tag_ids = {int(row["tag_id"]) for row in old_tag_rows}

    conn.execute("DELETE FROM doc_tags WHERE doc_id = ?", (doc_id,))
    normalized_tags = parse_tags(",".join(str(tag) for tag in tags))
    if normalized_tags:
        conn.executemany(
            "INSERT INTO tags (name) VALUES (?) ON CONFLICT(name) DO NOTHING",
            [(tag,) for tag in normalized_tags],
        )
        placeholders = ",".join("?" for _ in normalized_tags)
        tag_rows = conn.execute(
            f"SELECT id FROM tags WHERE name COLLATE NOCASE IN ({placeholders})",
            normalized_tags,
        ).fetchall()
        if tag_rows:
            affected_tag_ids.update(int(row["id"]) for row in tag_rows)
            conn.executemany(
                "INSERT OR IGNORE INTO doc_tags (doc_id, tag_id) VALUES (?, ?)",
                [(doc_id, int(row["id"])) for row in tag_rows],
            )
    if affected_tag_ids:
        placeholders = ",".join("?" for _ in affected_tag_ids)
        conn.execute(
            f"""
            DELETE FROM tags
            WHERE id IN ({placeholders})
              AND NOT EXISTS (
                SELECT 1 FROM doc_tags WHERE doc_tags.tag_id = tags.id
              )
            """,
            list(affected_tag_ids),
        )


def set_doc_references(conn: sqlite3.Connection, doc_id: int, references: dict[str, list[str]]) -> None:
    payload = normalize_reference_payload(references)
    conn.execute("DELETE FROM doc_references WHERE source_doc_id = ?", (doc_id,))

    rows: list[tuple[int, str, str, str, str]] = []
    for ref_type, key in (("link", "links"), ("template", "templates")):
        for raw_target in payload[key]:
            rows.append(
                (
                    doc_id,
                    ref_type,
                    raw_target,
                    reference_title_key(raw_target),
                    slugify(raw_target),
                )
            )
    if rows:
        conn.executemany(
            """
            INSERT OR IGNORE INTO doc_references
            (source_doc_id, ref_type, raw_target, target_title_key, target_slug_key)
            VALUES (?, ?, ?, ?, ?)
            """,
            rows,
        )


def update_fts(fts_conn: sqlite3.Connection, doc_id: int, title: str, content: str) -> None:
    fts_conn.execute("DELETE FROM docs_fts WHERE rowid = ?", (doc_id,))
    fts_conn.execute(
        "INSERT INTO docs_fts (rowid, title, content) VALUES (?, ?, ?)",
        (doc_id, title, content),
    )

def infer_title_from_content(content: str, fallback: str) -> str:
    match = re.search(r"^\s*#\s+(.+)$", content, flags=re.MULTILINE)
    if match:
        title = match.group(1).strip()
        if title:
            return title
    return fallback


def resolve_doc_reference(conn: sqlite3.Connection, ref: str) -> str | None:
    lookup = ref.strip()
    if not lookup:
        return None

    row = conn.execute(
        "SELECT slug FROM docs WHERE title = ? COLLATE NOCASE",
        (lookup,),
    ).fetchone()
    if row:
        return row["slug"]

    row = conn.execute(
        "SELECT slug FROM docs WHERE slug = ? COLLATE NOCASE",
        (slugify(lookup),),
    ).fetchone()
    if row:
        return row["slug"]
    return None


def iter_sqlite_chunks(values: list[str], size: int = SQLITE_IN_CLAUSE_CHUNK_SIZE):
    for index in range(0, len(values), size):
        yield values[index : index + size]


def bulk_resolve_doc_references(conn: sqlite3.Connection, refs: list[str]) -> dict[str, str | None]:
    lookups: list[str] = []
    lookup_keys: list[str] = []
    seen: set[str] = set()
    for ref in refs:
        lookup = ref.strip()
        if not lookup:
            continue
        key = lookup.casefold()
        if key in seen:
            continue
        seen.add(key)
        lookups.append(lookup)
        lookup_keys.append(key)

    if not lookups:
        return {}

    resolved: dict[str, str | None] = {key: None for key in lookup_keys}
    for chunk in iter_sqlite_chunks(lookups):
        placeholders = ",".join("?" for _ in chunk)
        rows = conn.execute(
            f"SELECT title, slug FROM docs WHERE title COLLATE NOCASE IN ({placeholders})",
            chunk,
        ).fetchall()
        for row in rows:
            resolved[str(row["title"]).strip().casefold()] = str(row["slug"])

    slug_to_keys: defaultdict[str, list[str]] = defaultdict(list)
    for lookup, key in zip(lookups, lookup_keys):
        if resolved.get(key):
            continue
        slug_to_keys[slugify(lookup).casefold()].append(key)

    slug_lookups = list(slug_to_keys.keys())
    for chunk in iter_sqlite_chunks(slug_lookups):
        placeholders = ",".join("?" for _ in chunk)
        rows = conn.execute(
            f"SELECT slug FROM docs WHERE slug COLLATE NOCASE IN ({placeholders})",
            chunk,
        ).fetchall()
        for row in rows:
            slug = str(row["slug"])
            for key in slug_to_keys.get(slug.casefold(), []):
                resolved[key] = slug

    return resolved


def render_markdown(conn: sqlite3.Connection, text: str) -> Markup:
    wiki_refs, template_refs = extract_reference_targets(text)
    reference_cache = bulk_resolve_doc_references(conn, [*wiki_refs, *template_refs])

    def resolve_cached(ref: str) -> str | None:
        key = ref.strip().casefold()
        if key not in reference_cache:
            reference_cache[key] = resolve_doc_reference(conn, ref)
        return reference_cache[key]

    html = markdown_engine.render(
        text,
        resolve_doc_reference=resolve_cached,
        read_document=read_document_if_exists,
    )
    return Markup(html)


def normalize_search_query(query: str) -> str:
    query = re.sub(
        r"\b(and|or|not)\b",
        lambda m: m.group(1).upper(),
        query,
        flags=re.IGNORECASE,
    )
    return " ".join(query.split())


def extract_tag_search_terms(query: str) -> list[str]:
    terms: list[str] = []
    seen: set[str] = set()
    for raw in re.split(r"\s+", query):
        token = raw.strip().strip("\"'()")
        token = token.lstrip("#")
        if not token:
            continue
        if token.casefold() in {"and", "or", "not"}:
            continue
        key = token.casefold()
        if key in seen:
            continue
        seen.add(key)
        terms.append(token)
    return terms


def search_docs_by_tags(conn: sqlite3.Connection, terms: list[str], *, limit: int = 200) -> list[dict]:
    if not terms:
        return []

    patterns = [f"%{term}%" for term in terms if term]
    if not patterns:
        return []

    where_clause = " OR ".join("t.name LIKE ? COLLATE NOCASE" for _ in patterns)
    rows = conn.execute(
        f"""
        SELECT
            d.id AS doc_id,
            d.title,
            d.slug,
            d.updated_at,
            GROUP_CONCAT(DISTINCT t.name) AS matched_tags_csv
        FROM docs d
        JOIN doc_tags dt ON dt.doc_id = d.id
        JOIN tags t ON t.id = dt.tag_id
        WHERE {where_clause}
        GROUP BY d.id, d.title, d.slug, d.updated_at
        ORDER BY d.updated_at DESC, d.title COLLATE NOCASE
        LIMIT ?
        """,
        [*patterns, limit],
    ).fetchall()

    hits: list[dict] = []
    for row in rows:
        matched_tags = parse_tags(str(row["matched_tags_csv"] or ""))
        hits.append(
            {
                "doc_id": int(row["doc_id"]),
                "title": str(row["title"]),
                "slug": str(row["slug"]),
                "matched_tags": matched_tags,
            }
        )
    return hits


@lru_cache(maxsize=8192)
def singularize_token(token: str) -> str:
    if not token.isascii() or not token.isalpha():
        return token
    if token.endswith("ies") and len(token) > 4:
        return token[:-3] + "y"
    if token.endswith(("sses", "xes", "zes", "ches", "shes")) and len(token) > 4:
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

    rows = conn.execute("SELECT id, title, slug FROM docs").fetchall()
    if not rows:
        with _TAG_RECOMMEND_CACHE_LOCK:
            _TAG_RECOMMEND_CACHE["signature"] = signature
            _TAG_RECOMMEND_CACHE["corpus"] = []
            _TAG_RECOMMEND_CACHE["df_counter"] = Counter()
            _TAG_RECOMMEND_CACHE["total_docs"] = 0
        return []

    doc_meta_by_id = {int(row["id"]): row for row in rows}
    doc_ids = list(doc_meta_by_id.keys())
    tag_map = build_doc_tag_map(conn, doc_ids)
    content_cutoff = get_tag_recommend_corpus_content_cutoff(len(rows))
    seen_fts_doc_ids: set[int] = set()
    corpus: list[dict[str, object]] = []

    def append_corpus_doc(doc_id: int, content: str) -> None:
        row = doc_meta_by_id.get(doc_id)
        if row is None:
            return
        doc_tokens = tokenize_text(f"{row['title']}\n{content}")
        if not doc_tokens:
            return
        tf_counter = Counter(doc_tokens)
        corpus.append(
            {
                "doc_id": doc_id,
                "slug": str(row["slug"]),
                "title": str(row["title"]),
                "tags": tag_map.get(doc_id, []),
                "tf_counter": tf_counter,
                "token_set": set(tf_counter.keys()),
            }
        )

    if content_cutoff is None:
        fts_cursor = fts_conn.execute("SELECT rowid AS doc_id, content FROM docs_fts")
    else:
        fts_cursor = fts_conn.execute(
            "SELECT rowid AS doc_id, substr(content, 1, ?) AS content FROM docs_fts",
            (content_cutoff,),
        )

    for fts_row in fts_cursor:
        doc_id = int(fts_row["doc_id"])
        seen_fts_doc_ids.add(doc_id)
        append_corpus_doc(doc_id, str(fts_row["content"] or ""))

    for doc_id in doc_ids:
        if doc_id not in seen_fts_doc_ids:
            append_corpus_doc(doc_id, "")

    df_counter: Counter[str] = Counter()
    for doc in corpus:
        for token in doc["token_set"]:
            df_counter[token] += 1

    total_docs = len(corpus)
    for doc in corpus:
        doc_vec = build_tfidf_vector(doc["tf_counter"], df_counter, total_docs)
        doc["tfidf_vector"] = doc_vec
        doc["tfidf_norm"] = vector_norm(doc_vec)
        doc.pop("tf_counter", None)
        doc.pop("token_set", None)

    with _TAG_RECOMMEND_CACHE_LOCK:
        _TAG_RECOMMEND_CACHE["signature"] = signature
        _TAG_RECOMMEND_CACHE["corpus"] = corpus
        _TAG_RECOMMEND_CACHE["df_counter"] = df_counter
        _TAG_RECOMMEND_CACHE["total_docs"] = total_docs
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


def find_backlinks(conn: sqlite3.Connection, target_slug: str) -> list[dict]:
    target_row = conn.execute("SELECT title, slug FROM docs WHERE slug = ?", (target_slug,)).fetchone()
    if target_row is None:
        return []

    title_key = reference_title_key(target_row["title"])
    slug_key = str(target_row["slug"])

    rows = conn.execute(
        """
        WITH matched_refs AS (
            SELECT source_doc_id, ref_type
            FROM doc_references
            WHERE target_title_key = ?
            UNION ALL
            SELECT source_doc_id, ref_type
            FROM doc_references
            WHERE target_slug_key = ?
        )
        SELECT
            d.id,
            d.title,
            d.slug,
            d.updated_at,
            MAX(CASE WHEN r.ref_type = 'link' THEN 1 ELSE 0 END) AS has_link,
            MAX(CASE WHEN r.ref_type = 'template' THEN 1 ELSE 0 END) AS has_template
        FROM matched_refs r
        JOIN docs d ON d.id = r.source_doc_id
        WHERE d.slug != ?
        GROUP BY d.id, d.title, d.slug, d.updated_at
        ORDER BY d.updated_at DESC, d.title COLLATE NOCASE
        """,
        (title_key, slug_key, target_slug),
    ).fetchall()

    backlinks: list[dict] = []
    for row in rows:
        reasons: list[str] = []
        if row["has_link"]:
            reasons.append("link")
        if row["has_template"]:
            reasons.append("template")

        item = dict(row)
        item.pop("has_link", None)
        item.pop("has_template", None)
        item["reasons"] = reasons
        backlinks.append(item)
    return backlinks


def iso_from_timestamp(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp).isoformat(timespec="seconds")


def timestamp_from_iso(value: str | None) -> float:
    if not value:
        return 0.0
    try:
        return datetime.fromisoformat(str(value)).timestamp()
    except ValueError:
        return 0.0


def build_markdown_snapshot() -> dict[str, dict[str, object]]:
    snapshot: dict[str, dict[str, object]] = {}
    for md_file in sorted(DOC_DIR.glob("*.md")):
        try:
            mtime = float(md_file.stat().st_mtime)
        except OSError:
            continue
        snapshot[md_file.stem] = {
            "path": md_file,
            "mtime": mtime,
        }
    return snapshot


def build_db_snapshot(conn: sqlite3.Connection) -> dict[str, sqlite3.Row]:
    rows = conn.execute("SELECT id, slug, title, created_at, updated_at, meta_json FROM docs").fetchall()
    return {row["slug"]: row for row in rows}


def detect_incremental_changes(
    md_snapshot: dict[str, dict[str, object]],
    db_snapshot: dict[str, sqlite3.Row],
) -> tuple[list[str], list[str], list[str]]:
    md_slugs = set(md_snapshot.keys())
    db_slugs = set(db_snapshot.keys())

    new_slugs = sorted(md_slugs - db_slugs)
    deleted_slugs = sorted(db_slugs - md_slugs)

    modified_slugs: list[str] = []
    for slug in sorted(md_slugs & db_slugs):
        md_mtime = float(md_snapshot[slug]["mtime"])
        db_updated_ts = timestamp_from_iso(str(db_snapshot[slug]["updated_at"]))
        if md_mtime > (db_updated_ts + DIRTY_MTIME_GRACE_SECONDS):
            modified_slugs.append(slug)
    return new_slugs, deleted_slugs, modified_slugs


def is_severe_divergence(*, md_count: int, db_count: int, total_changes: int) -> bool:
    if total_changes < SEVERE_DIFF_MIN_CHANGES:
        return False
    baseline = max(md_count, db_count, 1)
    return (total_changes / baseline) >= SEVERE_DIFF_RATIO


def sync_new_doc(
    conn: sqlite3.Connection,
    fts_conn: sqlite3.Connection,
    *,
    slug: str,
    md_file: Path,
    mtime: float,
) -> None:
    content = read_text_normalized(md_file)
    references = extract_reference_payload(content)
    sidecar = load_sidecar(slug)

    sidecar_title = str(sidecar.get("title", "")).strip()
    title_candidate = sidecar_title or infer_title_from_content(content, slug) or slug
    title = ensure_unique_title(conn, title_candidate)

    created_at_raw = str(sidecar.get("created_at") or "").strip()
    created_at = created_at_raw or iso_from_timestamp(mtime)
    updated_at = iso_from_timestamp(mtime)

    meta_value = sidecar.get("meta")
    meta = meta_value if isinstance(meta_value, dict) else {}
    meta["sidecar"] = f"json/{slug}.json"

    conn.execute(
        """
        INSERT INTO docs (title, slug, file_path, meta_json, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            title,
            slug,
            str(md_file),
            json.dumps(meta, ensure_ascii=False),
            created_at,
            updated_at,
        ),
    )
    doc_row = conn.execute("SELECT id FROM docs WHERE slug = ?", (slug,)).fetchone()
    if doc_row is None:
        raise RuntimeError(f"failed to create doc row for slug '{slug}'")
    doc_id = int(doc_row["id"])

    tags = collect_sidecar_tags(sidecar)

    set_doc_tags(conn, doc_id, tags)
    set_doc_references(conn, doc_id, references)
    update_fts(fts_conn, doc_id, title, content)
    write_sidecar(
        slug=slug,
        title=title,
        created_at=created_at,
        updated_at=updated_at,
        tags=tags,
        meta=meta,
        references=references,
    )


def sync_modified_doc(
    conn: sqlite3.Connection,
    fts_conn: sqlite3.Connection,
    *,
    row: sqlite3.Row,
    md_file: Path,
    mtime: float,
) -> None:
    slug = str(row["slug"])
    doc_id = int(row["id"])
    content = read_text_normalized(md_file)
    references = extract_reference_payload(content)

    current_title = str(row["title"] or "").strip()
    if not current_title:
        inferred = infer_title_from_content(content, slug) or slug
        current_title = ensure_unique_title(conn, inferred, exclude_doc_id=doc_id)

    meta = safe_load_json(row["meta_json"])
    meta["sidecar"] = f"json/{slug}.json"
    updated_at = iso_from_timestamp(mtime)

    conn.execute(
        """
        UPDATE docs
        SET title = ?, file_path = ?, meta_json = ?, updated_at = ?
        WHERE id = ?
        """,
        (
            current_title,
            str(md_file),
            json.dumps(meta, ensure_ascii=False),
            updated_at,
            doc_id,
        ),
    )
    tags = list_doc_tags(conn, doc_id)
    set_doc_references(conn, doc_id, references)
    update_fts(fts_conn, doc_id, current_title, content)
    write_sidecar(
        slug=slug,
        title=current_title,
        created_at=row["created_at"],
        updated_at=updated_at,
        tags=tags,
        meta=meta,
        references=references,
    )


def sync_deleted_doc(conn: sqlite3.Connection, fts_conn: sqlite3.Connection, *, row: sqlite3.Row) -> None:
    doc_id = int(row["id"])
    fts_conn.execute("DELETE FROM docs_fts WHERE rowid = ?", (doc_id,))
    conn.execute("DELETE FROM docs WHERE id = ?", (doc_id,))


def repair_fts_mismatch(conn: sqlite3.Connection, fts_conn: sqlite3.Connection) -> tuple[int, int]:
    doc_rows = conn.execute("SELECT id, title, slug FROM docs").fetchall()
    fts_rows = fts_conn.execute("SELECT rowid FROM docs_fts").fetchall()

    docs_by_id = {int(row["id"]): row for row in doc_rows}
    doc_ids = set(docs_by_id.keys())
    fts_ids = {int(row["rowid"]) for row in fts_rows}

    missing_ids = sorted(doc_ids - fts_ids)
    orphan_ids = sorted(fts_ids - doc_ids)

    for doc_id in missing_ids:
        row = docs_by_id[doc_id]
        content = read_document_if_exists(str(row["slug"])) or ""
        update_fts(fts_conn, doc_id, str(row["title"]), content)

    for doc_id in orphan_ids:
        fts_conn.execute("DELETE FROM docs_fts WHERE rowid = ?", (doc_id,))

    return len(missing_ids), len(orphan_ids)


def sync_documents_incremental() -> dict[str, int]:
    with closing(connect_db()) as conn, closing(connect_fts_db()) as fts_conn:
        md_snapshot = build_markdown_snapshot()
        db_snapshot = build_db_snapshot(conn)
        new_slugs, deleted_slugs, modified_slugs = detect_incremental_changes(md_snapshot, db_snapshot)

        total_changes = len(new_slugs) + len(deleted_slugs) + len(modified_slugs)
        if len(md_snapshot) != len(db_snapshot):
            print(f"[SYNC] count mismatch detected md={len(md_snapshot)} db={len(db_snapshot)}")
        if is_severe_divergence(
            md_count=len(md_snapshot),
            db_count=len(db_snapshot),
            total_changes=total_changes,
        ):
            raise StartupRecoveryNeeded(
                "severe startup divergence detected "
                f"(md={len(md_snapshot)}, db={len(db_snapshot)}, changes={total_changes})"
            )

        for slug in new_slugs:
            md_file = md_snapshot[slug]["path"]
            mtime = float(md_snapshot[slug]["mtime"])
            sync_new_doc(conn, fts_conn, slug=slug, md_file=md_file, mtime=mtime)

        for slug in modified_slugs:
            md_file = md_snapshot[slug]["path"]
            mtime = float(md_snapshot[slug]["mtime"])
            row = db_snapshot[slug]
            sync_modified_doc(conn, fts_conn, row=row, md_file=md_file, mtime=mtime)

        for slug in deleted_slugs:
            sync_deleted_doc(conn, fts_conn, row=db_snapshot[slug])

        fts_missing, fts_orphan = repair_fts_mismatch(conn, fts_conn)

        conn.commit()
        fts_conn.commit()
        if total_changes > 0 or fts_missing > 0 or fts_orphan > 0:
            invalidate_tag_recommendation_cache()

        print(
            "[SYNC] startup incremental sync "
            f"new={len(new_slugs)} deleted={len(deleted_slugs)} modified={len(modified_slugs)} "
            f"fts_missing={fts_missing} fts_orphan={fts_orphan}"
        )
        return {
            "new": len(new_slugs),
            "deleted": len(deleted_slugs),
            "modified": len(modified_slugs),
            "fts_missing": fts_missing,
            "fts_orphan": fts_orphan,
        }


def resolve_db_fix_command() -> list[str] | None:
    if getattr(sys, "frozen", False):
        exe_path = Path(sys.executable).resolve().parent / "PersonalWikiDBFix.exe"
        if exe_path.exists():
            return [str(exe_path)]
        return None

    script_path = DATA_DIR / "personal_wiki_db_fix.py"
    if script_path.exists():
        return [sys.executable, str(script_path)]

    exe_path = DATA_DIR / "PersonalWikiDBFix.exe"
    if exe_path.exists():
        return [str(exe_path)]
    return None


def run_db_fix_tool(reason: str) -> bool:
    command = resolve_db_fix_command()
    if command is None:
        print(f"[ERROR] DB fix tool not found. reason={reason}")
        return False

    print(f"[WARN] Running PersonalWikiDBFix due to startup issue: {reason}")
    try:
        completed = subprocess.run(
            command,
            cwd=str(DATA_DIR),
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError as error:
        print(f"[ERROR] Failed to launch PersonalWikiDBFix: {error}")
        return False

    if completed.stdout:
        print(completed.stdout.rstrip())
    if completed.stderr:
        print(completed.stderr.rstrip())

    if completed.returncode != 0:
        print(f"[ERROR] PersonalWikiDBFix failed with exit code {completed.returncode}")
        return False
    return True


def sync_documents_on_startup() -> None:
    try:
        sync_documents_incremental()
        return
    except StartupRecoveryNeeded as error:
        reason = str(error)
    except sqlite3.DatabaseError as error:
        reason = f"database error: {error}"

    release_data_lock()
    try:
        db_fix_ok = run_db_fix_tool(reason)
    finally:
        acquire_data_lock()

    if not db_fix_ok:
        raise RuntimeError(f"startup sync failed and DB fix did not complete: {reason}")
    print("[SYNC] Startup recovery completed by PersonalWikiDBFix.")


def ensure_default_home() -> None:
    with closing(connect_db()) as conn, closing(connect_fts_db()) as fts_conn:
        count = conn.execute("SELECT COUNT(*) AS c FROM docs").fetchone()["c"]
        if count > 0:
            return

        title = "Home"
        slug = "home"
        content = """# Home

개인 위키에 오신 것을 환영합니다.

## 기본 문법

- 문서 링크: `[[문서명]]` 또는 `[[문서명|표시 텍스트]]`
- 이미지 삽입: `![[sample.png]]`
- 하이라이트: `==강조==`
- 스포일러: `||숨김 텍스트||`
- 유튜브 임베드: `![[youtube(HhnETSN6U_E)]]`
- 템플릿 포함: `{{공통문서}}` (중첩 템플릿은 확장되지 않음)
- 콜아웃: `!!! note 내용`, `!!! info 내용`, `!!! warn 내용`, `!!! danger 내용`

## 검색
검색창에서 `AND`, `OR`, `NOT` 연산자를 지원합니다.
예: `flask AND sqlite`, `python NOT django`
"""
        now = now_iso()
        meta = {"sidecar": f"json/{slug}.json"}
        conn.execute(
            """
            INSERT INTO docs (title, slug, file_path, meta_json, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                title,
                slug,
                str(document_path(slug)),
                json.dumps(meta, ensure_ascii=False),
                now,
                now,
            ),
        )
        doc_id = conn.execute("SELECT id FROM docs WHERE slug = ?", (slug,)).fetchone()["id"]
        tags = ["guide", "start"]
        references = extract_reference_payload(content)
        write_document(slug, content)
        set_doc_tags(conn, doc_id, tags)
        set_doc_references(conn, doc_id, references)
        update_fts(fts_conn, doc_id, title, content)
        write_sidecar(
            slug=slug,
            title=title,
            created_at=now,
            updated_at=now,
            tags=tags,
            meta=meta,
            references=references,
        )
        conn.commit()
        fts_conn.commit()
        invalidate_tag_recommendation_cache()


def fetch_doc_with_tags(conn: sqlite3.Connection, slug: str) -> dict | None:
    row = conn.execute("SELECT * FROM docs WHERE slug = ?", (slug,)).fetchone()
    if row is None:
        return None
    data = dict(row)
    data["tags"] = list_doc_tags(conn, row["id"])
    data["meta"] = safe_load_json(row["meta_json"])
    return data


def fetch_docs_for_index(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        """
        SELECT id, title, slug, created_at, updated_at
        FROM docs
        ORDER BY updated_at DESC, title COLLATE NOCASE
        LIMIT 100
        """
    ).fetchall()
    tag_map = build_doc_tag_map(conn, [int(row["id"]) for row in rows])
    docs: list[dict] = []
    for row in rows:
        item = dict(row)
        item["tags"] = tag_map.get(int(row["id"]), [])
        docs.append(item)
    return docs


@app.template_filter("pretty_time")
def pretty_time_filter(value: str) -> str:
    try:
        dt = datetime.fromisoformat(value)
        return dt.strftime("%Y-%m-%d %H:%M")
    except ValueError:
        return value


@app.route("/")
def index():
    conn = get_db()
    docs = fetch_docs_for_index(conn)
    return render_template("index.html", docs=docs)


@app.route("/doc/<path:slug>")
def view_doc(slug: str):
    conn = get_db()
    doc = fetch_doc_with_tags(conn, slug)
    if doc is None:
        return render_template("not_found.html", requested=slug), 404

    content = read_document(doc["slug"])
    rendered = render_markdown(conn, content)
    backlinks = find_backlinks(conn, doc["slug"])
    return render_template(
        "view.html",
        doc=doc,
        content=content,
        rendered=rendered,
        backlinks=backlinks,
    )


@app.route("/new", methods=["GET", "POST"])
def new_doc():
    conn = get_db()
    fts_conn = get_fts_db()

    if request.method == "POST":
        title = request.form.get("title", "").strip()
        content = normalize_newlines(request.form.get("content", ""))
        tags = parse_tags(request.form.get("tags", ""))
        ignore_tag_warning = request.form.get("ignore_tag_warning") == "1"
        spellcheck_action = request.form.get("spellcheck_action", "")
        if spellcheck_action == "auto_fix":
            title, content = apply_korean_spell_autofix(title, content)

        def suggested_tags_for_form() -> list[str]:
            return recommend_tags(
                conn,
                fts_conn,
                title=title,
                content=content,
                exclude_tags=tags,
                limit=TAG_RECOMMEND_LIMIT,
            )

        title_warning = title_prefix_warning(title)

        if not title:
            return render_edit_form(
                mode="new",
                doc={"title": "", "slug": ""},
                content=content,
                tags_text=", ".join(tags),
                error="문서 제목을 입력해 주세요.",
                tag_warning=None,
                title_warning=title_warning,
                show_ignore_tag_warning=False,
                recommended_tags=suggested_tags_for_form(),
            )

        duplicate = conn.execute(
            "SELECT 1 FROM docs WHERE title = ? COLLATE NOCASE",
            (title,),
        ).fetchone()
        if duplicate:
            return render_edit_form(
                mode="new",
                doc={"title": title, "slug": ""},
                content=content,
                tags_text=", ".join(tags),
                error="같은 제목의 문서가 이미 있습니다.",
                tag_warning=None,
                title_warning=title_warning,
                show_ignore_tag_warning=False,
                recommended_tags=suggested_tags_for_form(),
            )

        if len(tags) < 2 and not ignore_tag_warning:
            return render_edit_form(
                mode="new",
                doc={"title": title, "slug": ""},
                content=content,
                tags_text=", ".join(tags),
                error=None,
                tag_warning="태그를 2개 이상 등록하면 나중에 검색이 더 쉬워집니다. 계속 생성하려면 아래 버튼을 눌러 주세요.",
                title_warning=title_warning,
                show_ignore_tag_warning=True,
                recommended_tags=suggested_tags_for_form(),
            )

        if spellcheck_action not in {"auto_fix", "save_as_is"}:
            spell_issues = collect_korean_spell_issues(title, content)
            if spell_issues:
                return render_edit_form(
                    mode="new",
                    doc={"title": title, "slug": ""},
                    content=content,
                    tags_text=", ".join(tags),
                    error=None,
                    tag_warning=None,
                    title_warning=title_warning,
                    show_ignore_tag_warning=False,
                    ignore_tag_warning=ignore_tag_warning,
                    spell_warning=korean_spell_warning_message(spell_issues["count"]),
                    show_spellcheck_warning=True,
                    spellcheck_samples=spell_issues["samples"],
                    recommended_tags=suggested_tags_for_form(),
                )

        slug = ensure_unique_slug(conn, slugify(title))
        created_at = now_iso()
        meta = {"sidecar": f"json/{slug}.json"}
        try:
            conn.execute(
                """
                INSERT INTO docs (title, slug, file_path, meta_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    title,
                    slug,
                    str(document_path(slug)),
                    json.dumps(meta, ensure_ascii=False),
                    created_at,
                    created_at,
                ),
            )
            doc_id = conn.execute("SELECT id FROM docs WHERE slug = ?", (slug,)).fetchone()["id"]

            references = extract_reference_payload(content)
            write_document(slug, content)
            set_doc_tags(conn, doc_id, tags)
            set_doc_references(conn, doc_id, references)
            update_fts(fts_conn, doc_id, title, content)
            write_sidecar(
                slug=slug,
                title=title,
                created_at=created_at,
                updated_at=created_at,
                tags=tags,
                meta=meta,
                references=references,
            )
            conn.commit()
            fts_conn.commit()
            invalidate_tag_recommendation_cache()
        except Exception:
            conn.rollback()
            fts_conn.rollback()
            raise
        return redirect(url_for("view_doc", slug=slug))

    prefilled_title = request.args.get("title", "").strip()
    return render_edit_form(
        mode="new",
        doc={"title": prefilled_title, "slug": ""},
        content="",
        tags_text="",
        error=None,
        tag_warning=None,
        title_warning=title_prefix_warning(prefilled_title),
        show_ignore_tag_warning=False,
        recommended_tags=[],
    )


@app.route("/edit/<path:slug>", methods=["GET", "POST"])
def edit_doc(slug: str):
    conn = get_db()
    fts_conn = get_fts_db()
    row = conn.execute("SELECT * FROM docs WHERE slug = ?", (slug,)).fetchone()
    if row is None:
        abort(404)

    doc = dict(row)
    tags = list_doc_tags(conn, row["id"])
    current_content = read_document(row["slug"])

    if request.method == "POST":
        new_title = request.form.get("title", "").strip()
        new_content = normalize_newlines(request.form.get("content", ""))
        new_tags = parse_tags(request.form.get("tags", ""))
        spellcheck_action = request.form.get("spellcheck_action", "")
        if spellcheck_action == "auto_fix":
            new_title, new_content = apply_korean_spell_autofix(new_title, new_content)

        def suggested_tags_for_form() -> list[str]:
            return recommend_tags(
                conn,
                fts_conn,
                title=new_title or row["title"],
                content=new_content,
                current_slug=row["slug"],
                exclude_tags=new_tags,
                limit=TAG_RECOMMEND_LIMIT,
            )

        title_warning = title_prefix_warning(new_title)

        if not new_title:
            return render_edit_form(
                mode="edit",
                doc=doc,
                content=new_content,
                tags_text=", ".join(new_tags),
                error="문서 제목을 입력해 주세요.",
                tag_warning=None,
                title_warning=title_warning,
                show_ignore_tag_warning=False,
                recommended_tags=suggested_tags_for_form(),
            )

        duplicate = conn.execute(
            "SELECT id FROM docs WHERE title = ? COLLATE NOCASE AND id != ?",
            (new_title, row["id"]),
        ).fetchone()
        if duplicate:
            return render_edit_form(
                mode="edit",
                doc=doc,
                content=new_content,
                tags_text=", ".join(new_tags),
                error="같은 제목의 문서가 이미 있습니다.",
                tag_warning=None,
                title_warning=title_warning,
                show_ignore_tag_warning=False,
                recommended_tags=suggested_tags_for_form(),
            )

        if spellcheck_action not in {"auto_fix", "save_as_is"}:
            spell_issues = collect_korean_spell_issues(new_title, new_content)
            if spell_issues:
                return render_edit_form(
                    mode="edit",
                    doc=doc,
                    content=new_content,
                    tags_text=", ".join(new_tags),
                    error=None,
                    tag_warning=None,
                    title_warning=title_warning,
                    show_ignore_tag_warning=False,
                    spell_warning=korean_spell_warning_message(spell_issues["count"]),
                    show_spellcheck_warning=True,
                    spellcheck_samples=spell_issues["samples"],
                    recommended_tags=suggested_tags_for_form(),
                )

        new_slug_candidate = slugify(new_title)
        new_slug = ensure_unique_slug(conn, new_slug_candidate, exclude_doc_id=row["id"])
        old_slug = row["slug"]

        updated_at = now_iso()
        meta = safe_load_json(row["meta_json"])
        meta["sidecar"] = f"json/{new_slug}.json"
        moved_assets: list[tuple[Path, Path]] = []
        try:
            if new_slug != old_slug:
                moved_assets = move_document_assets_for_slug_change(old_slug, new_slug)
            conn.execute(
                """
                UPDATE docs
                SET title = ?, slug = ?, file_path = ?, meta_json = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    new_title,
                    new_slug,
                    str(document_path(new_slug)),
                    json.dumps(meta, ensure_ascii=False),
                    updated_at,
                    row["id"],
                ),
            )
            references = extract_reference_payload(new_content)
            write_document(new_slug, new_content)
            set_doc_tags(conn, row["id"], new_tags)
            set_doc_references(conn, row["id"], references)
            update_fts(fts_conn, row["id"], new_title, new_content)
            write_sidecar(
                slug=new_slug,
                title=new_title,
                created_at=row["created_at"],
                updated_at=updated_at,
                tags=new_tags,
                meta=meta,
                references=references,
            )
            conn.commit()
            fts_conn.commit()
            invalidate_tag_recommendation_cache()
        except Exception:
            conn.rollback()
            fts_conn.rollback()
            rollback_document_asset_moves(moved_assets)
            raise
        return redirect(url_for("view_doc", slug=new_slug))

    doc["tags"] = tags
    return render_edit_form(
        mode="edit",
        doc=doc,
        content=current_content,
        tags_text=", ".join(tags),
        error=None,
        tag_warning=None,
        title_warning=title_prefix_warning(doc["title"]),
        show_ignore_tag_warning=False,
        recommended_tags=recommend_tags(
            conn,
            fts_conn,
            title=doc["title"],
            content=current_content,
            current_slug=doc["slug"],
            exclude_tags=tags,
            limit=TAG_RECOMMEND_LIMIT,
        ),
    )


@app.post("/delete/<path:slug>")
def delete_doc(slug: str):
    conn = get_db()
    fts_conn = get_fts_db()
    row = conn.execute("SELECT * FROM docs WHERE slug = ?", (slug,)).fetchone()
    if row is None:
        abort(404)

    try:
        fts_conn.execute("DELETE FROM docs_fts WHERE rowid = ?", (row["id"],))
        conn.execute("DELETE FROM docs WHERE id = ?", (row["id"],))
        conn.commit()
        fts_conn.commit()
        invalidate_tag_recommendation_cache()
    except Exception:
        conn.rollback()
        fts_conn.rollback()
        raise

    md = document_path(slug)
    side = sidecar_path(slug)
    if md.exists():
        md.unlink()
    if side.exists():
        side.unlink()

    return redirect(url_for("index"))


@app.route("/tag/<path:tag_name>")
def docs_by_tag(tag_name: str):
    conn = get_db()
    rows = conn.execute(
        """
        SELECT d.id, d.title, d.slug, d.updated_at
        FROM docs d
        JOIN doc_tags dt ON dt.doc_id = d.id
        JOIN tags t ON t.id = dt.tag_id
        WHERE t.name = ? COLLATE NOCASE
        ORDER BY d.updated_at DESC, d.title COLLATE NOCASE
        """,
        (tag_name,),
    ).fetchall()
    docs = [dict(row) for row in rows]
    return render_template("tag.html", tag_name=tag_name, docs=docs)


@app.route("/search")
def search():
    conn = get_db()
    fts_conn = get_fts_db()
    query = request.args.get("q", "").strip()
    results: list[dict] = []
    error: str | None = None

    if query:
        merged_by_doc_id: dict[int, dict] = {}
        ordered_doc_ids: list[int] = []
        normalized = normalize_search_query(query)

        try:
            fts_rows = fts_conn.execute(
                """
                SELECT
                    rowid AS doc_id,
                    snippet(docs_fts, 1, '<mark>', '</mark>', ' ... ', 24) AS excerpt
                FROM docs_fts
                WHERE docs_fts MATCH ?
                ORDER BY bm25(docs_fts)
                LIMIT 200
                """,
                (normalized,),
            ).fetchall()

            doc_ids = [int(row["doc_id"]) for row in fts_rows]
            docs_by_id: dict[int, sqlite3.Row] = {}
            if doc_ids:
                placeholders = ",".join("?" for _ in doc_ids)
                meta_rows = conn.execute(
                    f"SELECT id, title, slug, updated_at FROM docs WHERE id IN ({placeholders})",
                    doc_ids,
                ).fetchall()
                docs_by_id = {int(row["id"]): row for row in meta_rows}

            for row in fts_rows:
                doc_id = int(row["doc_id"])
                doc_meta = docs_by_id.get(doc_id)
                if not doc_meta:
                    continue
                merged_by_doc_id[doc_id] = {
                    "title": doc_meta["title"],
                    "slug": doc_meta["slug"],
                    "excerpt": row["excerpt"],
                    "matched_tags": [],
                }
                ordered_doc_ids.append(doc_id)
        except sqlite3.OperationalError:
            error = "검색식이 올바르지 않습니다. 예: flask AND sqlite, python NOT django"

        tag_terms = extract_tag_search_terms(query)
        for hit in search_docs_by_tags(conn, tag_terms, limit=200):
            doc_id = int(hit["doc_id"])
            if doc_id in merged_by_doc_id:
                existing = merged_by_doc_id[doc_id]
                existing_tags = [str(tag) for tag in existing.get("matched_tags", [])]
                existing["matched_tags"] = parse_tags(",".join([*existing_tags, *hit["matched_tags"]]))
                continue

            merged_by_doc_id[doc_id] = {
                "title": hit["title"],
                "slug": hit["slug"],
                "excerpt": "",
                "matched_tags": hit["matched_tags"],
            }
            ordered_doc_ids.append(doc_id)

        results = [merged_by_doc_id[doc_id] for doc_id in ordered_doc_ids]

    return render_template(
        "search.html",
        query=query,
        results=results,
        error=error,
    )


@app.post("/preview")
def preview():
    conn = get_db()
    payload = request.get_json(silent=True) or {}
    content = normalize_newlines(str(payload.get("content", "")))
    html = render_markdown(conn, content)
    return jsonify({"html": str(html)})


@app.post("/api/tag-suggestions")
def tag_suggestions():
    conn = get_db()
    fts_conn = get_fts_db()
    payload = request.get_json(silent=True) or {}

    title = str(payload.get("title", "")).strip()
    content = normalize_newlines(str(payload.get("content", "")))
    current_slug = str(payload.get("slug", "")).strip() or None

    raw_tags = payload.get("tags", "")
    if isinstance(raw_tags, list):
        tags = parse_tags(",".join(str(item) for item in raw_tags))
    else:
        tags = parse_tags(str(raw_tags))

    suggestions = recommend_tags(
        conn,
        fts_conn,
        title=title,
        content=content,
        current_slug=current_slug,
        exclude_tags=tags,
        limit=TAG_RECOMMEND_LIMIT,
    )
    return jsonify({"tags": suggestions})


@app.route("/img/<path:filename>")
def serve_image(filename: str):
    return send_from_directory(IMG_DIR, filename)


@app.route("/file/<path:filename>")
def serve_file(filename: str):
    return send_from_directory(FILE_DIR, filename)


@app.route("/favicon.ico")
def favicon():
    return send_from_directory(IMG_DIR, "icon.ico")


def bootstrap() -> None:
    init_storage()
    acquire_data_lock()
    init_db()
    init_fts_db()
    sync_documents_on_startup()
    ensure_default_home()


bootstrap()


if __name__ == "__main__":
    app.run(host=DEFAULT_HOST, port=DEFAULT_PORT, debug=False)

