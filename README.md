# Personal Wiki (Flask + SQLite + JSON)

개인 PC에서 실행하는 로컬 위키입니다.

## 주요 기능

- Markdown 문서 작성/수정 + 실시간 미리보기
- 위키 링크: `[[문서명]]` 또는 `[[문서명|표시 텍스트]]`
- 이미지 삽입: `![[파일명.png]]`
- 하이라이트: `==강조==`
- 스포일러: `||숨김 텍스트||` (클릭 토글)
- 유튜브 임베드:
  - `![[youtube(HhnETSN6U_E)]]`
  - `![[youtube(HhnETSN6U_E, width=640, height=360)]]`
- 콜아웃:
  - `!!! note 내용` (초록)
  - `!!! info 내용` (파랑)
  - `!!! warn 내용` (주황)
  - `!!! danger 내용` (빨강)
- 템플릿 포함: `{{템플릿문서}}` (한 번만 확장, 중첩 템플릿 미확장)
- 백링크: 현재 문서를 링크/템플릿 참조한 문서 목록
- 태그 경고: 문서 생성 시 태그 2개 미만이면 경고
- 태그 자동추천: 불용어/복수형 정규화 + TF-IDF + 코사인 유사도 기반, 최대 10개

## 저장 구조

- `wiki.db`: 문서 메타/태그
- `wiki_fts.db`: 본문 검색용 SQLite FTS5 전용 DB
- `doc/*.md`: 문서 본문
- `doc/*.json`: 문서 sidecar 메타
- `img/`: 이미지 파일

## 실행

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python app.py
```

브라우저에서 `http://127.0.0.1:6885` 접속

## EXE 빌드 (PyInstaller onedir)

```powershell
.\build.ps1
```

실행 파일:

- `dist/PersonalWiki/PersonalWiki.exe`
- 단일 파일이 아닌 폴더(onedir) 형식
