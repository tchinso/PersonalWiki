# Personal Wiki (Flask + SQLite + JSON)

개인 용도로 로컬에서 실행하는 위키입니다.

## 요구사항 반영

- Flask 웹 앱
- 폴더 구조
  - `/doc`: 마크다운 문서(`.md`) + 문서 메타데이터(`.json`)
  - `/img`: 이미지 파일
  - `wiki.db`: SQLite 데이터베이스(태그, 검색 인덱스 등)
- 브라우저에서 Markdown 작성/수정 + 실시간 미리보기
- GFM 스타일 렌더링(테이블, 취소선, 태스크리스트, URL 등)
- 위키 링크: `[[문서명]]`, `[[문서명|표시텍스트]]`
- 이미지 단축 문법: `![[파일명.png]]` → `/img/파일명.png`
- 템플릿 포함 문법: `{{문서명}}`
- 태그 관리 + 태그 클릭 페이지
- 제목+본문 검색 + `AND OR NOT` 연산자

## 실행

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python app.py
```

브라우저에서 `http://127.0.0.1:5000` 접속

## EXE 빌드 (PyInstaller onedir)

```powershell
.\build.ps1
```

PowerShell 실행 정책 때문에 막히면:

```powershell
powershell -ExecutionPolicy Bypass -File .\build.ps1
```

빌드 결과:

- `dist/PersonalWiki/PersonalWiki.exe`
- 단일 파일이 아닌 폴더(`onedir`) 형식

## 검색 예시

- `flask AND sqlite`
- `python OR rust`
- `template NOT draft`
