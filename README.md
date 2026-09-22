# PersonalWiki

간단한 로컬 기반 위키 시스템
Markdown 기반 문서 작성 + 빠른 검색 + 위키 링크 지원

---

## 주요 기능

### 텍스트 스타일

* `*기울임*`
* `**진하게**`
* `~~취소선~~`

### 하이라이트

* `==형광펜==`
* `==**진한글씨 형광펜**==`

### 스포일러

* `||숨김 텍스트||`

### 위키 링크

* `[[문서명]]`
* `[[문서명|표시 텍스트]]`

### 이미지 삽입

* `![[image.png]]`
* `![[image.png, width=640]]`
* `![[image.png, height=360]]`

### 파일 첨부

* `[[file/file.zip]]`

---

### 템플릿

* `{{템플릿}}`
  → 한 번만 확장됨 (중첩 템플릿 미지원)

* 접기 템플릿
  `||{{템플릿}}||`

> ⚠️ 문서 내부 folding은 지원하지 않음
> folding이 필요한 내용은 반드시 별도 문서로 분리 후 템플릿으로 사용

---

### 유튜브 임베드

* `![[youtube(HhnETSN6U_E)]]`
* `![[youtube(HhnETSN6U_E, width=640, height=360)]]`

---

### 태그 문서 목록

* `![[tag(태그이름)]]`

해당 태그를 가진 문서가 링크로 나열됩니다. 각 문서는 ` ・ ` (U+30FB, 양쪽 공백 한 칸)으로 구분되며, 문서 내보내기에도 같은 목록이 포함됩니다.

---

### 인용

```
> 인용
>> 더블인용
```

---

### 콜아웃

* `!!! note 내용` (전구 아이콘)
* `!!! info 내용` (정보 아이콘, #9ed828)
* `!!! warn 내용` (삼각형 느낌표)
* `!!! danger 내용` (빨간 팔각형 느낌표)

---

## 저장 구조

```
wiki.db        # 문서 메타 / 태그 DB
wiki_fts.db    # 본문 검색용 DB

doc/*.md       # 문서 본문
doc/*.json     # 문서 메타 (sidecar)

img/           # 이미지 파일
file/          # 첨부 파일
```

---

## 실행 방법

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python app.py
```

브라우저에서 접속:

```
http://127.0.0.1:6885
```

---

## 설정

실행 중인 위키의 상단 메뉴에서 **설정**을 열어 서버 포트와 웹 브라우저용 글꼴을 변경할 수 있습니다.

* 포트는 `1`~`65535` 범위이며, 저장한 뒤 PersonalWiki를 다시 시작하면 적용됩니다.
* 글꼴은 다음 페이지 새로고침부터 적용됩니다. 목록의 글꼴 또는 설치된 글꼴 이름 하나를 직접 지정할 수 있습니다.
* 설정은 실행 파일 옆의 `wikisettings.cfg`에 저장됩니다. 주석과 다른 사용자 설정 키는 유지됩니다.
* 이 글꼴 설정은 웹 브라우저 화면 전용입니다. PersonalWikiClient는 접속 포트와 글꼴을 자체 설정에서 별도로 관리합니다.

직접 파일을 편집할 때의 예시:

```ini
port=6885
browser_font=malgun-gothic
# 또는 browser_font=custom:맑은 고딕
```

---

## PersonalWikiClient 로컬 API

PersonalWikiClient는 SQLite나 문서 파일을 직접 읽지 않고 PersonalWiki 서버의 JSON API만 사용합니다. `/api/client/*`는 직접 `127.0.0.1` 또는 `::1`에서 온 요청만 받으며, ngrok 같은 역방향 프록시가 붙이는 전달 헤더가 있으면 거부합니다. 이 API는 터널·외부 브라우저용이 아닙니다. 프록시를 별도로 구성한다면 `/api/client/*`를 라우팅하지 말고 전달 헤더를 제거하는 구성도 사용하지 마세요. 기존 브라우저 페이지의 ngrok·Tailscale 접속 방식은 그대로 사용할 수 있습니다.

* `GET /api/client/bootstrap`에서 API 버전과 네이티브 Markdown AST 사용 가능 여부를 확인합니다.
* `GET /api/client/documents`는 기본적으로 모든 문서를 반환해 클라이언트가 문서를 조용히 누락하지 않습니다. 큰 위키는 `?limit=1000&offset=0`처럼 페이지를 지정할 수 있고, `limit`은 1~1000이며 응답의 `pagination.total`, `pagination.next_offset`으로 다음 페이지를 요청할 수 있습니다.
* 문서 원본·메타·AST·역링크, 생성·수정·삭제, 태그·검색·태그 추천은 모두 `/api/client/` 아래에서 처리됩니다.
* 글꼴과 접속 포트는 PersonalWikiClient의 자체 설정입니다. 웹 브라우저용 `wikisettings.cfg` 설정을 공유하지 않습니다.

---

## EXE 빌드

```powershell
.\build.ps1
```

빌드할 때만 Python 3(서버)과 .NET 8 SDK(네이티브 클라이언트)가 필요합니다. PATH 대신 실행 파일을 지정할 수도 있습니다.

```powershell
.\build.ps1 -PythonPath C:\path\to\python.exe -DotnetPath C:\path\to\dotnet.exe
```

빌드 결과는 실행 환경에 Python이나 .NET SDK를 요구하지 않습니다. `PersonalWikiClient.exe`는 self-contained 단일 파일이며 WebView2·Chromium 런타임을 사용하지 않습니다.

또는
👉 [https://github.com/tchinso/PersonalWiki/releases/](https://github.com/tchinso/PersonalWiki/releases/) 에서 최신 릴리즈 다운로드

---

## 실행 파일

* `dist/PersonalWiki/PersonalWiki.exe`
* `dist/PersonalWiki/PersonalWikiDBFix.exe`
* `dist/PersonalWikiClient/PersonalWikiClient.exe`

GitHub Release에서 다운로드한 경우
→ `PersonalWiki.exe` 바로 실행. 가벼운 로컬 읽기·편집에는 `PersonalWikiClient.exe`를 실행한 뒤 클라이언트 자체 설정에서 서버 포트를 입력합니다.

---

## 첨부 파일 사용 방법

현재 위키 내부에서 파일 업로드 기능은 없음

따라서:

* 이미지 → `/img/` 폴더에 직접 추가
* 파일 → `/file/` 폴더에 직접 추가

---

[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/tchinso/PersonalWiki)
