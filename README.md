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

### 인용

```
> 인용
>> 더블인용
```

---

### 콜아웃

* `!!! note 내용` (초록)
* `!!! info 내용` (파랑)
* `!!! warn 내용` (주황)
* `!!! danger 내용` (빨강)

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

## EXE 빌드 (PyInstaller - onedir)

```powershell
.\build.ps1
```

또는
👉 [https://github.com/tchinso/PersonalWiki/releases/](https://github.com/tchinso/PersonalWiki/releases/) 에서 최신 릴리즈 다운로드

---

## 실행 파일

* `dist/PersonalWiki/PersonalWiki.exe`

GitHub Release에서 다운로드한 경우
→ `PersonalWiki.exe` 바로 실행

---

## 첨부 파일 사용 방법

현재 위키 내부에서 파일 업로드 기능은 없음

따라서:

* 이미지 → `/img/` 폴더에 직접 추가
* 파일 → `/file/` 폴더에 직접 추가

---
