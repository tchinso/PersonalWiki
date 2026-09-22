# PersonalWikiClient

`PersonalWikiClient`는 PersonalWiki를 읽고 편집하기 위한 Windows 로컬 클라이언트입니다. WinForms 컨트롤과 `HttpClient`로 문서를 직접 렌더링하며 WebView2, Chromium, `WebBrowser` 컨트롤 또는 SQLite 직접 접근을 포함하지 않습니다. Windows GDI+에 없는 WebP 코덱만 MIT 라이선스의 `SkiaSharp`/`SkiaSharp.NativeAssets.Win32` 4.152.1를 self-contained 방식으로 함께 넣어 직접 디코딩합니다. 이는 브라우저·웹 렌더러를 포함하는 것이 아닙니다.

서버와는 별개 프로그램입니다. `127.0.0.1:<클라이언트에서 설정한 포트>`의 `/api/client/*` JSON API만 호출합니다. ngrok, Tailscale, 외부 IP·도메인에는 접속하지 않으며 PersonalWiki의 `wikisettings.cfg`, `wiki.db`, 문서 파일을 읽거나 쓰지 않습니다.

## 사용

1. PersonalWiki 서버를 localhost에서 실행합니다.
2. `PersonalWikiClient.exe`를 실행합니다.
3. 연결 실패 화면 또는 **연결·글꼴**에서 PersonalWiki의 현재 포트 번호를 입력하고 다시 시도합니다.

클라이언트 포트와 글꼴은 `%LocalAppData%\PersonalWikiClient\settings.json`에만 저장됩니다. 이 글꼴은 웹 브라우저용 PersonalWiki 글꼴 설정과 독립적입니다.

문서 보기와 미리보기는 서버가 제공한 PersonalWiki native AST를 WinForms 컨트롤로 직접 렌더링합니다. Markdown, 위키/파일 링크, 이미지, YouTube 카드(썸네일과 기본 브라우저 열기), 태그 문서 목록, 표·열 비율/정렬/colspan, 스포일러, 콜아웃, 템플릿/접는 템플릿, 목차, 각주, 텍스트 스타일을 지원합니다. 안전한 `<br>` 및 `<kbd>`만 전용 컨트롤로 처리하며, 그 밖의 raw HTML은 실행하지 않고 문자 그대로 보여 줍니다.

문서 내부 링크는 `/doc/...`, `/new?...`, `/file/...` 및 같은 문서의 `#heading-anchor`만 네이티브 동작으로 처리합니다. 외부 링크는 `http`/`https`만 기본 브라우저에 전달하며 `file://`, `mailto:`, 조각 외부 실행 및 그 밖의 프로토콜은 실행하지 않습니다.

## 이미지 붙여넣기와 드롭

편집기에서 `Ctrl+V`로 이미지 비트맵을 붙여넣거나 이미지 파일을 드롭하면, 실행 파일 바로 옆의 날짜 폴더에 저장하고 커서 위치에 `![[YYYYMMDD/파일명]]`을 삽입합니다.

```text
PersonalWikiClient.exe 옆
└─ 20260922
   └─ clipboard-....webp
```

클립보드 비트맵은 함께 포함된 native WebP 인코더로 품질 90의 `.webp`로 저장됩니다. 해당 인코딩이 실패할 때만 PNG로 안전하게 대체 저장합니다. 파일 드롭에서 `.webp`는 재인코딩하지 않고 원본 확장자와 바이트를 보존하며, 보기에서는 같은 native WebP 디코더로 표시합니다.

문서 저장에 성공하면 최종 본문에 실제로 남아 있는 이번 편집의 날짜 폴더·파일 목록과 함께 안내가 표시됩니다. 해당 클라이언트의 `YYYYMMDD` 폴더를 PersonalWiki 서버의 `img\YYYYMMDD`에 **직접 복사**해야 이미지가 표시됩니다. 클라이언트는 이를 자동 복사·업로드하지 않습니다.

## 빌드

빌드 시에는 `.NET 8 SDK`와 최초 한 번의 NuGet 패키지 복원이 필요합니다. 만들어진 EXE에는 WebP 디코더가 포함되므로 Python, WebView2 런타임, 설치된 .NET 런타임 또는 별도 이미지 코덱은 필요하지 않습니다.

```powershell
.\PersonalWikiClient\build.ps1 -DotnetPath C:\path\to\dotnet.exe
```

이 스크립트는 native renderer smoke test 후 self-contained, single-file `win-x64` EXE를 `PersonalWikiClient\dist\PersonalWikiClient.exe`에 만듭니다. 같은 폴더에는 포함된 SkiaSharp WebP 디코더의 라이선스·서드파티 고지 파일도 함께 출력됩니다. 기본 `win-x64` 외 런타임 식별자가 필요하면 `-Runtime`을 지정할 수 있습니다.
