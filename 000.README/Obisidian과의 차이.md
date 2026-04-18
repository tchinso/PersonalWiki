# Obsidian 에 없는 기능

## 유튜브 임베드
독자적인 유튜브 첨부 문법 사용
* Obsidian

지원하지 않음
* Personal Wiki

`![[youtube(HhnETSN6U_E)]]`

## 첨부파일 기능

미디어 파일이 아닌 파일도 첨부할 수 있음

* Obsidian

지원하지 않음
* PersonalWiki

`[[file/file.zip]]`

## 접기 기능
* Obsidian

지원하지 않음
* PersonalWiki
`||{{템플릿테스트}}||`

# Obsidian과 호환됨

## Obsidian Web Clipper

PersonalWiki에서는 웹사이트 링크를 첨부할 때 아래 형태가 표준 문법입니다

[[https://example.com|보이는 텍스트]] 

그러나, Obsidian Web Clipper와의 호환을 위해 아래 문법도 지원합니다.

[https://example.com](보이는 텍스트)

# Obsidian과 차이가 있음

## 이미지 첨부

* Obsidian

` [[이미지경로]]]` 
* PersonalWiki
` ![[이미지경로]]` 


## 템플릿(문서 내용 불러오기)

* Obsidian

` [[템플릿이름]]]` 
* PersonalWiki

 `{{템플릿이름}}` 


## 콜아웃

콜아웃 문법이 다름

* Obsidian

\> [!note]

* * 콜아웃에서 마크다운 문법이나 미디어 첨부를 지원함
  * 콜아웃 유형을 지정하지 않으면 기본적으로 note로 인식함
  * 콜아웃이 note, summary, tip, todo등 다양함

* PersonalWiki

\!!!note

* * 콜아웃 에서 마크다운 문법이나 첨부 미디어를 지원하지 않음
  * 콜아웃 유형 지정이 필수임
  * 콜아웃 유형이 note/info/warn/danger 로 뿐임
