$ErrorActionPreference = "Stop"

Write-Host "[1/3] Installing dependencies..."
python -m pip install -r requirements.txt
if ($LASTEXITCODE -ne 0) {
  throw "Dependency installation failed."
}

Write-Host "[2/3] Building PersonalWiki.exe (onedir) with PyInstaller..."
python -m PyInstaller --noconfirm --clean --onedir --name PersonalWiki `
  --add-data "templates;templates" `
  --add-data "static;static" `
  app.py
if ($LASTEXITCODE -ne 0) {
  throw "PyInstaller build failed. If dist\\PersonalWiki is in use, close the running EXE and try again."
}

Write-Host "[3/3] Building PersonalWikiDBFix.exe (onefile) next to PersonalWiki.exe..."
python -m PyInstaller --noconfirm --clean --onefile --name PersonalWikiDBFix `
  --distpath "dist\\PersonalWiki" `
  --workpath "build\\PersonalWikiDBFix" `
  --specpath "build\\PersonalWikiDBFix" `
  personal_wiki_db_fix.py
if ($LASTEXITCODE -ne 0) {
  throw "PyInstaller build for PersonalWikiDBFix failed."
}

Write-Host "Build complete."
Write-Host "Run: .\dist\PersonalWiki\PersonalWiki.exe"
Write-Host "Fix tool: .\dist\PersonalWiki\PersonalWikiDBFix.exe"
