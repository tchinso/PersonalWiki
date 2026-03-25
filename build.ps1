$ErrorActionPreference = "Stop"

Write-Host "[1/2] Installing dependencies..."
python -m pip install -r requirements.txt
if ($LASTEXITCODE -ne 0) {
  throw "Dependency installation failed."
}

Write-Host "[2/2] Building onedir executable with PyInstaller..."
python -m PyInstaller --noconfirm --clean --onedir --name PersonalWiki `
  --add-data "templates;templates" `
  --add-data "static;static" `
  app.py
if ($LASTEXITCODE -ne 0) {
  throw "PyInstaller build failed. If dist\\PersonalWiki is in use, close the running EXE and try again."
}

Write-Host "Build complete."
Write-Host "Run: .\dist\PersonalWiki\PersonalWiki.exe"
