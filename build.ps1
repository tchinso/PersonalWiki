$ErrorActionPreference = "Stop"

Write-Host "[1/2] Installing dependencies..."
python -m pip install -r requirements.txt

Write-Host "[2/2] Building onedir executable with PyInstaller..."
python -m PyInstaller --noconfirm --clean --onedir --name PersonalWiki `
  --add-data "templates;templates" `
  --add-data "static;static" `
  app.py

Write-Host "Build complete."
Write-Host "Run: .\dist\PersonalWiki\PersonalWiki.exe"
