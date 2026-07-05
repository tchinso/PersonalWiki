$ErrorActionPreference = "Stop"

$utf8NoBomEncoding = New-Object System.Text.UTF8Encoding -ArgumentList $false

function Convert-ToUtf8NoBomFile {
  param(
    [Parameter(Mandatory = $true)]
    [string]$Path
  )

  if (-not (Test-Path -LiteralPath $Path)) {
    return
  }

  $resolvedPath = (Resolve-Path -LiteralPath $Path).Path
  $text = [System.IO.File]::ReadAllText($resolvedPath)
  [System.IO.File]::WriteAllText($resolvedPath, $text, $utf8NoBomEncoding)
}

$pythonCommand = Get-Command python -ErrorAction SilentlyContinue
if (-not $pythonCommand) {
  $pythonCommand = Get-Command py -ErrorAction SilentlyContinue
}
if (-not $pythonCommand) {
  throw "Python executable not found. Install Python 3 and ensure 'python' or 'py' is available in PATH."
}

$pythonExe = $pythonCommand.Source
$iconPath = (Resolve-Path "img\\icon.ico").Path
$distDir = Join-Path (Get-Location) "dist\\PersonalWiki"
$runtimeBackup = Join-Path ([System.IO.Path]::GetTempPath()) ("PersonalWiki-build-backup-" + [guid]::NewGuid().ToString("N"))
$hasRuntimeBackup = $false

if (Test-Path -LiteralPath $distDir) {
  New-Item -ItemType Directory -Path $runtimeBackup -Force | Out-Null
  foreach ($directoryName in @("doc", "img", "file")) {
    $source = Join-Path $distDir $directoryName
    if (Test-Path -LiteralPath $source) {
      Copy-Item -LiteralPath $source -Destination $runtimeBackup -Recurse -Force
      $hasRuntimeBackup = $true
    }
  }
  Get-ChildItem -LiteralPath $distDir -File -ErrorAction SilentlyContinue |
    Where-Object { $_.Name -like "wiki*.db*" -or $_.Name -eq "wikisettings.cfg" } |
    ForEach-Object {
      Copy-Item -LiteralPath $_.FullName -Destination $runtimeBackup -Force
      $hasRuntimeBackup = $true
    }
}

Write-Host "[1/3] Installing dependencies..."
& $pythonExe -m pip install -r requirements.txt
if ($LASTEXITCODE -ne 0) {
  throw "Dependency installation failed."
}

Write-Host "[2/3] Building PersonalWiki.exe (onedir) with PyInstaller..."
& $pythonExe -m PyInstaller --noconfirm --clean --onedir --name PersonalWiki `
  --add-data "templates;templates" `
  --add-data "static;static" `
  --add-data "img;img" `
  --icon "$iconPath" `
  app.py
if ($LASTEXITCODE -ne 0) {
  throw "PyInstaller build failed. If dist\\PersonalWiki is in use, close the running EXE and try again."
}

Write-Host "[3/3] Building PersonalWikiDBFix.exe (onefile) next to PersonalWiki.exe..."
& $pythonExe -m PyInstaller --noconfirm --clean --onefile --name PersonalWikiDBFix `
  --distpath "dist\\PersonalWiki" `
  --workpath "build\\PersonalWikiDBFix" `
  --specpath "build\\PersonalWikiDBFix" `
  --icon "$iconPath" `
  personal_wiki_db_fix.py
if ($LASTEXITCODE -ne 0) {
  throw "PyInstaller build for PersonalWikiDBFix failed."
}

foreach ($directoryName in @("doc", "img", "file")) {
  New-Item -ItemType Directory -Path (Join-Path $distDir $directoryName) -Force | Out-Null
}
Copy-Item -Path "img\\*" -Destination (Join-Path $distDir "img") -Force
Convert-ToUtf8NoBomFile -Path "wikisettings.cfg"
$distSettingsPath = Join-Path $distDir "wikisettings.cfg"
Copy-Item -LiteralPath "wikisettings.cfg" -Destination $distSettingsPath -Force
Convert-ToUtf8NoBomFile -Path $distSettingsPath

if ($hasRuntimeBackup) {
  Get-ChildItem -LiteralPath $runtimeBackup -Force | ForEach-Object {
    Copy-Item -LiteralPath $_.FullName -Destination $distDir -Recurse -Force
  }
}
Convert-ToUtf8NoBomFile -Path $distSettingsPath

$syntaxDocName = -join @([char]0xC704, [char]0xD0A4, "-", [char]0xBB38, [char]0xBC95, "-", [char]0xC124, [char]0xBA85, [char]0xC11C)
$sourceDocDir = Join-Path (Get-Location) "doc"
$sourceJsonDir = Join-Path $sourceDocDir "json"
$sourceSyntaxDoc = Join-Path $sourceDocDir ($syntaxDocName + ".md")
$sourceSyntaxJson = Join-Path $sourceJsonDir ($syntaxDocName + ".json")
$distDocDir = Join-Path $distDir "doc"
$distJsonDir = Join-Path $distDocDir "json"
New-Item -ItemType Directory -Path $distDocDir -Force | Out-Null
New-Item -ItemType Directory -Path $distJsonDir -Force | Out-Null
if (Test-Path -LiteralPath $sourceSyntaxDoc) {
  Copy-Item -LiteralPath $sourceSyntaxDoc -Destination (Join-Path $distDocDir ($syntaxDocName + ".md")) -Force
}
if (Test-Path -LiteralPath $sourceSyntaxJson) {
  Copy-Item -LiteralPath $sourceSyntaxJson -Destination (Join-Path $distJsonDir ($syntaxDocName + ".json")) -Force
}

$resolvedTemp = [System.IO.Path]::GetFullPath([System.IO.Path]::GetTempPath())
$resolvedBackup = [System.IO.Path]::GetFullPath($runtimeBackup)
if ($resolvedBackup.StartsWith($resolvedTemp, [System.StringComparison]::OrdinalIgnoreCase) -and (Test-Path -LiteralPath $resolvedBackup)) {
  Remove-Item -LiteralPath $resolvedBackup -Recurse -Force
}

Write-Host "Build complete."
Write-Host "Run: .\dist\PersonalWiki\PersonalWiki.exe"
Write-Host "Fix tool: .\dist\PersonalWiki\PersonalWikiDBFix.exe"
