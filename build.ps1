[CmdletBinding()]
param(
  [string]$PythonPath = "python",
  [string]$DotnetPath = "dotnet",
  [ValidateSet("win-x64", "win-arm64", "win-x86")]
  [string]$ClientRuntime = "win-x64",
  [switch]$SkipClient
)

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

function Resolve-BuildExecutable {
  param(
    [Parameter(Mandatory = $true)]
    [string]$Requested,
    [Parameter(Mandatory = $true)]
    [string]$DisplayName
  )

  if (Test-Path -LiteralPath $Requested -PathType Leaf) {
    $candidate = (Resolve-Path -LiteralPath $Requested).Path
  }
  else {
    $command = Get-Command $Requested -CommandType Application -ErrorAction SilentlyContinue | Select-Object -First 1
    if (-not $command) {
      throw "$DisplayName executable not found. Pass -$($DisplayName)Path with its full path."
    }
    $candidate = $command.Source
  }

  try {
    & $candidate --version *> $null
  }
  catch {
    throw "$DisplayName executable could not be started: $candidate"
  }
  if ($LASTEXITCODE -ne 0) {
    throw "$DisplayName executable is not usable: $candidate"
  }
  return $candidate
}

function Resolve-PythonBuildExecutable {
  param(
    [Parameter(Mandatory = $true)]
    [string]$Requested,
    [Parameter(Mandatory = $true)]
    [bool]$AllowLauncherFallback
  )

  try {
    return Resolve-BuildExecutable -Requested $Requested -DisplayName "Python"
  }
  catch {
    if (-not $AllowLauncherFallback) {
      throw
    }
  }

  return Resolve-BuildExecutable -Requested "py" -DisplayName "Python"
}

function Restore-RuntimeState {
  param(
    [Parameter(Mandatory = $true)]
    [string]$BackupDistribution,
    [Parameter(Mandatory = $true)]
    [string]$DestinationDistribution
  )

  foreach ($directoryName in @("doc", "img", "file", "db_backups")) {
    $source = Join-Path $BackupDistribution $directoryName
    if (Test-Path -LiteralPath $source -PathType Container) {
      Copy-Item -LiteralPath $source -Destination $DestinationDistribution -Recurse -Force
    }
  }

  Get-ChildItem -LiteralPath $BackupDistribution -File -Force -ErrorAction SilentlyContinue |
    Where-Object { $_.Name -like "wiki*.db*" -or $_.Name -eq "wikisettings.cfg" } |
    ForEach-Object {
      Copy-Item -LiteralPath $_.FullName -Destination $DestinationDistribution -Force
    }
}

function Restore-DistributionBackup {
  param(
    [Parameter(Mandatory = $true)]
    [string]$BackupDistribution,
    [Parameter(Mandatory = $true)]
    [string]$DestinationDistribution,
    [Parameter(Mandatory = $true)]
    [string]$AllowedParent
  )

  if (-not (Test-Path -LiteralPath $BackupDistribution -PathType Container)) {
    throw "Backup distribution is missing: $BackupDistribution"
  }

  $resolvedParent = [System.IO.Path]::GetFullPath($AllowedParent).TrimEnd(
    [System.IO.Path]::DirectorySeparatorChar,
    [System.IO.Path]::AltDirectorySeparatorChar
  )
  $resolvedDestination = [System.IO.Path]::GetFullPath($DestinationDistribution)
  $expectedDestination = Join-Path $resolvedParent "PersonalWiki"
  if (-not [string]::Equals($resolvedDestination, $expectedDestination, [System.StringComparison]::OrdinalIgnoreCase)) {
    throw "Refusing to replace an unexpected distribution path: $resolvedDestination"
  }

  $resolvedBackup = [System.IO.Path]::GetFullPath($BackupDistribution)
  $destinationPrefix = $resolvedDestination + [System.IO.Path]::DirectorySeparatorChar
  if ($resolvedBackup.StartsWith($destinationPrefix, [System.StringComparison]::OrdinalIgnoreCase)) {
    throw "Refusing to restore a backup located inside the destination: $resolvedBackup"
  }

  if (Test-Path -LiteralPath $resolvedDestination) {
    $destinationItem = Get-Item -LiteralPath $resolvedDestination -Force
    if ($destinationItem.Attributes -band [System.IO.FileAttributes]::ReparsePoint) {
      throw "Refusing to replace a reparse-point destination: $resolvedDestination"
    }
    Remove-Item -LiteralPath $resolvedDestination -Recurse -Force
  }

  New-Item -ItemType Directory -Path $resolvedDestination -Force | Out-Null
  Get-ChildItem -LiteralPath $BackupDistribution -Force | ForEach-Object {
    Copy-Item -LiteralPath $_.FullName -Destination $resolvedDestination -Recurse -Force
  }
}

$projectRoot = (Resolve-Path -LiteralPath $PSScriptRoot).Path
$distRoot = Join-Path $projectRoot "dist"
$distDir = Join-Path $distRoot "PersonalWiki"
$requirementsPath = Join-Path $projectRoot "requirements.txt"
$appPath = Join-Path $projectRoot "app.py"
$dbFixPath = Join-Path $projectRoot "personal_wiki_db_fix.py"
$templatesDir = Join-Path $projectRoot "templates"
$staticDir = Join-Path $projectRoot "static"
$imageSourceDir = Join-Path $projectRoot "img"
$settingsSourcePath = Join-Path $projectRoot "wikisettings.cfg"
$serverBuildDir = Join-Path $projectRoot "build\PersonalWiki"
$dbFixBuildDir = Join-Path $projectRoot "build\PersonalWikiDBFix"
$runtimeBackup = Join-Path ([System.IO.Path]::GetTempPath()) ("PersonalWiki-build-backup-" + [guid]::NewGuid().ToString("N"))
$backupDistribution = Join-Path $runtimeBackup "PersonalWiki"
$hasDistributionBackup = $false
$buildSucceeded = $false
$recoverySucceeded = $false

$pythonExe = Resolve-PythonBuildExecutable `
  -Requested $PythonPath `
  -AllowLauncherFallback (-not $PSBoundParameters.ContainsKey("PythonPath"))
$dotnetExe = $null
if (-not $SkipClient) {
  $dotnetExe = Resolve-BuildExecutable -Requested $DotnetPath -DisplayName "Dotnet"
}
$iconPath = (Resolve-Path -LiteralPath (Join-Path $imageSourceDir "icon.ico")).Path

try {
  if (Test-Path -LiteralPath $distDir -PathType Container) {
    New-Item -ItemType Directory -Path $runtimeBackup -Force | Out-Null
    Copy-Item -LiteralPath $distDir -Destination $runtimeBackup -Recurse -Force
    if (-not (Test-Path -LiteralPath $backupDistribution -PathType Container)) {
      throw "Could not create a complete distribution backup at: $backupDistribution"
    }
    $hasDistributionBackup = $true
  }

  Write-Host "[1/4] Installing Python build dependencies..."
  & $pythonExe -m pip install -r $requirementsPath
  if ($LASTEXITCODE -ne 0) {
    throw "Dependency installation failed."
  }

  Write-Host "[2/4] Building PersonalWiki.exe (onedir) with PyInstaller..."
  New-Item -ItemType Directory -Path $serverBuildDir -Force | Out-Null
  & $pythonExe -m PyInstaller --noconfirm --clean --onedir --name PersonalWiki `
    --distpath $distRoot `
    --workpath $serverBuildDir `
    --specpath $serverBuildDir `
    --add-data "$templatesDir;templates" `
    --add-data "$staticDir;static" `
    --add-data "$imageSourceDir;img" `
    --icon "$iconPath" `
    $appPath
  if ($LASTEXITCODE -ne 0) {
    throw "PyInstaller build failed. If dist\\PersonalWiki is in use, close the running EXE and try again."
  }

  Write-Host "[3/4] Building PersonalWikiDBFix.exe (onefile) next to PersonalWiki.exe..."
  New-Item -ItemType Directory -Path $dbFixBuildDir -Force | Out-Null
  & $pythonExe -m PyInstaller --noconfirm --clean --onefile --name PersonalWikiDBFix `
    --distpath $distDir `
    --workpath $dbFixBuildDir `
    --specpath $dbFixBuildDir `
    --icon "$iconPath" `
    $dbFixPath
  if ($LASTEXITCODE -ne 0) {
    throw "PyInstaller build for PersonalWikiDBFix failed."
  }

  foreach ($directoryName in @("doc", "img", "file")) {
    New-Item -ItemType Directory -Path (Join-Path $distDir $directoryName) -Force | Out-Null
  }
  Copy-Item -Path (Join-Path $imageSourceDir "*") -Destination (Join-Path $distDir "img") -Force
  Convert-ToUtf8NoBomFile -Path $settingsSourcePath
  $distSettingsPath = Join-Path $distDir "wikisettings.cfg"
  Copy-Item -LiteralPath $settingsSourcePath -Destination $distSettingsPath -Force
  Convert-ToUtf8NoBomFile -Path $distSettingsPath

  if ($hasDistributionBackup) {
    Restore-RuntimeState -BackupDistribution $backupDistribution -DestinationDistribution $distDir
  }
  Convert-ToUtf8NoBomFile -Path $distSettingsPath

  $syntaxDocName = -join @([char]0xC704, [char]0xD0A4, "-", [char]0xBB38, [char]0xBC95, "-", [char]0xC124, [char]0xBA85, [char]0xC11C)
  $sourceDocDir = Join-Path $projectRoot "doc"
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

  if (-not $SkipClient) {
    $clientBuildScript = Join-Path $projectRoot "PersonalWikiClient\build.ps1"
    $clientDistDir = Join-Path $distRoot "PersonalWikiClient"
    Write-Host "[4/4] Building PersonalWikiClient.exe (self-contained native renderer)..."
    & $clientBuildScript `
      -DotnetPath $dotnetExe `
      -Configuration "Release" `
      -Runtime $ClientRuntime `
      -OutputPath $clientDistDir
    if ($LASTEXITCODE -ne 0) {
      throw "PersonalWikiClient build failed."
    }
  }

  $buildSucceeded = $true
}
catch {
  $buildFailure = $_
  if ($hasDistributionBackup) {
    try {
      Restore-DistributionBackup `
        -BackupDistribution $backupDistribution `
        -DestinationDistribution $distDir `
        -AllowedParent $distRoot
      $recoverySucceeded = $true
      Write-Warning "Build failed; the previous PersonalWiki distribution was restored."
    }
    catch {
      Write-Warning "Build failed and automatic recovery also failed. The backup remains at: $runtimeBackup. Recovery error: $($_.Exception.Message)"
    }
  }
  throw $buildFailure
}
finally {
  if (Test-Path -LiteralPath $runtimeBackup) {
    if ($buildSucceeded -or $recoverySucceeded -or -not $hasDistributionBackup) {
      Remove-Item -LiteralPath $runtimeBackup -Recurse -Force
    }
    else {
      Write-Warning "Build backup was retained for manual recovery: $runtimeBackup"
    }
  }
}

Write-Host "Build complete."
Write-Host "Run: $(Join-Path $distDir 'PersonalWiki.exe')"
Write-Host "Fix tool: $(Join-Path $distDir 'PersonalWikiDBFix.exe')"
if (-not $SkipClient) {
  Write-Host "Native client: $(Join-Path $distRoot 'PersonalWikiClient\PersonalWikiClient.exe')"
}
