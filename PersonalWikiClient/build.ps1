[CmdletBinding()]
param(
    [string] $DotnetPath = "dotnet",
    [ValidateSet("Debug", "Release")]
    [string] $Configuration = "Release",
    [string] $Runtime = "win-x64",
    [string] $OutputPath = (Join-Path $PSScriptRoot "dist"),
    [switch] $SkipSmokeTest
)

$ErrorActionPreference = "Stop"
$projectPath = Join-Path $PSScriptRoot "PersonalWikiClient.csproj"
$smokeProjectPath = Join-Path $PSScriptRoot "..\PersonalWikiClient.SmokeTests\PersonalWikiClient.SmokeTests.csproj"

if (-not (Test-Path -LiteralPath $DotnetPath -PathType Leaf) -and -not (Get-Command $DotnetPath -ErrorAction SilentlyContinue)) {
    throw "dotnet SDK를 찾을 수 없습니다. -DotnetPath에 dotnet.exe 경로를 지정하세요."
}

if (-not $SkipSmokeTest) {
    & $DotnetPath run --project $smokeProjectPath --nologo
    if ($LASTEXITCODE -ne 0) {
        throw "네이티브 렌더러 smoke test가 실패했습니다."
    }
}

& $DotnetPath publish $projectPath `
    --configuration $Configuration `
    --runtime $Runtime `
    --self-contained true `
    --output $OutputPath `
    -p:PublishSingleFile=true `
    -p:IncludeNativeLibrariesForSelfExtract=true `
    -p:DebugType=None `
    -p:DebugSymbols=false `
    --nologo

if ($LASTEXITCODE -ne 0) {
    throw "PersonalWikiClient publish가 실패했습니다."
}

Write-Host "완료: $(Join-Path $OutputPath 'PersonalWikiClient.exe')"
