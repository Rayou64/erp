param(
  [Parameter(Mandatory = $true)]
  [string]$DoToken,

  [Parameter(Mandatory = $true)]
  [string]$GithubRepo,

  [string]$GithubBranch = "main",
  [string]$AppName = "ryanerp",
  [string]$Region = "fra",
  [string]$JwtSecret = ""
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($JwtSecret)) {
  $JwtSecret = [Guid]::NewGuid().ToString("N") + [Guid]::NewGuid().ToString("N")
}

$root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$templatePath = Join-Path $root ".do\app.template.yaml"
$specPath = Join-Path $root ".do\app.generated.yaml"

if (-not (Test-Path $templatePath)) {
  throw "Template introuvable: $templatePath"
}

$content = Get-Content -Raw -Path $templatePath
$content = $content.Replace("__APP_NAME__", $AppName)
$content = $content.Replace("__REGION__", $Region)
$content = $content.Replace("__GITHUB_REPO__", $GithubRepo)
$content = $content.Replace("__GITHUB_BRANCH__", $GithubBranch)
$content = $content.Replace("__JWT_SECRET__", $JwtSecret)
Set-Content -Path $specPath -Value $content -Encoding UTF8

doctl auth init --access-token $DoToken | Out-Null

$existing = doctl apps list --format ID,Spec.Name --no-header 2>$null | Where-Object { $_ -match "\s$AppName$" }
if ($existing) {
  $appId = ($existing -split "\s+")[0]
  Write-Host "Mise a jour de l'app $AppName ($appId)..."
  doctl apps update $appId --spec $specPath
} else {
  Write-Host "Creation de l'app $AppName..."
  doctl apps create --spec $specPath
}

Write-Host "Spec utilisee: $specPath"
Write-Host "Termine. Verifie le status avec: doctl apps list"