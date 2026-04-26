$ErrorActionPreference = 'SilentlyContinue'

$projectRoot = Split-Path -Parent $PSScriptRoot
$logsDir = Join-Path $projectRoot 'logs'
$logFile = Join-Path $logsDir 'auto-backup.log'

if (-not (Test-Path $logsDir)) {
  New-Item -ItemType Directory -Path $logsDir -Force | Out-Null
}

function Write-Log($msg) {
  $line = "[$(Get-Date -Format s)] $msg"
  $line | Out-File -FilePath $logFile -Append -Encoding utf8
}

function Test-BackupWatcherRunning {
  $processes = Get-CimInstance Win32_Process -Filter "Name='node.exe'"
  foreach ($proc in $processes) {
    $cmd = [string]$proc.CommandLine
    if ($cmd -match 'backup:auto' -or $cmd -match 'chokidar') {
      return $true
    }
  }
  return $false
}

Write-Log "=== Demarrage Auto-Backup ==="

if (Test-BackupWatcherRunning) {
  Write-Log "Watcher backup deja actif. Aucun nouveau lancement."
  exit 0
}

Set-Location $projectRoot

if (-not (Get-Command npm.cmd -ErrorAction SilentlyContinue)) {
  Write-Log "npm.cmd introuvable. Auto-backup non lance."
  exit 1
}

Write-Log "Lancement de: npm run backup:auto"
$npmOut = Join-Path $logsDir 'auto-backup-run.out.log'
$npmErr = Join-Path $logsDir 'auto-backup-run.err.log'

Start-Process -FilePath 'npm.cmd' -ArgumentList 'run', 'backup:auto' -WorkingDirectory $projectRoot -WindowStyle Hidden -RedirectStandardOutput $npmOut -RedirectStandardError $npmErr

Write-Log "Watcher backup lance."
exit 0
