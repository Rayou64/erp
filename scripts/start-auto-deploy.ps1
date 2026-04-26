$ErrorActionPreference = 'SilentlyContinue'

$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot

# Supprimer RAILWAY_TOKEN si vide/invalide pour utiliser la session OAuth
Remove-Item Env:\RAILWAY_TOKEN -ErrorAction SilentlyContinue

$logsDir = Join-Path $projectRoot 'logs'
$logFile = Join-Path $logsDir 'auto-deploy.log'

function Write-Log($msg) {
  $line = "[$(Get-Date -Format s)] $msg"
  $line | Out-File -FilePath $logFile -Append -Encoding utf8
  Write-Host $line
}

function Railway-Login {
  Write-Log "=== TOKEN EXPIRE - RECONNEXION RAILWAY ==="
  # Lance login browserless et capture la sortie ligne par ligne
  $loginProc = Start-Process -FilePath 'railway' -ArgumentList 'login','--browserless' `
    -NoNewWindow -PassThru -RedirectStandardOutput "$logsDir\railway-login-out.txt" `
    -RedirectStandardError "$logsDir\railway-login-err.txt"
  
  # Attendre que le fichier contienne le code (max 10s)
  $waited = 0
  $code = $null; $url = $null
  while ($waited -lt 20 -and (-not $code)) {
    Start-Sleep -Milliseconds 500
    $waited++
    $content = ''
    if (Test-Path "$logsDir\railway-login-out.txt") { $content = Get-Content "$logsDir\railway-login-out.txt" -Raw }
    if ($content -match 'authentication code is: ([A-Z0-9\-]+)') { $code = $Matches[1] }
    if ($content -match 'https://railway\.com/activate') { $url = 'https://railway.com/activate' }
  }

  if ($code) {
    $msg = "RAILWAY LOGIN REQUIS ! Code: $code | URL: $url"
    Write-Log $msg
    # Notification Windows toast
    [Windows.UI.Notifications.ToastNotificationManager, Windows.UI.Notifications, ContentType = WindowsRuntime] | Out-Null
    $template = [Windows.UI.Notifications.ToastNotificationManager]::GetTemplateContent(
      [Windows.UI.Notifications.ToastTemplateType]::ToastText02)
    $template.SelectSingleNode('//text[@id="1"]').InnerText = 'RyanERP Auto-Deploy'
    $template.SelectSingleNode('//text[@id="2"]').InnerText = "Code Railway: $code - Allez sur railway.com/activate"
    $toast = [Windows.UI.Notifications.ToastNotification]::new($template)
    [Windows.UI.Notifications.ToastNotificationManager]::CreateToastNotifier('RyanERP').Show($toast)
    # Ouvrir le navigateur automatiquement
    Start-Process "https://railway.com/activate"
  }

  # Attendre que le login réussisse (max 5 minutes)
  $loginProc | Wait-Process -Timeout 300 -ErrorAction SilentlyContinue
  $outContent = if (Test-Path "$logsDir\railway-login-out.txt") { Get-Content "$logsDir\railway-login-out.txt" -Raw } else { '' }
  if ($outContent -match 'Logged in as') {
    Write-Log "Reconnexion réussie !"
    return $true
  }
  Write-Log "Reconnexion échouée ou timeout"
  return $false
}

function Deploy-Now {
  Write-Log "Déploiement en cours..."
  $result = railway up -d -s terrific-love -e production 2>&1 | Out-String
  $result | Out-File -FilePath $logFile -Append -Encoding utf8
  if ($result -match 'Unauthorized|invalid_grant|Token refresh failed') {
    Write-Log "Erreur auth détectée, reconnexion..."
    $ok = Railway-Login
    if ($ok) {
      Write-Log "Retry du déploiement..."
      railway up -d -s terrific-love -e production 2>&1 | Out-File -FilePath $logFile -Append -Encoding utf8
    }
  } elseif ($result -match 'Build Logs') {
    Write-Log "Déploiement réussi !"
  }
}

Write-Log "=== Démarrage RyanERP Auto-Deploy ==="
Deploy-Now

# Watcher de fichiers - surveille les changements et déploie
Write-Log "Watcher démarré. Surveillance: app.js, public/*, package.json"
$watcher = New-Object System.IO.FileSystemWatcher
$watcher.Path = $projectRoot
$watcher.Filter = '*.*'
$watcher.IncludeSubdirectories = $true
$watcher.EnableRaisingEvents = $true

$lastDeploy = [datetime]::MinValue
$debounceSeconds = 8

while ($true) {
  $changed = $watcher.WaitForChanged([System.IO.WatcherChangeTypes]::All, 1000)
  if (-not $changed.TimedOut) {
    $name = $changed.Name
    # Ignorer node_modules, archives, logs, scripts, .git
    if ($name -notmatch '^(node_modules|archives|logs|scripts|\.git|ngrok)') {
      $now = [datetime]::Now
      if (($now - $lastDeploy).TotalSeconds -gt $debounceSeconds) {
        $lastDeploy = $now
        Write-Log "Changement détecté: $name"
        Deploy-Now
      }
    }
  }
}