$ErrorActionPreference = 'Stop'

function Get-Token($base, $username, $password) {
  return (Invoke-RestMethod -Method Post -Uri "$base/api/auth/login" -ContentType 'application/json' -Body (@{ username = $username; password = $password } | ConvertTo-Json)).token
}

function Get-Json($base, $token, $path) {
  return Invoke-RestMethod -Method Get -Uri "$base$path" -Headers @{ Authorization = "Bearer $token" }
}

function Try-PostJson($base, $token, $path, $body) {
  try {
    $response = Invoke-RestMethod -Method Post -Uri "$base$path" -Headers @{ Authorization = "Bearer $token" } -ContentType 'application/json' -Body $body
    return [ordered]@{
      ok = $true
      body = $response
    }
  } catch {
    $resp = $_.Exception.Response
    if ($null -eq $resp) {
      return [ordered]@{
        ok = $false
        status = -1
        body = $_.Exception.Message
      }
    }
    $text = ''
    try {
      if ($resp -is [System.Net.Http.HttpResponseMessage]) {
        $text = $resp.Content.ReadAsStringAsync().GetAwaiter().GetResult()
      } elseif ($resp.PSObject.Methods.Name -contains 'GetResponseStream') {
        $reader = New-Object System.IO.StreamReader($resp.GetResponseStream())
        $text = $reader.ReadToEnd()
      }
    } catch {
      $text = $_.Exception.Message
    }
    return [ordered]@{
      ok = $false
      status = [int]$resp.StatusCode
      body = $text
    }
  }
}

function Summarize($value) {
  if ($value -is [System.Array]) {
    $first = $value | Select-Object -First 1
    return [ordered]@{
      type = 'array'
      count = $value.Count
      keys = if ($first) { @($first.PSObject.Properties.Name) } else { @() }
    }
  }

  return [ordered]@{
    type = 'object'
    count = 1
    keys = @($value.PSObject.Properties.Name)
  }
}

$local = 'http://localhost:4000'
$public = 'https://erp-new-20260423-220559-production.up.railway.app'

$localToken = Get-Token $local 'admin' 'admin123'
$publicToken = Get-Token $public 'admin' 'admin123'

$readPaths = @(
  '/api/auth/me',
  '/api/projects',
  '/api/material-catalog',
  '/api/material-requests',
  '/api/purchase-orders'
)

$writeTests = @(
  @{ path = '/api/material-requests/auto-stage'; body = '{}' },
  @{ path = '/api/purchase-orders'; body = '{}' }
)

$result = [ordered]@{
  reads = [ordered]@{}
  writes = [ordered]@{}
}

foreach ($path in $readPaths) {
  $localValue = Get-Json $local $localToken $path
  $publicValue = Get-Json $public $publicToken $path
  $result.reads[$path] = [ordered]@{
    local = Summarize $localValue
    public = Summarize $publicValue
  }
}

foreach ($test in $writeTests) {
  $result.writes[$test.path] = [ordered]@{
    local = Try-PostJson $local $localToken $test.path $test.body
    public = Try-PostJson $public $publicToken $test.path $test.body
  }
}

$result | ConvertTo-Json -Depth 10
