$ErrorActionPreference='Stop'
$local='http://localhost:4000'
$rail='https://terrific-love-production-10ec.up.railway.app'
$cred=@{username='admin';password='admin123'} | ConvertTo-Json
$lt=(Invoke-RestMethod "$local/api/auth/login" -Method Post -ContentType 'application/json' -Body $cred).token
$rt=(Invoke-RestMethod "$rail/api/auth/login" -Method Post -ContentType 'application/json' -Body $cred).token
$lh=@{Authorization="Bearer $lt"}
$rh=@{Authorization="Bearer $rt"}

function Normalize([object]$s){
  if ($null -eq $s) { return '' }
  return ([string]$s).Trim().ToLowerInvariant()
}
function Nz([object]$v, [object]$d){
  if ($null -eq $v) { return $d }
  return $v
}

$rcats = Invoke-RestMethod "$rail/api/project-catalog" -Headers $rh
$tmp = $rcats | Where-Object { $_.nomProjet -eq 'TEST-CATALOG-TMP' } | Select-Object -First 1
if ($tmp) {
  try {
    Invoke-RestMethod "$rail/api/project-catalog/$($tmp.id)" -Method Delete -Headers $rh | Out-Null
    Write-Host "cleanup: deleted TEST-CATALOG-TMP (id=$($tmp.id))"
  } catch {
    Write-Host "cleanup: delete endpoint not available, keeping TEST-CATALOG-TMP"
  }
} else {
  Write-Host 'cleanup: no TEST-CATALOG-TMP found'
}

$localProjects = Invoke-RestMethod "$local/api/projects" -Headers $lh
$railProjects  = Invoke-RestMethod "$rail/api/projects" -Headers $rh

function FindRailProjectId($lp){
  $m = $railProjects | Where-Object {
    ((Normalize $_.nomProjet) -eq (Normalize $lp.nomProjet)) -and
    ((Normalize $_.prefecture) -eq (Normalize $lp.prefecture)) -and
    ((Normalize $_.nomSite) -eq (Normalize $lp.nomSite))
  } | Select-Object -First 1
  if ($m) { return [int]$m.id }
  return $null
}

$localMat = Invoke-RestMethod "$local/api/material-catalog" -Headers $lh
$railMat  = Invoke-RestMethod "$rail/api/material-catalog" -Headers $rh
$matAdded=0; $matErr=0
foreach($m in $localMat){
  $exists = $railMat | Where-Object {
    (Normalize $_.projectFolder) -eq (Normalize $m.projectFolder) -and
    (Normalize $_.materialName) -eq (Normalize $m.materialName) -and
    (Normalize $_.unite) -eq (Normalize $m.unite)
  } | Select-Object -First 1
  if(-not $exists){
    $body=@{
      projectFolder=$m.projectFolder
      materialName=$m.materialName
      unite=$m.unite
      quantiteParBatiment=[double](Nz $m.quantiteParBatiment 0)
      prixUnitaire=[double](Nz $m.prixUnitaire 0)
      notes=[string](Nz $m.notes '')
    } | ConvertTo-Json
    try {
      Invoke-RestMethod "$rail/api/material-catalog" -Method Post -Headers $rh -ContentType 'application/json' -Body $body | Out-Null
      $matAdded++
    } catch {
      $matErr++
      Write-Host "material error: $($_.Exception.Message)"
    }
  }
}
Write-Host "material-catalog sync => added:$matAdded errors:$matErr"

$railMat  = Invoke-RestMethod "$rail/api/material-catalog" -Headers $rh

$localProg = Invoke-RestMethod "$local/api/project-progress" -Headers $lh
$railProg  = Invoke-RestMethod "$rail/api/project-progress" -Headers $rh
$progAdded=0; $progErr=0
foreach($p in $localProg){
  $lp = $localProjects | Where-Object { [int]$_.id -eq [int]$p.projectId } | Select-Object -First 1
  if(-not $lp){ continue }
  $rid = FindRailProjectId $lp
  if(-not $rid){ continue }

  $exists = $railProg | Where-Object {
    [int]$_.projectId -eq [int]$rid -and
    (Normalize $_.stage) -eq (Normalize $p.stage) -and
    (Normalize $_.title) -eq (Normalize $p.title)
  } | Select-Object -First 1

  if(-not $exists){
    $body=@{
      projectId=$rid
      stage=$p.stage
      title=$p.title
      note=[string](Nz $p.note '')
      percentage=[double](Nz $p.progressPercent 0)
      materialsUsed=''
      laborCount=0
      dateEtape=$p.createdAt
    } | ConvertTo-Json
    try {
      Invoke-RestMethod "$rail/api/project-progress" -Method Post -Headers $rh -ContentType 'application/json' -Body $body | Out-Null
      $progAdded++
    } catch {
      $progErr++
      Write-Host "progress error: $($_.Exception.Message)"
    }
  }
}
Write-Host "project-progress sync => added:$progAdded errors:$progErr"

$railProg  = Invoke-RestMethod "$rail/api/project-progress" -Headers $rh

$localExp = Invoke-RestMethod "$local/api/expenses" -Headers $lh
$railExp  = Invoke-RestMethod "$rail/api/expenses" -Headers $rh
$expAdded=0; $expErr=0
foreach($e in $localExp){
  $lp = $localProjects | Where-Object { [int]$_.id -eq [int]$e.projetId } | Select-Object -First 1
  $rid = $null
  if($lp){ $rid = FindRailProjectId $lp }

  $exists = $railExp | Where-Object {
    (Normalize $_.description) -eq (Normalize $e.description) -and
    [double](Nz $_.quantite 0) -eq [double](Nz $e.quantite 0) -and
    [double](Nz $_.prixUnitaire 0) -eq [double](Nz $e.prixUnitaire 0) -and
    (Normalize $_.categorie) -eq (Normalize $e.categorie)
  } | Select-Object -First 1

  if(-not $exists){
    $body=@{
      projectId=$rid
      description=$e.description
      quantity=[double](Nz $e.quantite 0)
      unitPrice=[double](Nz $e.prixUnitaire 0)
      supplier=[string](Nz $e.fournisseur '')
      category=[string](Nz $e.categorie 'autres')
    } | ConvertTo-Json
    try {
      Invoke-RestMethod "$rail/api/expenses" -Method Post -Headers $rh -ContentType 'application/json' -Body $body | Out-Null
      $expAdded++
    } catch {
      $expErr++
      Write-Host "expense error: $($_.Exception.Message)"
    }
  }
}
Write-Host "expenses sync => added:$expAdded errors:$expErr"

$eps=@('/api/project-catalog','/api/project-folders','/api/projects','/api/material-catalog','/api/project-progress','/api/purchase-orders','/api/expenses')
foreach($ep in $eps){
  $lc=(Invoke-RestMethod "$local$ep" -Headers $lh).Count
  $rc=(Invoke-RestMethod "$rail$ep" -Headers $rh).Count
  Write-Host "$ep => local:$lc railway:$rc"
}
