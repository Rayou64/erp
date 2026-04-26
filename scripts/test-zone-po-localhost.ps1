$ErrorActionPreference = 'Stop'

$base = 'http://localhost:4000'

$loginBody = @{ username = 'admin'; password = 'admin123' } | ConvertTo-Json
$login = Invoke-RestMethod -Method Post -Uri "$base/api/auth/login" -ContentType 'application/json' -Body $loginBody
$token = $login.token
$headers = @{ Authorization = "Bearer $token" }
$me = Invoke-RestMethod -Method Get -Uri "$base/api/auth/me" -Headers $headers

$catalog = Invoke-RestMethod -Method Get -Uri "$base/api/material-catalog" -Headers $headers
if (-not $catalog -or $catalog.Count -eq 0) {
  throw 'Catalogue vide: test impossible'
}

$first = $catalog | Select-Object -First 1
$nomProjet = [string]$first.projectFolder
$materialName = [string]$first.materialName
$stage = if ([string]::IsNullOrWhiteSpace([string]$first.notes)) { 'Fondation' } else { ([string]$first.notes -split '[,;|/]')[0].Trim() }
if ([string]::IsNullOrWhiteSpace($stage)) { $stage = 'Fondation' }

$zone = 'ZONE-TEST-' + (Get-Date -Format 'yyyyMMddHHmmss')
$warehouseId = 'entrepot-plateau'

$reqBody = @{
  zoneName = $zone
  nomProjet = $nomProjet
  demandeur = 'Test Localhost Zone'
  etapeApprovisionnement = $stage
  warehouseId = $warehouseId
  description = 'Test API demande par zone'
  lines = @(
    @{
      itemName = $materialName
      quantiteDemandee = 2
    }
  )
} | ConvertTo-Json -Depth 8

$reqResp = Invoke-RestMethod -Method Post -Uri "$base/api/material-requests/auto-stage" -Headers $headers -ContentType 'application/json' -Body $reqBody
$created = @($reqResp.createdRequests)
if ($created.Count -eq 0) {
  throw 'Aucune demande créée'
}

$reqId = [int]$created[0].id
$allRequests = Invoke-RestMethod -Method Get -Uri "$base/api/material-requests" -Headers $headers
$requestDetail = @($allRequests) | Where-Object { [int]$_.id -eq $reqId } | Select-Object -First 1
if (-not $requestDetail) {
  throw "Demande créée introuvable dans la liste (id=$reqId)"
}

# Test BC lié seulement à la zone (sans siteId, nomSiteManuel = zone)
$poBodyZoneOnly = @{
  creePar = 'admin'
  fournisseur = 'Fournisseur Zone Test'
  dateCommande = (Get-Date -Format 'yyyy-MM-dd')
  projetId = $requestDetail.projetId
  nomProjetManuel = $nomProjet
  siteId = $null
  nomSiteManuel = "Zone: $zone"
  warehouseId = $warehouseId
  etapeApprovisionnement = $stage
  items = @(
    @{
      materialRequestId = $null
      article = $materialName
      quantite = 1
      prixUnitaire = 1000
      totalLigne = 1000
    }
  )
} | ConvertTo-Json -Depth 10

$poZoneOnly = Invoke-RestMethod -Method Post -Uri "$base/api/purchase-orders" -Headers $headers -ContentType 'application/json' -Body $poBodyZoneOnly

# Test BC depuis la demande de zone (item lié)
$poBodyFromReq = @{
  creePar = 'admin'
  fournisseur = 'Fournisseur Zone Demande'
  dateCommande = (Get-Date -Format 'yyyy-MM-dd')
  items = @(
    @{
      materialRequestId = $reqId
      article = $materialName
      quantite = 1
      prixUnitaire = 1200
      totalLigne = 1200
    }
  )
  warehouseId = $warehouseId
  etapeApprovisionnement = $stage
} | ConvertTo-Json -Depth 10

$poFromReqResult = $null
try {
  $poFromReqResult = Invoke-RestMethod -Method Post -Uri "$base/api/purchase-orders" -Headers $headers -ContentType 'application/json' -Body $poBodyFromReq
} catch {
  $poFromReqResult = @{ error = $_.ErrorDetails.Message }
}

$result = [ordered]@{
  loginUser = $me.username
  loginRole = $me.role
  chosenNomProjet = $nomProjet
  chosenStage = $stage
  chosenMaterial = $materialName
  zoneName = $zone
  demandeZone = [ordered]@{
    requestId = $reqId
    projetId = $requestDetail.projetId
    projetNom = $requestDetail.projetNom
    siteLabel = $requestDetail.numeroMaison
    stage = $requestDetail.etapeApprovisionnement
    warehouseId = $requestDetail.warehouseId
  }
  bonCommandeZoneOnly = [ordered]@{
    id = $poZoneOnly.id
    statut = $poZoneOnly.statut
    projetId = $poZoneOnly.projetId
    siteId = $poZoneOnly.siteId
    nomProjet = $poZoneOnly.nomProjet
    numeroMaison = $poZoneOnly.numeroMaison
    montantTotal = $poZoneOnly.montantTotal
  }
  bonCommandeDepuisDemandeZone = $poFromReqResult
}

$result | ConvertTo-Json -Depth 10
