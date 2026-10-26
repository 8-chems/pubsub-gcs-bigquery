# Dot-source at the start of every PowerShell session:  . .\env.ps1
if (-not $env:PROJECT_ID) { $env:PROJECT_ID = (gcloud config get-value project 2>$null) }
$env:GCP_PROJECT = $env:PROJECT_ID
$BUCKET = "$($env:PROJECT_ID)-landing"
Write-Host "PROJECT_ID=$($env:PROJECT_ID)  BUCKET=$BUCKET"
