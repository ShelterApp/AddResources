#script to invoke a particular azure function scraper.
param($scraperName)
$ErrorActionPreference = "Stop"

if ($null -eq $scraperName -or $scraperName -eq "") {
    "Invalid name of the scraper specified. Invoke script with parameter `-scraperName <name>` . Allowed names are: " /
    + "IRS" /
    + "HUD" /
    + "CareerOneStop" /
    + "NWHospitality" 
}

$vaultName = 'ShelterAppKeyVault'
$secretName = 'scraper-function-api-key'
$functionKey = (Get-AzKeyVaultSecret -VaultName $vaultName -Name "$secretName").SecretValue | ConvertFrom-SecureString -AsPlainText

$funcUrl = "https://shelterapp-scrapers.azurewebsites.net/api/$scraperName"

$headers = @{
    'Content-Type'  = "application/json"
    'x-function-key' = "$functionKey"
}
$requestBody = "{}" | ConvertTo-Json
$azureResponse = Invoke-RestMethod -Uri $funcUrl -Method Post -Body $requestBody -Headers $headers -StatusCodeVariable responseStatus -SkipHttpErrorCheck

if ($responseStatus -eq '202') {
    "$scraperName scraper started successfully."
} 
else {
    "Couldn't start the scraper. Status code: $responseStatus and Error: "
    $azureResponse | ConvertTo-Json | String
}
