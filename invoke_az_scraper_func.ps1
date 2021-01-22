param([Parameter(Mandatory)][string]$scraperName)

# Script to invoke a particular azure function scraper.
$ErrorActionPreference = "Stop"

function ConsoleWriteline($arg = "") {
    Write-Information $arg -InformationAction Continue
}

function CheckPowershellVersion() {    
    $pwshVersion = (Get-Host).Version
    if ($pwshVersion.Major -lt 7) {
        "Current Powershell version"
        $pwshVersion #display current version.
        "This script requires powershell version 7 or more. Install latest powershell version from here: https://github.com/PowerShell/PowerShell/releases"
        exit
    }
}

# *************************************************
# All functions go above this line.
# *************************************************

CheckPowershellVersion

if (Get-Module -Name AzureRM -ListAvailable) {
    Write-Warning -Message ('Az module not installed. Having both the AzureRM and ' +
      'Az modules installed at the same time is not supported.')
} else {
    Install-Module -Name Az -AllowClobber -Scope CurrentUser
}

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

$funcUrl = "https://shelterapp-scrapers.azurewebsites.net/admin/functions/$scraperName"
$headers = @{
    'x-functions-key' = "$functionKey"
}
$requestBody = "{}"
$azureResponse = Invoke-RestMethod -Uri $funcUrl -Method 'Post'-Body $requestBody -Headers $headers -ContentType 'application/json' -StatusCodeVariable responseStatus

if ($responseStatus -eq '202') {
    "$scraperName scraper started successfully."
} 
else {
    "Couldn't start the scraper. Status code: $responseStatus and Error: "
    # $azureResponseJson = $azureResponse | ConvertTo-Json
    # ConsoleWriteline $azureResponseJson
    ConsoleWriteline $azureResponse
    return 1 #Failed.
}

return 0 #Success