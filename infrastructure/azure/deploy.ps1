param($envName, $deployName = 'deployment')
$ErrorActionPreference = "Stop"

if (Get-Module -Name AzureRM -ListAvailable) {
    Write-Warning -Message ('Az module not installed. Having both the AzureRM and ' +
      'Az modules installed at the same time is not supported.')
} else {
    Install-Module -Name Az -AllowClobber -Scope CurrentUser
}

"Deploying ShelterApp Infrastructure..."

# provisioning azure function app
New-AzResourceGroupDeployment -ResourceGroupName 'ShelterApp' -TemplateFile "$PSScriptRoot/azure_function_app/template.json" -TemplateParameterFile "$PSScriptRoot/azure_function_app/parameters.json"
