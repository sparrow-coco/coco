function activate_venv {
    param (
        $VenvActFile
    )
    if ((-not ([string]::IsNullOrEmpty($VenvActFile))) -and (Test-Path -Path $VenvActFile -PathType Leaf)) {
        Invoke-Expression ".\$VenvActFile"
    }
}

function deactivate_venv {
    param (
        $VenvActFile
    )
    if ((-not ([string]::IsNullOrEmpty($VenvActFile))) -and (Test-Path -Path $VenvActFile -PathType Leaf)) {
        deactivate
    }
}

$VenvFile = "venv\Scripts\activate"
$Dir = "$(Set-Location ${PSScriptRoot}\..\..\; Get-Location)"
Set-Location $DIR
$CurrDate = "$(Get-Date -Format yyyyMMdd)"

activate_venv "$VenvFile"
python .\main.py .\configures_dev.json crawler facebook > logs\facebook_$CurrDate.log *>&1
deactivate_venv "$VenvFile"