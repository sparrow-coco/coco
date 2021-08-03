Set-Location ${PSScriptRoot}

# somehow does not work
Start-Job -FilePath "${PSScriptRoot}\start_facebook.ps1"
Start-Job -FilePath "${PSScriptRoot}\start_facebook_search.ps1"
Start-Job -FilePath "${PSScriptRoot}\start_twitter.ps1"
Start-Job -FilePath "${PSScriptRoot}\start_twitter_search.ps1"