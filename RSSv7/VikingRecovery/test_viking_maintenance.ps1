param([Parameter(Mandatory=$true)][string]$Helper)
$ErrorActionPreference = 'Stop'
$testRoot = Join-Path ([IO.Path]::GetTempPath()) ('viking-maint-test-' + [Guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $testRoot | Out-Null
$statePath = Join-Path $testRoot 'maintenance.json'
$global:vikingTestTasks = @(
    [pscustomobject]@{TaskName='BotOps-AutoRecovery';TaskPath='\';Settings=@{Enabled=$true};Actions='wrapper.ps1'},
    [pscustomobject]@{TaskName='BotOps-AutoRecovery-TaskGuard';TaskPath='\';Settings=@{Enabled=$true};Actions='guard.ps1'},
    [pscustomobject]@{TaskName='BotOps-AutoRecovery-Worker';TaskPath='\';Settings=@{Enabled=$true};Actions='worker.ps1'},
    [pscustomobject]@{TaskName='OldStoppedBot';TaskPath='\';Settings=@{Enabled=$false};Actions='GnBots.exe'},
    [pscustomobject]@{TaskName='RSSv7';TaskPath='\';Settings=@{Enabled=$true};Actions='RssCounterWebV7.py'},
    [pscustomobject]@{TaskName='BotOps-GnBotsSplitTunnel';TaskPath='\';Settings=@{Enabled=$true};Actions='OpenVPN.ps1'}
)
$global:vikingTestStopOrder = @()
$global:vikingTestBot = $false
function Get-ScheduledTask {
    param($TaskName,$TaskPath)
    if ($TaskName) { return $global:vikingTestTasks | Where-Object TaskName -eq $TaskName }
    return $global:vikingTestTasks
}
function Disable-ScheduledTask { param($TaskName,$TaskPath); (Get-ScheduledTask $TaskName).Settings.Enabled=$false }
function Enable-ScheduledTask { param($TaskName,$TaskPath); (Get-ScheduledTask $TaskName).Settings.Enabled=$true }
function Stop-ScheduledTask { param($TaskName,$TaskPath,$ErrorAction); $global:vikingTestStopOrder += $TaskName }
function Get-CimInstance { param($ClassName); return @() }
function Get-Process {
    param($Name,$Id,$ErrorAction)
    if ($Id -or ($global:vikingTestBot -and $Name -contains 'GnBots')) {
        return [pscustomobject]@{SessionId=1}
    }
    return @()
}
function Stop-Process { param($Id,[switch]$Force,$ErrorAction); if ($Id) { throw 'Unexpected process in test' } }
function Start-Process { param($FilePath,$ArgumentList,$WorkingDirectory,$WindowStyle); $global:vikingTestBot=$true }

& $Helper -Action Pause -StatePath $statePath -GnBotsDir $testRoot | Out-Null
if ($global:vikingTestStopOrder[0] -ne 'BotOps-AutoRecovery-TaskGuard') { throw 'TaskGuard must stop first' }
if (@($global:vikingTestTasks | Where-Object { $_.TaskName -like 'BotOps-AutoRecovery*' -and $_.Settings.Enabled }).Count) {
    throw 'AutoRecovery task remained enabled'
}
if (-not (Get-ScheduledTask RSSv7).Settings.Enabled -or
    -not (Get-ScheduledTask BotOps-GnBotsSplitTunnel).Settings.Enabled) { throw 'Unrelated task was changed' }
& $Helper -Action Resume -StatePath $statePath -GnBotsDir $testRoot | Out-Null
if (@($global:vikingTestTasks | Where-Object { $_.TaskName -like 'BotOps-AutoRecovery*' -and -not $_.Settings.Enabled }).Count) {
    throw 'AutoRecovery task was not restored'
}
if ((Get-ScheduledTask OldStoppedBot).Settings.Enabled) { throw 'Originally disabled task was enabled' }
Write-Output 'Maintenance mocked pause/resume: PASS'

