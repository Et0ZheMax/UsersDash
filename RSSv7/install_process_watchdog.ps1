[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$CloExecutable,
    [string]$TaskName = 'BotOps-RssV7-Clo-Watchdog'
)

$ErrorActionPreference = 'Stop'
$watchdog = Join-Path $PSScriptRoot 'process_watchdog.ps1'
$rssLauncher = Join-Path $PSScriptRoot 'run_rssv7.cmd'

foreach ($path in @($watchdog, $rssLauncher, $CloExecutable)) {
    if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
        throw "Required file not found: $path"
    }
}

$arguments = '-NoProfile -NonInteractive -ExecutionPolicy Bypass -File "{0}" -RssLauncher "{1}" -CloExecutable "{2}"' -f `
    $watchdog, $rssLauncher, $CloExecutable
$action = New-ScheduledTaskAction `
    -Execute 'powershell.exe' `
    -Argument $arguments `
    -WorkingDirectory $PSScriptRoot
$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) `
    -RepetitionInterval (New-TimeSpan -Hours 1)
$principal = New-ScheduledTaskPrincipal -GroupId 'S-1-5-4' -RunLevel Highest
$settings = New-ScheduledTaskSettingsSet `
    -MultipleInstances IgnoreNew `
    -StartWhenAvailable `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 10)

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger $trigger `
    -Principal $principal `
    -Settings $settings `
    -Description 'Hourly recovery of RSSv7 and CLO in the interactive Windows session.' `
    -Force | Out-Null

Start-ScheduledTask -TaskName $TaskName
Write-Output "Installed and started scheduled task $TaskName."

