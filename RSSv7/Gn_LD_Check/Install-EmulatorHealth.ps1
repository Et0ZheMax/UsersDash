param(
    [Parameter(Mandatory=$true)][string]$PythonPath,
    [string]$SourceDirectory='',
    [ValidatePattern('^[\p{L}\p{N}_. -]{1,40}$')][string]$ServerLabel=''
)
$ErrorActionPreference='Stop'
if(-not $SourceDirectory){$SourceDirectory=$PSScriptRoot}
if(-not ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole(
    [Security.Principal.WindowsBuiltInRole]::Administrator)){throw 'Administrator privileges required'}
$PythonPath=(Resolve-Path -LiteralPath $PythonPath).Path
$SourceDirectory=(Resolve-Path -LiteralPath $SourceDirectory).Path
$runner=Join-Path $SourceDirectory 'Run-EmulatorHealth.ps1'
if(-not (Test-Path -LiteralPath $runner)){throw 'Health runner is missing'}
$taskName='BotOps-EmulatorHealth'
$existing=Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
if($existing) {
    $data=Join-Path $SourceDirectory 'data'
    New-Item -ItemType Directory -Path $data -Force | Out-Null
    Export-ScheduledTask -TaskName $taskName | Set-Content -LiteralPath (
        Join-Path $data ('EmulatorHealth.before-'+(Get-Date -Format 'yyyyMMdd-HHmmss')+'.xml')) -Encoding UTF8
}
$arguments='-NoProfile -NonInteractive -ExecutionPolicy Bypass -File "{0}" -PythonPath "{1}" -SourceDirectory "{2}"' -f (
    $runner,$PythonPath,$SourceDirectory)
if($ServerLabel){$arguments+=' -ServerLabel "'+$ServerLabel+'"'}
$action=New-ScheduledTaskAction -Execute 'powershell.exe' -Argument $arguments -WorkingDirectory $SourceDirectory
$triggers=@(
    (New-ScheduledTaskTrigger -AtStartup),
    (New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) -RepetitionInterval (New-TimeSpan -Minutes 1))
)
$principal=New-ScheduledTaskPrincipal -UserId SYSTEM -LogonType ServiceAccount -RunLevel Highest
$settings=New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Minutes 2) `
    -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $triggers -Principal $principal `
    -Settings $settings -Description 'Read-only Android health and named account alerts' -Force | Out-Null
Start-ScheduledTask -TaskName $taskName
Write-Output 'BotOps-EmulatorHealth installed: SYSTEM, startup and every minute'
