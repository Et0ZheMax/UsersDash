param(
    [ValidateSet('Inspect','Pause','Resume')][string]$Action = 'Inspect',
    [Parameter(Mandatory=$true)][string]$StatePath,
    [string]$GnBotsDir = 'C:\Program Files\GnBots'
)
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
[Console]::OutputEncoding = New-Object System.Text.UTF8Encoding($false)
$taskPattern = '(?i)GnBots\.exe|gnrestarter|Gn_Ld_Check|LD_check\.py|LD_problems\.py|BOT_RESTART|BOT_STOP|MONITOR_onlyLD|taskSTOP\.py'
$processPattern = '(?i)Gn_Ld_Check|LD_check\.py|LD_problems\.py|BOT_RESTART|BOT_STOP|MONITOR_onlyLD|taskSTOP\.py'

function Save-State($value) {
    $temporary = $StatePath + '.tmp'
    $value | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $temporary -Encoding UTF8
    Move-Item -LiteralPath $temporary -Destination $StatePath -Force
}

if ($Action -eq 'Inspect') {
    $tasks = @(Get-ScheduledTask | Where-Object {
        ($_.Actions | Out-String) -match $taskPattern
    } | ForEach-Object {
        @{Name=$_.TaskName; Path=$_.TaskPath; Enabled=[bool]$_.Settings.Enabled}
    })
    @{tasks=$tasks; session_id=(Get-Process -Id $PID).SessionId; administrator=([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole(
        [Security.Principal.WindowsBuiltInRole]::Administrator
    )} | ConvertTo-Json -Depth 5 -Compress
    exit 0
}

if ($Action -eq 'Pause') {
    if (Test-Path -LiteralPath $StatePath) {
        $existing = Get-Content -LiteralPath $StatePath -Raw | ConvertFrom-Json
        if ($existing.phase -ne 'restored') { throw 'Сначала восстановите предыдущую остановку сервера' }
    }
    $tasks = @(Get-ScheduledTask | Where-Object {
        ($_.Actions | Out-String) -match $taskPattern
    } | ForEach-Object {
        @{Name=$_.TaskName; Path=$_.TaskPath; Enabled=[bool]$_.Settings.Enabled}
    })
    $monitors = @(Get-CimInstance Win32_Process | Where-Object {
        $_.Name -match '^pythonw?\.exe$' -and $_.CommandLine -match $processPattern
    } | ForEach-Object {
        $arguments = $_.CommandLine -replace '^\s*("[^"]+"|\S+)\s*',''
        @{Id=$_.ProcessId; Executable=$_.ExecutablePath; Arguments=$arguments}
    })
    $state = @{phase='pausing'; tasks=$tasks; monitors=$monitors; bot_started=$false}
    Save-State $state
    foreach ($task in $tasks) {
        if ($task.Enabled) {
            Disable-ScheduledTask -TaskName $task.Name -TaskPath $task.Path | Out-Null
            Stop-ScheduledTask -TaskName $task.Name -TaskPath $task.Path -ErrorAction SilentlyContinue
        }
    }
    foreach ($monitor in $monitors) { Stop-Process -Id $monitor.Id -Force -ErrorAction SilentlyContinue }
    Get-Process gnrestarter -ErrorAction SilentlyContinue | Stop-Process -Force
    foreach ($bot in @(Get-Process GnBots -ErrorAction SilentlyContinue)) {
        $null = $bot.CloseMainWindow()
        if (-not $bot.WaitForExit(10000)) { Stop-Process -Id $bot.Id -Force }
    }
    if (Get-Process GnBots,gnrestarter -ErrorAction SilentlyContinue) { throw 'GnBots не остановлен' }
    $state.phase='paused'
    Save-State $state
    @{ok=$true; phase='paused'; tasks=$tasks.Count} | ConvertTo-Json -Compress
    exit 0
}

$state = Get-Content -LiteralPath $StatePath -Raw | ConvertFrom-Json
foreach ($task in $state.tasks) {
    $current = Get-ScheduledTask -TaskName $task.Name -TaskPath $task.Path
    if ($task.Enabled) { Enable-ScheduledTask -TaskName $task.Name -TaskPath $task.Path | Out-Null }
    else { Disable-ScheduledTask -TaskName $task.Name -TaskPath $task.Path | Out-Null }
    $current = Get-ScheduledTask -TaskName $task.Name -TaskPath $task.Path
    if ([bool]$current.Settings.Enabled -ne [bool]$task.Enabled) { throw 'Не восстановлено состояние задачи' }
}
if (-not (Get-Process GnBots -ErrorAction SilentlyContinue)) {
    Start-Process -FilePath (Join-Path $GnBotsDir 'GnBots.exe') -ArgumentList '-start' -WorkingDirectory $GnBotsDir -WindowStyle Hidden
}
$sessionId = (Get-Process -Id $PID).SessionId
if (-not (Get-Process GnBots -ErrorAction SilentlyContinue | Where-Object { $_.SessionId -eq $sessionId })) {
    throw 'Не подтверждён запуск GnBots в текущей интерактивной сессии'
}
foreach ($monitor in $state.monitors) {
    $alreadyRunning = Get-CimInstance Win32_Process | Where-Object {
        $_.ExecutablePath -eq $monitor.Executable -and $_.CommandLine -like ('*'+$monitor.Arguments+'*')
    }
    if (-not $alreadyRunning) {
        Start-Process -FilePath $monitor.Executable -ArgumentList $monitor.Arguments -WindowStyle Hidden
    }
}
$state.phase='restored'
$state.bot_started=$true
Save-State $state
@{ok=$true; phase='restored'} | ConvertTo-Json -Compress
