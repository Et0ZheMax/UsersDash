[CmdletBinding()]
param(
    [string]$RssLauncher = (Join-Path $PSScriptRoot 'run_rssv7.cmd'),
    [Parameter(Mandatory = $true)]
    [string]$CloExecutable,
    [string]$LogPath = (Join-Path $PSScriptRoot 'logs\process-watchdog.log')
)

$ErrorActionPreference = 'Stop'
$mutex = $null
$hasMutex = $false
$transcriptPath = Join-Path $env:TEMP 'rssv7-clo-watchdog-task.log'

try {
    Start-Transcript -Path $transcriptPath -Append -Force | Out-Null
}
catch {
    # Transcript is diagnostic only; recovery must continue if it is unavailable.
}

function Write-WatchdogLog {
    param([string]$Message)

    $directory = Split-Path -Parent $LogPath
    if ($directory) {
        New-Item -ItemType Directory -Path $directory -Force | Out-Null
    }
    $line = '{0} {1}' -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), $Message
    try {
        Add-Content -LiteralPath $LogPath -Value $line -Encoding UTF8
    }
    catch {
        $fallbackDirectory = Join-Path $env:LOCALAPPDATA 'BotOps'
        New-Item -ItemType Directory -Path $fallbackDirectory -Force | Out-Null
        Add-Content -LiteralPath (Join-Path $fallbackDirectory 'process-watchdog.log') `
            -Value $line -Encoding UTF8
    }
    Write-Output $line
}

function Get-WatchedProcesses {
    Get-CimInstance Win32_Process -ErrorAction SilentlyContinue
}

try {
    $mutex = New-Object System.Threading.Mutex($false, 'Local\BotOps-RssV7-Clo-Watchdog')
    $hasMutex = $mutex.WaitOne(0)
    if (-not $hasMutex) {
        Write-WatchdogLog 'SKIP: another watchdog instance is running.'
        exit 0
    }

    $processes = @(Get-WatchedProcesses)
    $rssRunning = @(
        $processes | Where-Object {
            $_.Name -in @('python.exe', 'pythonw.exe', 'py.exe') -and
            $_.CommandLine -match '(?i)(^|[\\/\s])RssCounterWebV7\.py([\s"'']|$)'
        }
    ).Count -gt 0
    $cloRunning = @(
        $processes | Where-Object {
            $_.Name -ieq 'clo.exe' -and $_.CommandLine -match '(?i)(^|\s)run(\s|$)'
        }
    ).Count -gt 0

    if ($rssRunning) {
        Write-WatchdogLog 'OK: RSSv7 is running.'
    }
    else {
        if (-not (Test-Path -LiteralPath $RssLauncher -PathType Leaf)) {
            throw "RSSv7 launcher not found: $RssLauncher"
        }
        Start-Process -FilePath $env:ComSpec `
            -ArgumentList @('/d', '/c', ('"{0}"' -f $RssLauncher)) `
            -WorkingDirectory (Split-Path -Parent $RssLauncher) `
            -WindowStyle Minimized
        Write-WatchdogLog "RECOVERED: RSSv7 started with $RssLauncher."
    }

    if ($cloRunning) {
        Write-WatchdogLog 'OK: CLO is running.'
    }
    else {
        if (-not (Test-Path -LiteralPath $CloExecutable -PathType Leaf)) {
            throw "CLO executable not found: $CloExecutable"
        }
        Start-Process -FilePath $CloExecutable `
            -ArgumentList 'run' `
            -WorkingDirectory (Split-Path -Parent $CloExecutable) `
            -WindowStyle Minimized
        Write-WatchdogLog "RECOVERED: CLO started with $CloExecutable run."
    }
}
catch {
    Write-WatchdogLog "ERROR: $($_.Exception.Message)"
    exit 1
}
finally {
    if ($hasMutex -and $mutex) {
        $mutex.ReleaseMutex()
    }
    if ($mutex) {
        $mutex.Dispose()
    }
    try {
        Stop-Transcript | Out-Null
    }
    catch {
        # No transcript was started.
    }
}

