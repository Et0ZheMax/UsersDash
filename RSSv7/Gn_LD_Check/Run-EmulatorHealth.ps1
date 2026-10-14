param(
    [Parameter(Mandatory=$true)][string]$PythonPath,
    [string]$SourceDirectory='',
    [ValidatePattern('^[\p{L}\p{N}_. -]{1,40}$')][string]$ServerLabel=''
)
$ErrorActionPreference='Stop'
if(-not $SourceDirectory){$SourceDirectory=$PSScriptRoot}
$env:PYTHONUTF8='1'
$data=Join-Path $SourceDirectory 'data'
New-Item -ItemType Directory -Path $data -Force | Out-Null
$log=Join-Path $data 'emulator_health.runtime.log'
if((Test-Path -LiteralPath $log) -and (Get-Item -LiteralPath $log).Length -gt 10MB) {
    Move-Item -LiteralPath $log -Destination ($log+'.previous') -Force
}
$cliArgs=@((Join-Path $SourceDirectory 'emulator_health.py'))
if($ServerLabel){$cliArgs+=@('--server-label',$ServerLabel)}
Add-Content -LiteralPath $log -Value ('RUN '+(Get-Date -Format o)) -Encoding UTF8
& $PythonPath @cliArgs 2>&1 | Out-File -LiteralPath $log -Append -Encoding UTF8
exit $LASTEXITCODE
