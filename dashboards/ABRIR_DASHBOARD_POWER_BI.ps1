# Workshop 3 - Power BI Dashboard Launcher
$baseDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$pbitPath = Join-Path $baseDir "Workshop3_Happiness_Dashboard.pbit"
$pbiExe = "C:\Program Files\WindowsApps\Microsoft.MicrosoftPowerBIDesktop_2.159.1231.0_x64__8wekyb3d8bbwe\bin\pbidesktop.exe"

if (Test-Path $pbiExe) {
    Start-Process $pbiExe -ArgumentList "`"$pbitPath`""
} else {
    Start-Process $pbitPath
}
Write-Host "Dashboard lanzado exitosamente en Power BI Desktop." -ForegroundColor Green
