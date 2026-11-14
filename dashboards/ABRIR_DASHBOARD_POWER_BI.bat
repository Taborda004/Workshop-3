@echo off
echo ========================================================
echo Abriendo Dashboard de Power BI - Workshop 3...
echo ========================================================
set PBIT_PATH=%~dp0Workshop3_Happiness_Dashboard.pbit
set PBIP_PATH=%~dp0Workshop3_Happiness.pbip
set PBI_EXE=C:\Program Files\WindowsApps\Microsoft.MicrosoftPowerBIDesktop_2.159.1231.0_x64__8wekyb3d8bbwe\bin\pbidesktop.exe

if exist "%PBI_EXE%" (
    start "" "%PBI_EXE%" "%PBIT_PATH%"
) else (
    start "" "%PBIT_PATH%"
)
echo Dashboard lanzado exitosamente en Power BI Desktop.
