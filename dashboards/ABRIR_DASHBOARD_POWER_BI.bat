@echo off
chcp 65001 >nul
echo ========================================================
echo   WORKSHOP 3 - STREAMING ETL & ML (POWER BI DASHBOARD)
echo   Autor: Mauricio Taborda
echo ========================================================
echo.
echo [1/2] Verificando contenedores de base de datos y Kafka...
docker start happiness_postgres happiness_zookeeper happiness_kafka >nul 2>&1

echo [2/2] Abriendo Dashboard en Power BI Desktop...
set PBIT_PATH=%~dp0Workshop3_Happiness_Dashboard.pbit

explorer.exe "%PBIT_PATH%"

echo.
echo ========================================================
echo   ¡Dashboard lanzado exitosamente!
echo   * Servidor: localhost:6432
echo   * Base de datos: happiness_db
echo   * Usuario: postgres
echo   * Contraseña: postgres
echo.
echo   (Si Power BI te solicita credenciales, selecciona
echo    'Base de datos' e ingresa: postgres / postgres)
echo ========================================================
timeout /t 5
