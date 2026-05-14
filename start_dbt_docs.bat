@echo off
cd /d "%~dp0dbt_brasileirao"
dbt docs serve --port 8082
pause
