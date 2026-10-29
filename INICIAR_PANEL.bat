@echo off
REM Panel de control del pipeline ETL - doble clic para abrir
cd /d "%~dp0"
echo ============================================================
echo   PANEL DE CONTROL - PIPELINE ETL
echo   Abriendo en el navegador...
echo ============================================================
python app\servidor.py
if errorlevel 1 (
  echo.
  echo No se pudo iniciar con 'python'. Probando con 'py'...
  py app\servidor.py
)
if errorlevel 1 (
  echo.
  echo No se encontro Python. Abre dashboard_offline.html con doble clic:
  echo   no necesita Python ni internet.
  pause
)
