@echo off
REM Instala las librerias del pipeline (una sola vez)
cd /d "%~dp0"
python -m pip install -r requirements.txt || py -m pip install -r requirements.txt
echo.
echo Listo. Ahora abre INICIAR_PANEL.bat
pause
