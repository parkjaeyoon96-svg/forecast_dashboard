@echo off
chcp 65001 >nul
cd /d "%~dp0"

echo ============================================================
echo   Current Year Data Processing (Full Pipeline)
echo ============================================================
echo.

python scripts\run_current_year_pipeline.py

if errorlevel 1 (
    echo.
    echo ============================================================
    echo   Failed (Error code: %errorlevel%)
    echo ============================================================
    echo.
    echo Please check the error messages above.
    pause
    exit /b %errorlevel%
)

pause
exit /b 0
