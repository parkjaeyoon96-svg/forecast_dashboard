@echo off
REM ============================================================
REM   Previous Year / Plan Data Update Batch File
REM ============================================================
REM
REM Execution Process:
REM   Step 1: Download previous year data (Snowflake)
REM   Step 2: Process previous year data
REM   Step 3: Process plan data
REM   Step 4: Calculate weighted progress rate
REM
REM Note: Direct cost rate extraction is performed in 
REM       current_year_data_processing.bat
REM ============================================================

REM UTF-8 encoding setting
chcp 65001 >nul

REM Change to script directory
cd /d "%~dp0"

REM Set Python output encoding to UTF-8
set PYTHONIOENCODING=utf-8

echo ============================================================
echo   Previous Year / Plan Data Update
echo ============================================================
echo.

REM Get analysis month from user input
set /p ANALYSIS_MONTH="Enter analysis month (YYYYMM, e.g., 202601): "

REM Check if input is provided
if "%ANALYSIS_MONTH%"=="" (
    echo.
    echo [INFO] No analysis month provided. Using latest folder from raw/
    echo.
    python scripts\run_previous_year_plan_update.py
) else (
    echo.
    echo [INFO] Using analysis month: %ANALYSIS_MONTH%
    echo.
    python scripts\run_previous_year_plan_update.py %ANALYSIS_MONTH%
)

REM Error handling: If Python script execution fails
if errorlevel 1 (
    echo.
    echo ============================================================
    echo   FAILED (Error code: %errorlevel%)
    echo ============================================================
    echo.
    echo Please check the error message above.
    echo.
    echo Common issues:
    echo   - Analysis month folder not found in raw/
    echo   - Snowflake connection failed
    echo   - Missing master files in Master/
    echo.
    pause
    exit /b %errorlevel%
)

REM Success message
echo.
echo ============================================================
echo   COMPLETED!
echo ============================================================
echo.
echo Generated files are in raw/{YYYYMM}/previous_year/ and raw/{YYYYMM}/plan/
echo.

pause
exit /b 0
