@echo off
setlocal EnableExtensions EnableDelayedExpansion
chcp 65001 >nul 2>&1
cd /d "%~dp0"

echo ============================================================
echo   Dashboard JSON Generation
echo ============================================================
echo.

REM Set Python path: prioritize virtual environment, fallback to system python
set "PYTHON_CMD="
if exist "%~dp0Forcast_venv\Scripts\python.exe" (
    set "PYTHON_CMD=%~dp0Forcast_venv\Scripts\python.exe"
) else (
    where python >nul 2>&1
    if %errorlevel% equ 0 (
        set "PYTHON_CMD=python"
    ) else (
        echo [ERROR] Python not found. Please create Forcast_venv or check PATH.
        pause
        exit /b 1
    )
)
set "PYTHONIOENCODING=utf-8"
set "PYTHONUNBUFFERED=1"

REM Auto-select latest or manual input
set PIPELINE_ERROR=0
set ANALYSIS_MONTH=
set UPDATE_DATE=

set /p USE_LATEST="Use latest files? (Y/N): "

if /i "!USE_LATEST!"=="Y" (
    echo.
    echo Running in auto-select latest file mode...
    echo.
    for /f "delims=" %%d in ('dir /b /ad /o-n "raw" 2^>nul ^| findstr /r "^[0-9][0-9][0-9][0-9][0-9][0-9]$"') do (
        if exist "raw\%%d\current_year" (
            for /f "delims=" %%f in ('dir /b /ad /o-n "raw\%%d\current_year" 2^>nul ^| findstr /r "^[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]$"') do (
                if "!UPDATE_DATE!"=="" (
                    set "ANALYSIS_MONTH=%%d"
                    set "UPDATE_DATE=%%f"
                )
            )
        )
    )
) else if /i "!USE_LATEST!"=="N" (
    echo.
    echo Please enter Analysis Month and Update Date.
    set /p ANALYSIS_MONTH="Analysis Month (YYYYMM, e.g., 202512): "
    set /p UPDATE_DATE="Update Date (YYYYMMDD, e.g., 20251208): "
) else (
    echo.
    echo Invalid input. Please enter Y or N.
    pause
    exit /b 1
)

REM Validation
if not defined ANALYSIS_MONTH (
    echo [ERROR] Analysis month not found.
    echo Please check raw\YYYYMM\current_year\YYYYMMDD structure.
    pause
    exit /b 1
)

if not defined UPDATE_DATE (
    echo [ERROR] Update date not found.
    echo Please check raw\!ANALYSIS_MONTH!\current_year\YYYYMMDD structure.
    pause
    exit /b 1
)

echo.
echo ============================================================
echo   Analysis Month: !ANALYSIS_MONTH!
echo   Update Date: !UPDATE_DATE!
echo ============================================================
echo.

set DATE_STR=!UPDATE_DATE!

REM === Common Pipeline Start ===

call "%PYTHON_CMD%" scripts\update_brand_kpi.py !DATE_STR!
set STEP_ERR=!errorlevel!
if !STEP_ERR! neq 0 (
    echo [Step 1] Failed (Error code: !STEP_ERR!)
    set PIPELINE_ERROR=!STEP_ERR!
) else (
    echo [Step 1] Completed
)
echo.

echo [Step 1.5] Weekly Sales Trend Download ^(required for Step 2^)
set DATE_FORMATTED_STEP1_5=!DATE_STR:~0,4!-!DATE_STR:~4,2!-!DATE_STR:~6,2!
call "%PYTHON_CMD%" scripts\download_weekly_sales_trend.py !DATE_FORMATTED_STEP1_5!
set STEP_ERR=!errorlevel!
if !STEP_ERR! neq 0 (
    echo [Step 1.5] Failed (Error code: !STEP_ERR!)
    set PIPELINE_ERROR=!STEP_ERR!
) else (
    echo [Step 1.5] Completed
)
echo.

call "%PYTHON_CMD%" scripts\update_overview_data.py !DATE_STR!
set STEP_ERR=!errorlevel!
if !STEP_ERR! neq 0 (
    echo [Step 2] Failed (Error code: !STEP_ERR!)
    set PIPELINE_ERROR=!STEP_ERR!
) else (
    echo [Step 2] Completed
)
echo.

call "%PYTHON_CMD%" scripts\create_brand_pl_data.py !DATE_STR!
set STEP_ERR=!errorlevel!
if !STEP_ERR! neq 0 (
    echo [Step 3] Failed (Error code: !STEP_ERR!)
    set PIPELINE_ERROR=!STEP_ERR!
) else (
    echo [Step 3] Completed
)
echo.

call "%PYTHON_CMD%" scripts\update_brand_radar.py !DATE_STR!
set STEP_ERR=!errorlevel!
if !STEP_ERR! neq 0 (
    echo [Step 4] Failed (Error code: !STEP_ERR!)
    set PIPELINE_ERROR=!STEP_ERR!
) else (
    echo [Step 4] Completed
)
echo.

set YEAR_MONTH=!DATE_STR:~0,6!
call "%PYTHON_CMD%" scripts\process_channel_profit_loss.py --base-date !DATE_STR! --target-month !YEAR_MONTH! --format dashboard
set STEP_ERR=!errorlevel!
if !STEP_ERR! neq 0 (
    echo [Step 5] Failed (Error code: !STEP_ERR!)
    set PIPELINE_ERROR=!STEP_ERR!
) else (
    echo [Step 5] Completed
)
echo.

echo [Step 6] Stock Analysis Download
set DATE_FORMATTED=!DATE_STR:~0,4!-!DATE_STR:~4,2!-!DATE_STR:~6,2!
call "%PYTHON_CMD%" scripts\download_brand_stock_analysis.py --update-date !DATE_FORMATTED!
set STOCK_ERR=!errorlevel!

REM Always generate stock analysis from CSV to include aggregated data (clothingItemRatesOverall, etc.)
echo.
echo [Step 7-Post] Generating aggregated stock analysis from CSV
call "%PYTHON_CMD%" scripts\generate_brand_stock_analysis.py !DATE_STR!
set GEN_ERR=!errorlevel!
if !GEN_ERR! neq 0 (
    echo [Step 7-Post] Failed (Error code: !GEN_ERR!)
    set PIPELINE_ERROR=!GEN_ERR!
) else (
    echo [Step 7-Post] Success - Aggregated data generated
)
echo.

echo [Step 7.5] Downloading previous year treemap data for YOY calculation
call "%PYTHON_CMD%" scripts\download_previous_year_treemap_data.py !DATE_STR!
set STEP_ERR=!errorlevel!
if !STEP_ERR! neq 0 (
    echo [Step 7.5] Failed (Error code: !STEP_ERR!)
    echo [Warning] Previous year data download failed. YOY calculation will be skipped.
) else (
    echo [Step 7.5] Completed
)
echo.

echo [Step 8] Generating treemap JSON with YOY data
call "%PYTHON_CMD%" scripts\create_treemap_data_v2.py !DATE_STR!
set STEP_ERR=!errorlevel!
if !STEP_ERR! neq 0 (
    echo [Step 8] Failed (Error code: !STEP_ERR!)
    set PIPELINE_ERROR=!STEP_ERR!
) else (
    echo [Step 8] Completed
)
echo.

call "%PYTHON_CMD%" scripts\export_to_json.py !DATE_STR!
set STEP_ERR=!errorlevel!
if !STEP_ERR! neq 0 (
    echo [Step 9] Failed (Error code: !STEP_ERR!)
    set PIPELINE_ERROR=!STEP_ERR!
) else (
    echo [Step 9] Completed
)
echo.

call "%PYTHON_CMD%" scripts\generate_ai_insights.py --date !DATE_STR! --overview --all-brands
set STEP_ERR=!errorlevel!
if !STEP_ERR! neq 0 (
    echo [Step 10] Failed (Error code: !STEP_ERR!)
    echo [Warning] AI Insights generation failed, but continuing...
) else (
    echo [Step 10] Completed
)
echo.

echo.
echo ============================================================

if !PIPELINE_ERROR! neq 0 (
    echo   Failed (Error code: !PIPELINE_ERROR!)
    echo ============================================================
    echo.
    echo An error occurred. Please check the error messages above.
    echo.
) else (
    echo   Complete!
    echo ============================================================
    echo.
    echo All processing completed successfully.
    echo.
    if defined DATE_STR (
        echo Generated JSON files:
        if exist "public\data\!DATE_STR!" (
            dir /b "public\data\!DATE_STR!\*.json"
        )
        echo.
    )
)

echo Press any key to exit...
pause >nul
exit /b !PIPELINE_ERROR!
