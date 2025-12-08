@echo off
setlocal EnableExtensions EnableDelayedExpansion
chcp 65001 >nul 2>&1
cd /d "%~dp0"

echo ============================================================
echo   Dashboard JSON Generation
echo ============================================================
echo.

REM Python 경로 설정: 가상환경 우선, 없으면 시스템 python
set "PYTHON_CMD="
if exist "%~dp0Forcast_venv\Scripts\python.exe" (
    set "PYTHON_CMD=%~dp0Forcast_venv\Scripts\python.exe"
) else (
    where python >nul 2>&1
    if %errorlevel% equ 0 (
        set "PYTHON_CMD=python"
    ) else (
        echo [ERROR] Python을 찾을 수 없습니다. Forcast_venv를 생성하거나 PATH를 확인하세요.
        pause
        exit /b 1
    )
)
set "PYTHONIOENCODING=utf-8"

REM 최신 자동선택(Y/N) 후 필요 시 수동 입력
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

REM 유효성 검사
if not defined ANALYSIS_MONTH (
    echo [ERROR] 분석월을 찾을 수 없습니다.
    echo raw\YYYYMM\current_year\YYYYMMDD 구조를 확인해주세요.
    pause
    exit /b 1
)

if not defined UPDATE_DATE (
    echo [ERROR] 업데이트일자를 찾을 수 없습니다.
    echo raw\!ANALYSIS_MONTH!\current_year\YYYYMMDD 구조를 확인해주세요.
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

REM === 공통 파이프라인 시작 ===

call "%PYTHON_CMD%" scripts\update_brand_kpi.py !DATE_STR!
set STEP_ERR=!errorlevel!
if !STEP_ERR! neq 0 (
    echo [Step 1] Failed (Error code: !STEP_ERR!)
    set PIPELINE_ERROR=!STEP_ERR!
) else (
    echo [Step 1] Completed
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

set DATE_FORMATTED_STEP6=!DATE_STR:~0,4!-!DATE_STR:~4,2!-!DATE_STR:~6,2!
call "%PYTHON_CMD%" scripts\download_weekly_sales_trend.py !DATE_FORMATTED_STEP6!
set STEP_ERR=!errorlevel!
if !STEP_ERR! neq 0 (
    echo [Step 6] Failed (Error code: !STEP_ERR!)
    set PIPELINE_ERROR=!STEP_ERR!
) else (
    echo [Step 6] Completed
)
echo.

set DATE_FORMATTED=!DATE_STR:~0,4!-!DATE_STR:~4,2!-!DATE_STR:~6,2!
call "%PYTHON_CMD%" scripts\download_brand_stock_analysis.py --update-date !DATE_FORMATTED!
set STOCK_ERR=!errorlevel!

if !STOCK_ERR! neq 0 (
    echo.
    echo [Step 7-Alternative] Generating stock analysis from CSV
    call "%PYTHON_CMD%" scripts\generate_brand_stock_analysis.py !DATE_STR!
    set ALT_ERR=!errorlevel!
    if !ALT_ERR! neq 0 (
        echo [Step 7-Alternative] Failed (Error code: !ALT_ERR!)
    ) else (
        echo [Step 7-Alternative] Success
    )
) else (
    if not exist "public\data\!DATE_STR!\stock_analysis.json" (
        echo.
        echo [Step 7-Alternative] stock_analysis.json not found, generating from CSV
        call "%PYTHON_CMD%" scripts\generate_brand_stock_analysis.py !DATE_STR!
        set ALT_ERR=!errorlevel!
        if !ALT_ERR! neq 0 (
            echo [Step 7-Alternative] Failed (Error code: !ALT_ERR!)
        ) else (
            echo [Step 7-Alternative] Success
        )
    )
)
echo.

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