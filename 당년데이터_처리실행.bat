@echo off
setlocal enabledelayedexpansion
chcp 65001 >nul
cd /d "%~dp0"

echo ============================================================
echo   Current Year Data Processing (Full Pipeline)
echo ============================================================
echo.
echo Processing Steps:
echo   1. KE30 file preprocessing
echo   2. Brand/Channel/Item aggregation
echo   3. Direct cost calculation
echo      - If direct cost rate file exists, reuse it ^(skip plan file calculation^)
echo      - If not, extract from plan files
echo   4. Convert KE30 to Forecast
echo.
echo Note: Brand code 'C' is excluded from processing
echo.

REM Python 명령어 확인
set "PYTHON_CMD="
where python >nul 2>&1
if %errorlevel% equ 0 (
    set "PYTHON_CMD=python"
    echo [정보] Python 명령어: python
) else (
    where py >nul 2>&1
    if %errorlevel% equ 0 (
        set "PYTHON_CMD=py"
        echo [정보] Python 명령어: py
    ) else (
        echo [ERROR] Python이 설치되어 있지 않거나 PATH에 등록되어 있지 않습니다.
        echo Python을 설치하거나 PATH에 추가해주세요.
        pause
        exit /b 1
    )
)
echo.

set /p USE_LATEST="Use latest files? (Y/N): "

set PIPELINE_ERROR=0

if /i "!USE_LATEST!"=="Y" (
    echo.
    echo Running in auto-select latest file mode...
    echo.
    echo [Step] Starting Python script...
    if "!PYTHON_CMD!"=="python" (
        python scripts\run_current_year_pipeline.py
    ) else (
        py scripts\run_current_year_pipeline.py
    )
    set PIPELINE_ERROR=!errorlevel!
    echo.
    echo [Done] Python script finished (Exit code: !PIPELINE_ERROR!)
) else if /i "!USE_LATEST!"=="N" (
    echo.
    set /p ANALYSIS_MONTH="Enter analysis month (YYYYMM, e.g., 202511): "
    set /p UPDATE_DATE="Enter update date (YYYYMMDD, e.g., 20251201): "
    echo.
    echo ============================================================
    echo   Analysis Month: !ANALYSIS_MONTH!
    echo   Update Date: !UPDATE_DATE!
    echo ============================================================
    echo.
    echo [Step] Starting Python script...
    if "!PYTHON_CMD!"=="python" (
        python scripts\run_current_year_pipeline.py !ANALYSIS_MONTH! !UPDATE_DATE!
    ) else (
        py scripts\run_current_year_pipeline.py !ANALYSIS_MONTH! !UPDATE_DATE!
    )
    set PIPELINE_ERROR=!errorlevel!
    echo.
    echo [Done] Python script finished (Exit code: !PIPELINE_ERROR!)
) else (
    echo.
    echo Invalid input. Please enter Y or N.
    echo.
    pause
    exit /b 1
)

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
    echo Generated files:
    echo   - raw/!ANALYSIS_MONTH!/current_year/!UPDATE_DATE!/ke30_*_전처리완료.csv
    echo   - raw/!ANALYSIS_MONTH!/current_year/!UPDATE_DATE!/ke30_*_Shop.csv
    echo   - raw/!ANALYSIS_MONTH!/current_year/!UPDATE_DATE!/ke30_*_Shop_item.csv
    echo   - raw/!ANALYSIS_MONTH!/current_year/!UPDATE_DATE!/forecast_*_Shop.csv
    echo   - raw/!ANALYSIS_MONTH!/plan/!ANALYSIS_MONTH!R_직접비율_추출결과.csv
    echo.
)

echo Press any key to exit...
pause >nul
exit /b !PIPELINE_ERROR!



















