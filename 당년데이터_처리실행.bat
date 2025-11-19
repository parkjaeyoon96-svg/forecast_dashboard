@echo off
setlocal enabledelayedexpansion
cd /d "%~dp0"

echo ============================================================
echo   KE30 Processing and Treemap Data Generation
echo ============================================================
echo.

REM Step 1: KE30 Preprocessing
echo [Step 1/3] KE30 Preprocessing...
python "scripts\process_ke30_current_year.py"
if errorlevel 1 (
    echo.
    echo [ERROR] KE30 preprocessing failed
    goto error
)

echo.
echo [Step 2/3] Finding date folder...
set DATE_FOLDER=
REM 새 구조: raw/YYYYMM/present/YYYYMMDD/ 에서 최신 날짜 폴더 찾기
REM 먼저 월별 폴더를 찾고, 그 안의 present/YYYYMMDD/ 폴더를 찾음
for /f "delims=" %%m in ('dir /b /ad /o-n "raw" 2^>nul ^| findstr /r "^[0-9][0-9][0-9][0-9][0-9][0-9]$"') do (
    if exist "raw\%%m\present" (
        for /f "delims=" %%d in ('dir /b /ad /o-n "raw\%%m\present" 2^>nul ^| findstr /r "^[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]$"') do (
            if not defined DATE_FOLDER set DATE_FOLDER=%%d
        )
    )
    if defined DATE_FOLDER goto found_date
)
REM 기존 구조 호환: raw/YYYYMMDD/ 직접 찾기
for /f "delims=" %%d in ('dir /b /ad /o-n "raw" 2^>nul ^| findstr /r "^[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]$"') do (
    if not defined DATE_FOLDER set DATE_FOLDER=%%d
)
:found_date

echo.
echo [Step 3/3] Generating treemap data...
if defined DATE_FOLDER (
    echo Date folder found: !DATE_FOLDER!
    python "scripts\create_treemap_data.py" !DATE_FOLDER!
    if errorlevel 1 (
        echo.
        echo [ERROR] Treemap data generation failed
        goto error
    )
) else (
    echo [WARNING] No date folder found, using latest file
    python "scripts\create_treemap_data.py"
    if errorlevel 1 (
        echo.
        echo [ERROR] Treemap data generation failed
        goto error
    )
)

echo.
echo ============================================================
echo   Complete!
echo ============================================================
echo.
echo Generated files:
if defined DATE_FOLDER (
    echo   - public\treemap_data_!DATE_FOLDER!.js
    echo   - public\data_!DATE_FOLDER!.js
) else (
    echo   - public\treemap_data.js
    echo   - public\data.js
)
echo.
pause
exit /b 0

:error
echo.
echo ============================================================
echo   Failed (Error code: %errorlevel%)
echo ============================================================
echo.
echo Please check the error messages above.
pause
exit /b %errorlevel%
