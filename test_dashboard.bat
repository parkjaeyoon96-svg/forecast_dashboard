@echo off
chcp 65001 >nul
cd /d "%~dp0"

echo ============================================================
echo   Dashboard 테스트 도구
echo ============================================================
echo.

REM 최신 날짜 폴더 찾기
set LATEST_DATE=
for /f "delims=" %%d in ('dir /b /ad /o-n "public\data" 2^>nul ^| findstr /r "^[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]$"') do (
    if "!LATEST_DATE!"=="" set "LATEST_DATE=%%d"
)

if "%LATEST_DATE%"=="" (
    echo [ERROR] public\data 폴더에 날짜 폴더가 없습니다!
    echo.
    echo dashboard_json_gen.bat를 먼저 실행하세요.
    pause
    exit /b 1
)

echo [정보] 최신 데이터 날짜: %LATEST_DATE%
echo.

REM JSON 파일 개수 확인
set JSON_COUNT=0
for %%f in (public\data\%LATEST_DATE%\*.json) do set /a JSON_COUNT+=1

echo [정보] JSON 파일 개수: %JSON_COUNT%개
echo.

if %JSON_COUNT% LSS 10 (
    echo [경고] JSON 파일이 부족합니다. 최소 10개 이상 필요합니다.
    echo.
)

REM 주요 JSON 파일 존재 확인
echo [확인] 주요 JSON 파일 체크:
if exist "public\data\%LATEST_DATE%\overview_pl.json" (
    echo   [OK] overview_pl.json
) else (
    echo   [X] overview_pl.json - 없음!
)

if exist "public\data\%LATEST_DATE%\brand_kpi.json" (
    echo   [OK] brand_kpi.json
) else (
    echo   [X] brand_kpi.json - 없음!
)

if exist "public\data\%LATEST_DATE%\treemap.json" (
    echo   [OK] treemap.json
) else (
    echo   [X] treemap.json - 없음!
)

if exist "public\data\%LATEST_DATE%\weekly_trend.json" (
    echo   [OK] weekly_trend.json
) else (
    echo   [X] weekly_trend.json - 없음!
)

echo.
echo ============================================================
echo   로컬 서버 시작 중...
echo ============================================================
echo.
echo 브라우저에서 다음 URL로 접속하세요:
echo.
echo   http://localhost:8000/Dashboard.html?date=%LATEST_DATE%
echo.
echo 또는 기본 날짜로 접속:
echo   http://localhost:8000/Dashboard.html
echo.
echo 서버를 종료하려면 Ctrl+C를 누르세요.
echo.
echo ============================================================
echo.

cd public
python -m http.server 8000










