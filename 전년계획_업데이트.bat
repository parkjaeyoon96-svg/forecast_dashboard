@echo off
chcp 65001 >nul
cd /d "%~dp0"

REM Python 출력 인코딩을 UTF-8로 설정 (이모지 출력 오류 방지)
set PYTHONIOENCODING=utf-8

echo ============================================================
echo   전년/계획 데이터 업데이트
echo ============================================================
echo.

REM 전년/계획 데이터 업데이트 스크립트 실행 (Step 1-5 포함)
REM Step 1: 전년 데이터 다운로드
REM Step 2: 전년 데이터 전처리
REM Step 3: 계획 데이터 전처리
REM Step 4: 전년 누적 매출 다운로드
REM Step 5: 진척일수 계산
REM 
REM Note: 직접비율 추출은 당년데이터_처리실행.bat에서 수행됩니다
python scripts\run_previous_year_plan_update.py

if errorlevel 1 (
    echo.
    echo ============================================================
    echo   실패 (Error code: %errorlevel%)
    echo ============================================================
    echo.
    echo 위의 오류 메시지를 확인해주세요.
    pause
    exit /b %errorlevel%
)

echo.
echo ============================================================
echo   완료!
echo ============================================================
echo.

pause
exit /b 0
























