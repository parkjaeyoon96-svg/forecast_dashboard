@echo off
REM ============================================================
REM   전년/계획 데이터 업데이트 배치 파일
REM ============================================================
REM
REM 실행 프로세스:
REM   Step 1: 전년 데이터 다운로드 (Snowflake)
REM   Step 2: 전년 데이터 전처리
REM   Step 3: 계획 데이터 전처리
REM   Step 4: 전년 누적 매출 다운로드 (Snowflake)
REM   Step 5: 진척일수 계산
REM
REM 주의: 직접비율 추출은 당년데이터_처리실행.bat에서 수행됩니다
REM ============================================================

REM UTF-8 인코딩 설정
chcp 65001 >nul

REM 스크립트 파일 위치로 디렉토리 이동
cd /d "%~dp0"

REM Python 출력 인코딩을 UTF-8로 설정
set PYTHONIOENCODING=utf-8

echo ============================================================
echo   Previous Year / Plan Data Update
echo ============================================================
echo.

REM Python 스크립트 실행
REM scripts/run_previous_year_plan_update.py
python scripts\run_previous_year_plan_update.py

REM 에러 처리: Python 스크립트 실행 실패 시
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

REM 성공 메시지 출력
echo.
echo ============================================================
echo   COMPLETED!
echo ============================================================
echo.
echo Generated files are in raw/{YYYYMM}/previous_year/ and raw/{YYYYMM}/plan/
echo.

pause
exit /b 0
