@echo off
chcp 65001 >nul
cd /d "%~dp0"
setlocal EnableDelayedExpansion

REM Python 출력 인코딩을 UTF-8로 설정 (이모지 출력 오류 방지)
set PYTHONIOENCODING=utf-8

echo ============================================================
echo   대시보드 데이터 가공 및 JSON 생성
echo ============================================================
echo.

REM 날짜 파라미터 확인 및 설정
if "%1"=="" (
    echo [정보] 날짜 파라미터가 없습니다. 최신 metadata.json에서 업데이트 일자와 분석월을 찾습니다.
    echo.
    echo 사용법: %~n0.bat [YYYYMMDD]
    echo 예시: %~n0.bat 20251201
    echo.
    echo 날짜를 지정하지 않으면 raw\YYYYMM\current_year\YYYYMMDD\metadata.json 중 가장 마지막 일자 폴더를 기준으로 합니다.
    echo.

    REM Python 스크립트를 사용하여 최신 metadata.json에서 update_date / analysis_month 읽기
    python "scripts\get_latest_metadata_date.py" > "%TEMP%\~meta_date.tmp" 2>nul
    if exist "%TEMP%\~meta_date.tmp" (
        set /p DATE_STR=<"%TEMP%\~meta_date.tmp"
        del "%TEMP%\~meta_date.tmp" >nul 2>&1
    )
    
    python "scripts\get_latest_metadata_date.py" month > "%TEMP%\~meta_month.tmp" 2>nul
    if exist "%TEMP%\~meta_month.tmp" (
        set /p YEAR_MONTH=<"%TEMP%\~meta_month.tmp"
        del "%TEMP%\~meta_month.tmp" >nul 2>&1
    )

    if not defined DATE_STR (
        echo [오류] raw\*\current_year\*\metadata.json에서 업데이트 일자를 찾을 수 없습니다.
        echo 날짜를 수동으로 입력해주세요: %~n0.bat YYYYMMDD
        echo.
        echo 확인 사항:
        echo   1. raw\YYYYMM\current_year\YYYYMMDD\metadata.json 구조가 존재하는지 확인
        echo   2. metadata.json에 update_date, analysis_month 값이 있는지 확인
        echo.
        echo [안내] 먼저 당년데이터_처리실행.bat를 실행하여 전처리를 완료하세요.
        echo.
        pause
        exit /b 1
    )

    echo [자동] metadata.json에서 날짜 추출: !DATE_STR! (분석월: !YEAR_MONTH!)
    echo.
) else (
    set "DATE_STR=%1"
    echo [정보] 입력된 날짜: %DATE_STR%
    echo.
    
    REM 입력된 날짜에서 YEAR_MONTH 추출 (임시값, metadata.json에서 실제 값 읽음)
    set "YEAR_MONTH=%DATE_STR:~0,6%"
)

REM 날짜 형식 검증 (8자리 숫자인지 확인) - 직접 날짜를 입력한 경우에만 체크
if not "%1"=="" (
    echo !DATE_STR!| findstr /r "^[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]$" >nul
    if errorlevel 1 (
        echo [오류] 날짜 형식이 올바르지 않습니다.
        echo 올바른 형식: YYYYMMDD (예: 20251124)
        echo 입력된 값: !DATE_STR!
        echo.
        pause
        exit /b 1
    )
)

REM YEAR_MONTH가 설정되지 않았으면 DATE_STR에서 추출 (임시값)
if not defined YEAR_MONTH (
    set YEAR_MONTH=!DATE_STR:~0,6!
)

REM metadata.json에서 실제 analysis_month 읽기 (당년 전처리와 동일한 로직)
REM Python 스크립트를 사용하여 metadata.json 찾기 및 analysis_month 읽기
python -c "import json, os, glob; date_str='!DATE_STR!'; paths=glob.glob('raw/*/current_year/'+date_str+'/metadata.json'); path=paths[0] if paths else None; am=json.load(open(path,'r',encoding='utf-8')).get('analysis_month') if path and os.path.exists(path) else '!YEAR_MONTH!'; print(am)" > "%TEMP%\~analysis_month.tmp" 2>nul
if exist "%TEMP%\~analysis_month.tmp" (
    set /p YEAR_MONTH=<"%TEMP%\~analysis_month.tmp"
    del "%TEMP%\~analysis_month.tmp" >nul 2>&1
    echo [정보] metadata.json에서 읽은 분석월: !YEAR_MONTH!
) else (
    echo [경고] metadata.json을 찾을 수 없습니다. DATE_STR에서 추출한 값 사용: !YEAR_MONTH!
)

REM 날짜 형식 변환 (YYYYMMDD → YYYY-MM-DD)
set DATE_FORMATTED=!DATE_STR:~0,4!-!DATE_STR:~4,2!-!DATE_STR:~6,2!

echo [정보] 처리 날짜: !DATE_STR!
echo [정보] 분석 월: !YEAR_MONTH!
echo [정보] 날짜 형식 (YYYY-MM-DD): !DATE_FORMATTED!
echo.

REM 전처리된 데이터 경로 확인 (선택적 - 없어도 계속 진행)
set RAW_DATA_DIR=raw\!YEAR_MONTH!\current_year\!DATE_STR!
if exist "!RAW_DATA_DIR!" (
    echo [확인] 전처리된 데이터 경로: !RAW_DATA_DIR!
) else (
    echo [정보] 전처리된 데이터 폴더가 없습니다: !RAW_DATA_DIR!
    echo [정보] 메타데이터만 사용하여 진행합니다.
)
echo.

REM ============================================================
REM Step 1: 브랜드 KPI 업데이트
REM 생성 파일: brand_kpi.json
REM ============================================================
echo ============================================================
echo [1/10] 브랜드 KPI 업데이트
echo ============================================================
echo.

python scripts\update_brand_kpi.py !DATE_STR!

if errorlevel 1 (
    echo.
    echo [오류] 브랜드 KPI 업데이트 실패 (Error code: %errorlevel%)
    echo.
    pause
    exit /b %errorlevel%
)

echo.
echo [완료] 브랜드 KPI 업데이트 완료 → brand_kpi.json 생성
echo.

REM ============================================================
REM Step 2: 전체현황 데이터 업데이트
REM 생성 파일: overview_kpi.json, overview_by_brand.json, 
REM            overview_pl.json, overview_waterfall.json, 
REM            overview_trend.json, brand_plan.json, stock_analysis.json
REM ============================================================
echo ============================================================
echo [2/10] 전체현황 데이터 업데이트
echo ============================================================
echo.

python scripts\update_overview_data.py !DATE_STR!

if errorlevel 1 (
    echo.
    echo [오류] 전체현황 데이터 업데이트 실패 (Error code: %errorlevel%)
    echo.
    pause
    exit /b %errorlevel%
)

echo.
echo [완료] 전체현황 데이터 업데이트 완료
echo   - overview_kpi.json
echo   - overview_by_brand.json
echo   - overview_pl.json
echo   - overview_waterfall.json
echo   - overview_trend.json
echo   - brand_plan.json
echo   - stock_analysis.json
echo.

REM ============================================================
REM Step 3: 브랜드 손익계산서 생성
REM 생성 파일: brand_pl.json
REM ============================================================
echo ============================================================
echo [3/10] 브랜드 손익계산서 생성
echo ============================================================
echo.

python scripts\create_brand_pl_data.py !DATE_STR!

if errorlevel 1 (
    echo.
    echo [오류] 브랜드 손익계산서 생성 실패 (Error code: %errorlevel%)
    echo.
    pause
    exit /b %errorlevel%
)

echo.
echo [완료] 브랜드 손익계산서 생성 완료 → brand_pl.json 생성
echo.

REM ============================================================
REM Step 4: 브랜드 레이더 차트 데이터
REM 생성 파일: radar_chart.json
REM ============================================================
echo ============================================================
echo [4/10] 브랜드 레이더 차트 데이터
echo ============================================================
echo.

python scripts\update_brand_radar.py !DATE_STR!

if errorlevel 1 (
    echo.
    echo [오류] 브랜드 레이더 차트 데이터 생성 실패 (Error code: %errorlevel%)
    echo.
    pause
    exit /b %errorlevel%
)

echo.
echo [완료] 브랜드 레이더 차트 데이터 생성 완료 → radar_chart.json 생성
echo.

REM ============================================================
REM Step 5: 채널 손익 데이터
REM 생성 파일: channel_profit_loss.json
REM ============================================================
echo ============================================================
echo [5/10] 채널 손익 데이터
echo ============================================================
echo.

python scripts\process_channel_profit_loss.py --base-date !DATE_STR! --target-month !YEAR_MONTH! --format dashboard

if errorlevel 1 (
    echo.
    echo [오류] 채널 손익 데이터 생성 실패 (Error code: %errorlevel%)
    echo.
    pause
    exit /b %errorlevel%
)

echo.
echo [완료] 채널 손익 데이터 생성 완료 → channel_profit_loss.json 생성
echo.

REM ============================================================
REM Step 6: 주간 매출 추세 다운로드
REM 생성 파일: weekly_trend.json (export_to_json.py에서 변환)
REM ============================================================
echo ============================================================
echo [6/10] 주간 매출 추세 다운로드
echo ============================================================
echo.

python scripts\download_weekly_sales_trend.py !DATE_FORMATTED!

if errorlevel 1 (
    echo.
    echo [경고] 주간 매출 추세 다운로드 실패 (선택적 단계이므로 계속 진행)
    echo.
) else (
    echo.
    echo [완료] 주간 매출 추세 다운로드 완료
    echo.
)

REM ============================================================
REM Step 7: 재고 분석 다운로드
REM 생성 파일: stock_analysis.json (update_overview_data.py에서도 생성)
REM ============================================================
echo ============================================================
echo [7/10] 재고 분석 다운로드
echo ============================================================
echo.

python scripts\download_brand_stock_analysis.py --update-date !DATE_FORMATTED!

if errorlevel 1 (
    echo.
    echo [경고] 재고 분석 다운로드 실패 (선택적 단계이므로 계속 진행)
    echo.
) else (
    echo.
    echo [완료] 재고 분석 다운로드 완료
    echo.
)

REM ============================================================
REM Step 8: 트리맵 데이터 생성
REM 생성 파일: treemap.json
REM ============================================================
echo ============================================================
echo [8/9] 트리맵 데이터 생성
echo ============================================================
echo.

python scripts\create_treemap_data_v2.py !DATE_STR!

if errorlevel 1 (
    echo.
    echo [오류] 트리맵 데이터 생성 실패 (Error code: %errorlevel%)
    echo.
    pause
    exit /b %errorlevel%
)

echo.
echo [완료] 트리맵 데이터 생성 완료
echo.

REM ============================================================
REM Step 9: AI 인사이트 생성 (선택적)
REM 생성 파일: ai_insights/insights_data_*.json
REM ============================================================
echo ============================================================
echo [9/9] AI 인사이트 생성
echo ============================================================
echo.

python scripts\generate_ai_insights.py --date !DATE_STR! --all-brands

if errorlevel 1 (
    echo.
    echo [경고] AI 인사이트 생성 실패 (선택적 단계이므로 계속 진행)
    echo.
) else (
    echo.
    echo [완료] AI 인사이트 생성 완료 → ai_insights/insights_data_*.json 생성
    echo.
)

REM ============================================================
REM 완료 요약
REM ============================================================
echo ============================================================
echo   전체 파이프라인 완료
echo ============================================================
echo.

REM 생성된 JSON 파일 확인
set JSON_DIR=public\data\!DATE_STR!
if exist "%JSON_DIR%" (
    echo [생성된 JSON 파일 목록]
    echo.
    dir /b "%JSON_DIR%\*.json" 2>nul | findstr /v /i "ai_insights"
    if errorlevel 1 (
        echo   (JSON 파일 없음)
    )
    echo.
    if exist "%JSON_DIR%\ai_insights" (
        echo [AI 인사이트 파일]
        dir /b "%JSON_DIR%\ai_insights\*.json" 2>nul
        if errorlevel 1 (
            echo   (AI 인사이트 파일 없음)
        )
        echo.
    )
) else (
    echo [경고] JSON 디렉토리가 없습니다: %JSON_DIR%
    echo.
)

echo [대시보드 URL]
echo   http://localhost:3000/Dashboard.html?date=!DATE_STR!
echo.
echo [생성된 데이터 위치]
echo   public\data\!DATE_STR!\
echo.
echo ============================================================
echo.

pause
endlocal
exit /b 0

