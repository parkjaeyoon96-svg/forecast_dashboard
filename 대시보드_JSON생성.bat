@echo off
setlocal enabledelayedexpansion
chcp 65001 >nul
cd /d "%~dp0"

echo ============================================================
echo   Dashboard JSON Generation
echo ============================================================
echo.

set /p USE_LATEST="Use latest files? (Y/N): "

set PIPELINE_ERROR=0

if /i "!USE_LATEST!"=="Y" (
    echo.
    echo Running in auto-select latest file mode...
    echo.
    echo [Step] Starting Python script...
    python scripts\generate_dashboard_data.py
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
    
    set DATE_STR=!UPDATE_DATE!
    
    echo [Step 1] Brand KPI Update
    python scripts\update_brand_kpi.py !DATE_STR!
    set STEP_ERR=!errorlevel!
    if !STEP_ERR! neq 0 (
        echo [Step 1] Failed (Error code: !STEP_ERR!)
        set PIPELINE_ERROR=!STEP_ERR!
    ) else (
        echo [Step 1] Completed
    )
    echo.
    
    echo [Step 2] Overview Data Update
    python scripts\update_overview_data.py !DATE_STR!
    set STEP_ERR=!errorlevel!
    if !STEP_ERR! neq 0 (
        echo [Step 2] Failed (Error code: !STEP_ERR!)
        set PIPELINE_ERROR=!STEP_ERR!
    ) else (
        echo [Step 2] Completed
    )
    echo.
    
    echo [Step 3] Brand PL Data Creation
    python scripts\create_brand_pl_data.py !DATE_STR!
    set STEP_ERR=!errorlevel!
    if !STEP_ERR! neq 0 (
        echo [Step 3] Failed (Error code: !STEP_ERR!)
        set PIPELINE_ERROR=!STEP_ERR!
    ) else (
        echo [Step 3] Completed
    )
    echo.
    
    echo [Step 4] Brand Radar Chart Data
    python scripts\update_brand_radar.py !DATE_STR!
    set STEP_ERR=!errorlevel!
    if !STEP_ERR! neq 0 (
        echo [Step 4] Failed (Error code: !STEP_ERR!)
        set PIPELINE_ERROR=!STEP_ERR!
    ) else (
        echo [Step 4] Completed
    )
    echo.
    
    echo [Step 5] Channel Profit Loss Data
    set YEAR_MONTH=!DATE_STR:~0,6!
    python scripts\process_channel_profit_loss.py --base-date !DATE_STR! --target-month !YEAR_MONTH! --format dashboard
    set STEP_ERR=!errorlevel!
    if !STEP_ERR! neq 0 (
        echo [Step 5] Failed (Error code: !STEP_ERR!)
        set PIPELINE_ERROR=!STEP_ERR!
    ) else (
        echo [Step 5] Completed
    )
    echo.
    
    echo [Step 6] Weekly Sales Trend Download
    set DATE_FORMATTED_STEP6=!DATE_STR:~0,4!-!DATE_STR:~4,2!-!DATE_STR:~6,2!
    python scripts\download_weekly_sales_trend.py !DATE_FORMATTED_STEP6!
    set STEP_ERR=!errorlevel!
    if !STEP_ERR! neq 0 (
        echo [Step 6] Failed (Error code: !STEP_ERR!)
        set PIPELINE_ERROR=!STEP_ERR!
    ) else (
        echo [Step 6] Completed
    )
    echo.
    
    echo [Step 7] Stock Analysis Download
    set DATE_FORMATTED=!DATE_STR:~0,4!-!DATE_STR:~4,2!-!DATE_STR:~6,2!
    python scripts\download_brand_stock_analysis.py --update-date !DATE_FORMATTED!
    set STOCK_ERR=!errorlevel!
    
    if !STOCK_ERR! neq 0 (
        echo.
        echo [Step 7-Alternative] Generating stock analysis from CSV
        python scripts\generate_brand_stock_analysis.py !DATE_STR!
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
            python scripts\generate_brand_stock_analysis.py !DATE_STR!
            set ALT_ERR=!errorlevel!
            if !ALT_ERR! neq 0 (
                echo [Step 7-Alternative] Failed (Error code: !ALT_ERR!)
            ) else (
                echo [Step 7-Alternative] Success
            )
        )
    )
    echo.
    
    echo [Step 8] Treemap Data Creation
    python scripts\create_treemap_data_v2.py !DATE_STR!
    set STEP_ERR=!errorlevel!
    if !STEP_ERR! neq 0 (
        echo [Step 8] Failed (Error code: !STEP_ERR!)
        set PIPELINE_ERROR=!STEP_ERR!
    ) else (
        echo [Step 8] Completed
    )
    echo.
    
    echo [Step 9] JSON Export
    python scripts\export_to_json.py !DATE_STR!
    set STEP_ERR=!errorlevel!
    if !STEP_ERR! neq 0 (
        echo [Step 9] Failed (Error code: !STEP_ERR!)
        set PIPELINE_ERROR=!STEP_ERR!
    ) else (
        echo [Step 9] Completed
    )
    echo.
    
    echo [Step 10] AI Insights Generation
    python scripts\generate_ai_insights.py --date !DATE_STR! --overview --all-brands
    set STEP_ERR=!errorlevel!
    if !STEP_ERR! neq 0 (
        echo [Step 10] Failed (Error code: !STEP_ERR!)
        echo [Warning] AI Insights generation failed, but continuing...
        REM AI 인사이트 생성 실패는 경고만 출력하고 계속 진행
    ) else (
        echo [Step 10] Completed
    )
    echo.
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
