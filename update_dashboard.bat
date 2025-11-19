@echo off
chcp 65001 >nul
echo ====================================
echo   대시보드 데이터 업데이트
echo ====================================
echo.

echo [1/2] 데이터 처리 중...
echo.
python scripts\update_all.py

if errorlevel 1 (
    echo.
    echo ❌ 데이터 처리 실패
    echo    위 오류 메시지를 확인하세요
    pause
    exit /b 1
)

echo.
echo.
echo [2/2] Git 커밋 및 푸시...
echo.

git add .
git commit -m "데이터 업데이트 %date% %time%"

if errorlevel 1 (
    echo.
    echo ℹ️  변경사항이 없거나 커밋 실패
    echo    - 변경사항이 없다면 정상입니다
    echo    - 오류가 있다면 수동으로 git status 확인하세요
)

echo.
git push

if errorlevel 1 (
    echo.
    echo ❌ Git 푸시 실패
    echo    수동으로 git push 실행하세요
    pause
    exit /b 1
)

echo.
echo ====================================
echo   ✅ 완료!
echo ====================================
echo.
echo Vercel에서 자동 배포 중입니다.
echo 1-2분 후 대시보드를 확인하세요.
echo.
pause














