@echo off
chcp 65001 >nul
cd /d "%~dp0"

echo ============================================================
echo   Dashboard 로컬 서버 시작
echo ============================================================
echo.
echo 서버 주소: http://localhost:8000
echo Dashboard: http://localhost:8000/Dashboard.html?date=20251222
echo.
echo 서버를 종료하려면 Ctrl+C를 누르세요.
echo.
echo ============================================================
echo.

cd public
python -m http.server 8000

pause


















