@echo off
REM backend\services\start_realtime.bat
REM Start the real-time price update service for Windows

echo Starting NSE Real-Time Price Service...
echo Updates: Every 1 second during market hours [9:15 AM - 3:30 PM IST]
echo Press Ctrl+C to stop
echo.

cd /d "%~dp0..\..\" || exit /b 1

REM Run the Python service
python -m backend.services.realtime_price_service
