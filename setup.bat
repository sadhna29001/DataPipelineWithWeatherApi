@echo off
REM Setup script for Weather Data Pipeline (Windows)

echo ╔══════════════════════════════════════════════════════╗
echo ║     Weather Data Pipeline - Setup Script            ║
echo ╚══════════════════════════════════════════════════════╝
echo.

REM Check if Python is installed
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ❌ Python is not installed. Please install Python 3.8 or higher.
    exit /b 1
)

echo ✅ Python found
echo.

REM Create virtual environment
echo 📦 Creating virtual environment...
python -m venv venv

if %errorlevel% neq 0 (
    echo ❌ Failed to create virtual environment
    exit /b 1
)

echo ✅ Virtual environment created
echo.

REM Activate virtual environment
echo 🔧 Activating virtual environment...
call venv\Scripts\activate.bat

REM Upgrade pip
echo ⬆️  Upgrading pip...
python -m pip install --upgrade pip --quiet

REM Install requirements
echo 📥 Installing dependencies...
pip install -r requirements.txt

if %errorlevel% neq 0 (
    echo ❌ Failed to install dependencies
    exit /b 1
)

echo ✅ Dependencies installed successfully
echo.

REM Create necessary directories
echo 📁 Creating project directories...
if not exist "data" mkdir data
if not exist "logs" mkdir logs
if not exist "backups" mkdir backups

echo ✅ Directories created
echo.

REM Create .env file if it doesn't exist
if not exist ".env" (
    echo 📝 Creating .env file from template...
    copy .env.example .env
    echo ✅ .env file created
    echo.
    echo ⚠️  IMPORTANT: Edit the .env file and add your OpenWeatherMap API key!
    echo    Get your free API key at: https://openweathermap.org/api
) else (
    echo ℹ️  .env file already exists
)

echo.
echo ╔══════════════════════════════════════════════════════╗
echo ║              Setup Complete! 🎉                      ║
echo ╚══════════════════════════════════════════════════════╝
echo.
echo Next steps:
echo 1. Edit .env file and add your OpenWeatherMap API key
echo 2. Activate the virtual environment: venv\Scripts\activate
echo 3. Run the pipeline: python pipeline.py
echo.
echo For more information, see README.md

pause
