#!/bin/bash

# Setup script for Weather Data Pipeline

echo "╔══════════════════════════════════════════════════════╗"
echo "║     Weather Data Pipeline - Setup Script            ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is not installed. Please install Python 3.8 or higher."
    exit 1
fi

echo "✅ Python 3 found: $(python3 --version)"
echo ""

# Create virtual environment
echo "📦 Creating virtual environment..."
python3 -m venv venv

if [ $? -ne 0 ]; then
    echo "❌ Failed to create virtual environment"
    exit 1
fi

echo "✅ Virtual environment created"
echo ""

# Activate virtual environment
echo "🔧 Activating virtual environment..."
source venv/bin/activate

# Upgrade pip
echo "⬆️  Upgrading pip..."
pip install --upgrade pip --quiet

# Install requirements
echo "📥 Installing dependencies..."
pip install -r requirements.txt

if [ $? -ne 0 ]; then
    echo "❌ Failed to install dependencies"
    exit 1
fi

echo "✅ Dependencies installed successfully"
echo ""

# Create necessary directories
echo "📁 Creating project directories..."
mkdir -p data logs backups

echo "✅ Directories created"
echo ""

# Create .env file if it doesn't exist
if [ ! -f .env ]; then
    echo "📝 Creating .env file from template..."
    cp .env.example .env
    echo "✅ .env file created"
    echo ""
    echo "⚠️  IMPORTANT: Edit the .env file and add your OpenWeatherMap API key!"
    echo "   Get your free API key at: https://openweathermap.org/api"
else
    echo "ℹ️  .env file already exists"
fi

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║              Setup Complete! 🎉                      ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""
echo "Next steps:"
echo "1. Edit .env file and add your OpenWeatherMap API key"
echo "2. Activate the virtual environment: source venv/bin/activate"
echo "3. Run the pipeline: python pipeline.py"
echo ""
echo "For more information, see README.md"
