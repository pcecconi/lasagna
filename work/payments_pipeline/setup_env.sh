#!/bin/bash

# Payments Pipeline Environment Setup
# This script sets up a virtual environment for data generation

echo "🚀 Setting up Payments Pipeline Environment"
echo "=========================================="

# Check if Python 3 is available
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is not installed. Please install Python 3.8+ first."
    exit 1
fi

echo "✅ Python 3 found: $(python3 --version)"

# Create virtual environment
echo "📦 Creating virtual environment..."
python3 -m venv venv

# Activate virtual environment
echo "🔄 Activating virtual environment..."
source venv/bin/activate

# Upgrade pip
echo "⬆️ Upgrading pip..."
pip install --upgrade pip

# Install dependencies
echo "📚 Installing dependencies..."
pip install -r requirements.txt

echo ""
echo "🎉 Environment setup complete!"
echo ""
echo "📝 Usage:"
echo "1. Activate environment: source venv/bin/activate"
echo "2. Generate data: python data_generator.py --help"
echo "3. Deactivate when done: deactivate"
echo ""
echo "🔄 To activate the environment:"
echo "   source venv/bin/activate"
echo ""
echo "🧪 To run tests:"
echo "   python test_generator.py"
echo ""
echo "📊 To generate initial data:"
echo "   ./generate_data.sh"
