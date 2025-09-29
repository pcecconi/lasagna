#!/bin/bash

# Test runner for payments data generator

echo "🧪 Running Payments Data Generator Tests"
echo "========================================"

# Check if virtual environment is activated
if [[ "$VIRTUAL_ENV" == "" ]]; then
    echo "⚠️  Virtual environment not activated. Activating..."
    source venv/bin/activate
fi

# Run tests with pytest
echo "🔍 Running unit tests..."
pytest tests/ -v --tb=short

# Run tests with coverage
echo ""
echo "📊 Running tests with coverage..."
pytest tests/ --cov=. --cov-report=term-missing

echo ""
echo "✅ Tests completed!"

