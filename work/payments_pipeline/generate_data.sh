#!/bin/bash

# Payments Aggregator Data Generator
# This script demonstrates how to use the data generator

echo "🚀 Payments Aggregator Data Generator"
echo "====================================="

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "⚠️  Virtual environment not found. Setting up..."
    ./setup_env.sh
fi

# Activate virtual environment
echo "🔄 Activating virtual environment..."
source venv/bin/activate

# Create output directory
mkdir -p raw_data

# Generate initial dataset (6 months: Jan 2024 - Jun 2024)
echo "📊 Generating initial dataset (6 months)..."
python data_generator.py \
    --initial \
    --start-date 2024-01-01 \
    --end-date 2024-06-30 \
    --output-dir ./raw_data

echo ""
echo "✅ Initial dataset generated!"
echo "📁 Check the ./raw_data directory for:"
echo "   - merchants_initial_2024-01-01_2024-06-30.csv"
echo "   - transactions_initial_2024-01-01_2024-06-30.csv"
echo "   - data_state.json (tracks generation state)"

echo ""
echo "🔄 To generate incremental data (one more day):"
echo "   python data_generator.py --incremental"
echo ""
echo "🔄 To generate data for a specific date:"
echo "   python data_generator.py --incremental --date 2024-07-01"

echo ""
echo "📈 Data Summary:"
echo "================"
if [ -f "./raw_data/merchants_initial_2024-01-01_2024-06-30.csv" ]; then
    merchant_count=$(wc -l < "./raw_data/merchants_initial_2024-01-01_2024-06-30.csv")
    echo "👥 Total merchants: $((merchant_count - 1))"  # Subtract header
fi

if [ -f "./raw_data/transactions_initial_2024-01-01_2024-06-30.csv" ]; then
    transaction_count=$(wc -l < "./raw_data/transactions_initial_2024-01-01_2024-06-30.csv")
    echo "💳 Total transactions: $((transaction_count - 1))"  # Subtract header
fi

echo ""
echo "🎯 Next Steps:"
echo "1. Review the generated CSV files"
echo "2. Run incremental generation to add more data"
echo "3. Build bronze layer ingestion pipeline"
echo "4. Create silver layer transformations"
