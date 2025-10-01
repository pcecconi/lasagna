#!/bin/bash

# Payments Aggregator Data Generator
# This script provides a convenient wrapper for the data generator

# Function to display usage
show_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --initial              Generate initial dataset"
    echo "  --incremental          Generate incremental data"
    echo "  --start-date DATE      Start date (YYYY-MM-DD) for initial generation"
    echo "  --end-date DATE        End date (YYYY-MM-DD) for initial generation"
    echo "  --date DATE            Specific date (YYYY-MM-DD) for incremental generation"
    echo "  --output-dir DIR       Output directory (default: ./raw_data)"
    echo "  --debug                Enable debug output"
    echo "  --force                Skip confirmation prompt"
    echo "  --help                 Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 --initial --start-date 2024-01-01 --end-date 2024-06-30"
    echo "  $0 --incremental --date 2024-07-01"
    echo "  $0 --incremental"
    echo ""
    echo "If no parameters are provided, the script will use default values:"
    echo "  --initial --start-date 2024-01-01 --end-date 2024-06-30"
}

# Default values
INITIAL=false
INCREMENTAL=false
START_DATE=""
END_DATE=""
DATE=""
OUTPUT_DIR="./raw_data"
DEBUG=false
FORCE=false

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --initial)
            INITIAL=true
            shift
            ;;
        --incremental)
            INCREMENTAL=true
            shift
            ;;
        --start-date)
            START_DATE="$2"
            shift 2
            ;;
        --end-date)
            END_DATE="$2"
            shift 2
            ;;
        --date)
            DATE="$2"
            shift 2
            ;;
        --output-dir)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        --debug)
            DEBUG=true
            shift
            ;;
        --force)
            FORCE=true
            shift
            ;;
        --help)
            show_usage
            exit 0
            ;;
        *)
            echo "❌ Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# If no mode specified, default to initial with default dates
if [ "$INITIAL" = false ] && [ "$INCREMENTAL" = false ]; then
    INITIAL=true
    START_DATE="2024-01-01"
    END_DATE="2024-06-30"
fi

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

# Set Python to unbuffered output
export PYTHONUNBUFFERED=1

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Build the command
CMD="python -u new_data_generator.py --output-dir $OUTPUT_DIR -f"

if [ "$DEBUG" = true ]; then
    CMD="$CMD --debug"
fi

if [ "$INITIAL" = true ]; then
    CMD="$CMD --initial"
    if [ -n "$START_DATE" ]; then
        CMD="$CMD --start-date $START_DATE"
    fi
    if [ -n "$END_DATE" ]; then
        CMD="$CMD --end-date $END_DATE"
    fi
elif [ "$INCREMENTAL" = true ]; then
    CMD="$CMD --incremental"
    if [ -n "$DATE" ]; then
        CMD="$CMD --date $DATE"
    fi
fi

# Show what will be executed
echo ""
echo "📋 Command to execute:"
echo "$CMD"
echo ""

# Confirmation prompt (skip if --force is used)
if [ "$FORCE" = false ]; then
    read -p "🤔 Do you want to proceed with data generation? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "❌ Data generation cancelled."
        exit 0
    fi
fi

echo ""
echo "📊 Starting data generation..."
eval $CMD

# Check if generation was successful
if [ $? -eq 0 ]; then
    echo ""
    echo "✅ Data generation completed successfully!"
    echo "📁 Check the $OUTPUT_DIR directory for generated files"
    
    # Show summary if initial generation
    if [ "$INITIAL" = true ]; then
        echo ""
        echo "📈 Data Summary:"
        echo "================"
        
        # Find the generated files
        MERCHANTS_FILE=$(find "$OUTPUT_DIR" -name "merchants_*.csv" | head -1)
        TRANSACTIONS_FILE=$(find "$OUTPUT_DIR" -name "transactions_*.csv" | head -1)
        
        if [ -f "$MERCHANTS_FILE" ]; then
            merchant_count=$(wc -l < "$MERCHANTS_FILE")
            echo "👥 Total merchants: $((merchant_count - 1))"  # Subtract header
        fi
        
        if [ -f "$TRANSACTIONS_FILE" ]; then
            transaction_count=$(wc -l < "$TRANSACTIONS_FILE")
            echo "💳 Total transactions: $((transaction_count - 1))"  # Subtract header
        fi
    fi
    
    echo ""
    echo "🎯 Next Steps:"
    echo "1. Review the generated CSV files"
    echo "2. Run incremental generation to add more data"
    echo "3. Build bronze layer ingestion pipeline"
    echo "4. Create silver layer transformations"
else
    echo ""
    echo "❌ Data generation failed!"
    exit 1
fi
