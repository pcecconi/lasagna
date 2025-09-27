# 🏦 Payments Aggregator Data Generator

A realistic data generator for a payments aggregator service that simulates real-world business dynamics including merchant lifecycle, size classification, and transaction patterns.

## 🎯 What This Generates

### **Raw Data Files:**
- **Merchants**: Merchant information with size classification and lifecycle tracking
- **Transactions**: Payment transactions with realistic patterns and business logic
- **State Management**: Tracks generation state for incremental processing

### **Business Dynamics:**
- **Merchant Growth**: 8% monthly growth with size-based variations
- **Merchant Churn**: 3% monthly churn with higher rates for small merchants
- **Size Classification**: Small (70%), Medium (25%), Large (5%) merchants
- **Transaction Patterns**: Realistic daily, seasonal, and size-based patterns
- **Geographic Distribution**: US-based merchant and transaction locations

## 📊 Data Schema

### **Merchants Table:**
```csv
merchant_id,merchant_name,industry,address,city,state,zip_code,phone,email,mdr_rate,size_category,creation_date,status
M000001,Best Store 123,retail,123 Main St,New York,NY,10001,555-123-4567,contact@m000001.com,0.032,small,2024-01-01,active
```

### **Transactions Table:**
```csv
payment_id,payment_timestamp,payment_lat,payment_lng,payment_amount,payment_type,terminal_id,card_type,card_issuer,card_brand,payment_status,merchant_id,transactional_cost_rate,transactional_cost_amount,mdr_amount,net_profit
550e8400-e29b-41d4-a716-446655440000,2024-01-01 09:15:30,40.712800,-74.006000,45.50,card_present,T1234,debit,Chase,visa,approved,M000001,0.005,0.23,1.46,1.23
```

## 🚀 Quick Start

### **📍 Execution Strategy**
- **Data Generation**: Run on **HOST** (outside container) for speed
- **Data Processing**: Run in **CONTAINER** for Spark/Iceberg integration

### **1. Setup Environment (Host)**
```bash
# Setup virtual environment
./setup_env.sh

# Activate environment
source venv/bin/activate
```

### **2. Generate Initial Dataset (Host)**
```bash
# Run the complete setup
./generate_data.sh

# Or run manually
python data_generator.py \
    --initial \
    --start-date 2024-01-01 \
    --end-date 2024-06-30 \
    --output-dir ./raw_data
```

### **3. Start Lasagna Stack (Host)**
```bash
cd /Users/pablo/workspace/lasagna/
docker compose up -d
```

### **4. Process Data (Container)**
```bash
# Access JupyterLab at http://localhost:8888
# Navigate to work/payments_pipeline/
# Run bronze/silver layer notebooks
```

### **5. Generate Incremental Data (Host)**
```bash
# Add one more day (auto-increment)
python data_generator.py --incremental

# Add specific date
python data_generator.py --incremental --date 2024-07-01
```

## 📈 Business Logic

### **Merchant Size Classification:**
- **Small Merchants (70%)**: $500-$10K/month, 3.0-3.5% MDR, higher churn
- **Medium Merchants (25%)**: $10K-$100K/month, 2.5-2.9% MDR, balanced
- **Large Merchants (5%)**: $100K-$1M/month, 2.0-2.5% MDR, lower churn

### **Transaction Patterns:**
- **Daily Activity**: Business hours (8 AM - 10 PM) with peak periods
- **Weekly Patterns**: Reduced activity on weekends
- **Seasonal Variations**: Holiday boost (1.5x), summer increase (1.2x)
- **Size-Based Amounts**: Small ($5-$100), Medium ($25-$300), Large ($100-$1K)

### **Lifecycle Dynamics:**
- **Growth**: 8% monthly with size-based multipliers
- **Churn**: 3% monthly with higher rates for small/inactive merchants
- **Activity**: Merchants don't transact every day (realistic patterns)

## 🔧 Configuration

### **Business Parameters:**
```python
BUSINESS_CONFIG = {
    'initial_merchants': 1000,
    'monthly_growth_rate': 0.08,  # 8%
    'monthly_churn_rate': 0.03,   # 3%
    'merchant_size_distribution': {
        'small': 0.70,    # 70%
        'medium': 0.25,   # 25%
        'large': 0.05     # 5%
    }
}
```

### **Transactional Costs:**
- **Debit Cards**: 0.5-0.8% (lower costs)
- **Credit Cards**: 1.5-2.5% (higher costs)
- **Amex**: Highest costs (2.5%)
- **Visa/Mastercard**: Standard costs

## 📁 Output Structure

```
raw_data/
├── merchants_initial_2024-01-01_2024-06-30.csv
├── transactions_initial_2024-01-01_2024-06-30.csv
├── transactions_20240701.csv
├── transactions_20240702.csv
└── data_state.json
```

## 🎯 Usage Examples

### **Generate Initial 6 Months:**
```bash
python data_generator.py \
    --initial \
    --start-date 2024-01-01 \
    --end-date 2024-06-30
```

### **Add One More Day:**
```bash
python data_generator.py --incremental
```

### **Add Specific Date:**
```bash
python data_generator.py --incremental --date 2024-07-15
```

### **Custom Output Directory:**
```bash
python data_generator.py \
    --incremental \
    --date 2024-07-01 \
    --output-dir /path/to/custom/directory
```

## 📊 Sample Data Volume

### **6 Months Initial Dataset:**
- **Merchants**: ~1,000 initial + growth
- **Transactions**: ~500,000+ transactions
- **File Size**: ~50-100 MB (depending on data)

### **Daily Incremental:**
- **Transactions**: ~3,000 transactions per day
- **File Size**: ~300-500 KB per day

## 🔄 State Management

The generator maintains state in `data_state.json`:
```json
{
  "last_generated_date": "2024-06-30",
  "merchants": {...},
  "merchant_counter": 1050,
  "total_transactions": 542000
}
```

This enables:
- **Incremental Generation**: Add one day at a time
- **State Persistence**: Resume from last generated date
- **Merchant Tracking**: Maintain merchant lifecycle state

## 🎯 Next Steps

After generating raw data:

1. **Bronze Layer Pipeline**: Ingest raw CSV files into Iceberg tables
2. **Silver Layer Pipeline**: Transform and model data with SCD Type 2
3. **Analytics**: Build dashboards and reports
4. **Monitoring**: Set up data quality checks

## 🛠️ Dependencies

```bash
pip install pandas numpy
```

## 📝 Notes

- **Realistic Patterns**: Based on real payments industry patterns
- **Scalable**: Easy to adjust volume and complexity
- **Extensible**: Add new business rules and patterns
- **Stateful**: Maintains generation state for incremental processing

This generator creates the perfect foundation for building a comprehensive payments aggregator data pipeline with realistic business dynamics!
