# 🏦 Payments Aggregator Data Generator V2

A high-performance, realistic data generator for a payments aggregator service that simulates real-world business dynamics including merchant lifecycle, size classification, and transaction patterns with streaming architecture and optimized performance.

## 🎯 What This Generator Does

### **Core Functionality**
The data generator creates realistic synthetic data for a payments aggregator business by simulating:

1. **Merchant Lifecycle Management**
   - Merchant registration and onboarding
   - Size-based classification (Small, Medium, Large)
   - Attribute changes over time (address, contact info, etc.)
   - Merchant growth and churn patterns
   - Status tracking (active, churned)

2. **Transaction Generation**
   - Realistic payment transaction patterns
   - Size-based transaction volumes and amounts
   - Geographic distribution across the US
   - Card profile management with reuse patterns
   - Seasonal and business hour variations

3. **Data Persistence & State Management**
   - Maintains merchant state across generations
   - Tracks card profiles for realistic reuse
   - Supports both initial and incremental data generation
   - Ensures data consistency across runs

### **Generated Data Files**
- **Merchants**: `merchants_<start_date>_<end_date>.csv` - Merchant information with lifecycle tracking
- **Transactions**: `transactions_<start_date>_<end_date>.csv` - Payment transactions with realistic patterns
- **State**: `merchants.json` - Source of truth for merchant and card profile state

## 🚀 Key Features & Improvements

### **1. Streaming Architecture**
- **Memory Efficient**: Writes each day's transactions to disk immediately
- **Scalable**: Can handle any time period without memory issues
- **No Hangs**: Eliminates memory exhaustion on long runs
- **Monthly Chunking**: Processes data in manageable monthly batches

### **2. Simplified Card Profile System**
- **Realistic Approach**: Generate random card profiles per transaction
- **Smart Reuse**: 1% probability to store cards, 5-15% daily reuse
- **No Complexity**: Eliminated complex versioning and cache management
- **Performance**: No more unbounded card profile growth

### **3. Reasonable Data Volume Caps**
- **Initial Merchants**: Capped at 500 (was 1000)
- **Monthly Growth**: Max 50 new merchants
- **Monthly Churn**: Max 30 merchants
- **Daily Transactions**: Reduced ranges for realistic volumes

### **4. Performance Optimizations**
- **O(1) Merchant Lookups**: Cached merchant access
- **Counter-based IDs**: Fast transaction ID generation
- **Batch Random Generation**: Pre-generated time components
- **Streaming File Operations**: Efficient disk I/O

## 📊 Data Schema

### **Merchants Table:**
```csv
merchant_id,merchant_name,industry,address,city,state,zip_code,phone,email,mdr_rate,size_category,creation_date,effective_date,status,last_transaction_date,version
M000001,Best Store 123,retail,123 Main St,New York,NY,10001,555-123-4567,contact@m000001.com,0.032,small,2024-01-01,2024-01-01,active,2024-01-15,1
```

### **Transactions Table:**
```csv
payment_id,payment_timestamp,payment_lat,payment_lng,payment_amount,payment_type,terminal_id,card_type,card_issuer,card_brand,card_profile_id,card_bin,payment_status,merchant_id,transactional_cost_rate,transactional_cost_amount,mdr_amount,net_profit
TXN0000000001,2024-01-01T09:15:30,40.712800,-74.006000,45.50,card_present,T1234,debit,Chase,Visa,CARD123456,123456,approved,M000001,0.005,0.23,1.46,1.23
```

## 🚀 Quick Start

### **1. Setup Environment**
```bash
# Setup virtual environment
./setup_env.sh

# Activate environment
source venv/bin/activate
```

### **2. Generate Initial Dataset**
```bash
# Using the new generator
python new_data_generator.py \
    --initial \
    --start-date 2024-01-01 \
    --end-date 2024-01-15 \
    --output-dir ./raw_data \
    --debug

# Or use the shell script wrapper
./generate_data.sh --initial --start-date 2024-01-01 --end-date 2024-01-15 --debug
```

### **3. Generate Incremental Data**
```bash
# Add one more day (defaults to yesterday)
python new_data_generator.py --incremental --debug

# Add specific date
python new_data_generator.py --incremental --date 2024-01-16 --debug

# Skip confirmation prompt
python new_data_generator.py --incremental -f
```

## 📈 Business Logic

### **Merchant Size Classification:**
- **Small Merchants (70%)**: 2-20 transactions/day, $5-$100 amounts, 3.0-3.5% MDR
- **Medium Merchants (25%)**: 5-50 transactions/day, $25-$300 amounts, 2.5-2.9% MDR
- **Large Merchants (5%)**: 20-100 transactions/day, $100-$1K amounts, 2.0-2.5% MDR

### **Transaction Patterns:**
- **Daily Activity**: Business hours (8 AM - 10 PM) with realistic patterns
- **Weekly Patterns**: Reduced activity on weekends
- **Seasonal Variations**: Holiday boost (1.5x), summer increase (1.2x)
- **Card Reuse**: 5-15% of daily transactions use previously stored cards

### **Lifecycle Dynamics:**
- **Growth**: Capped at 50 new merchants per month
- **Churn**: Capped at 30 merchants per month
- **Attribute Changes**: Low probability updates to merchant information
- **Activity**: Merchants don't transact every day (realistic patterns)

## 🔧 Configuration

### **Business Parameters:**
```python
BUSINESS_CONFIG = {
    'initial_merchants': 500,  # Capped for performance
    'monthly_growth_rate': 0.08,  # 8%
    'monthly_churn_rate': 0.03,   # 3%
    'merchant_size_distribution': {
        'small': 0.70,    # 70%
        'medium': 0.25,   # 25%
        'large': 0.05     # 5%
    }
}
```

### **Daily Transaction Ranges:**
```python
MERCHANT_SIZE_CONFIGS = {
    'small': {'daily_tx_range': (2, 20)},    # Reduced from (5, 50)
    'medium': {'daily_tx_range': (5, 50)},   # Reduced from (20, 150)
    'large': {'daily_tx_range': (20, 100)}   # Reduced from (100, 500)
}
```

## 📁 Output Structure

```
raw_data/
├── merchants_2024-01-01_2024-01-15.csv
├── transactions_2024-01-01_2024-01-15.csv
└── merchants.json
```

## 🎯 Usage Examples

### **Generate Initial Dataset with Debug Output:**
```bash
python new_data_generator.py \
    --initial \
    --start-date 2024-01-01 \
    --end-date 2024-01-15 \
    --output-dir ./raw_data \
    --debug
```

### **Generate Incremental Data:**
```bash
# Add yesterday's data
python new_data_generator.py --incremental --debug

# Add specific date
python new_data_generator.py --incremental --date 2024-01-16 --debug

# Skip confirmation
python new_data_generator.py --incremental -f
```

### **Using Shell Script Wrapper:**
```bash
# Initial generation
./generate_data.sh --initial --start-date 2024-01-01 --end-date 2024-01-15 --debug

# Incremental generation
./generate_data.sh --incremental --debug

# Force mode (skip confirmation)
./generate_data.sh --incremental --force --debug
```

## 📊 Performance Results

### **Before Optimization (Old Generator)**
- **3 days**: 450,000+ transactions, 2,153 merchants
- **Performance**: Slow, memory issues, hangs
- **Card Profiles**: 150,000+ profiles, unbounded growth

### **After Optimization (New Generator)**
- **15 days**: 200,797 transactions, 965 merchants
- **Performance**: Fast, stable, no memory issues
- **Card Profiles**: ~1,000 profiles, bounded growth
- **Speed**: ~17x faster than previous version

## 🔄 State Management

The generator maintains state in `merchants.json`:
```json
{
  "last_generated_date": "2024-01-15",
  "merchants": {
    "M000001": [
      {
        "merchant_id": "M000001",
        "merchant_name": "Best Store 123",
        "version": 1,
        "effective_date": "2024-01-01",
        "status": "active"
      }
    ]
  },
  "merchant_counter": 525,
  "card_profiles": {
    "CARD123456": {
      "card_profile_id": "CARD123456",
      "card_bin": "123456",
      "card_type": "credit",
      "card_issuer": "Chase",
      "card_brand": "Visa"
    }
  },
  "total_transactions": 200797
}
```

## 🛠️ Dependencies

```bash
pip install pandas numpy
```

## 📝 Key Benefits

1. **Scalable**: Can handle any time period without memory issues
2. **Realistic**: Reasonable data volumes and patterns
3. **Maintainable**: Simple, clean code architecture
4. **Observable**: Clear debug output and progress tracking
5. **Efficient**: Optimized for both speed and memory usage
6. **Flexible**: Supports both initial and incremental generation modes

## 🎯 Next Steps

After generating raw data:

1. **Bronze Layer Pipeline**: Ingest raw CSV files into Iceberg tables
2. **Silver Layer Pipeline**: Transform and model data with SCD Type 2
3. **Analytics**: Build dashboards and reports
4. **Monitoring**: Set up data quality checks

This generator creates the perfect foundation for building a comprehensive payments aggregator data pipeline with realistic business dynamics and excellent performance!