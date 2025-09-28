#!/usr/bin/env python3
"""
Payments Aggregator Raw Data Generator

This script generates realistic raw data files for a payments aggregator service,
incorporating all business dynamics:
- Merchant lifecycle (growth, churn, inactivity)
- Merchant size classification (small/medium/large)
- Realistic transaction patterns
- Seasonal variations
- Incremental data generation

Usage:
    python data_generator.py --initial --start-date 2024-01-01 --end-date 2024-06-30
    python data_generator.py --incremental --date 2024-07-01
"""

import pandas as pd
import numpy as np
import random
import json
import csv
import os
from datetime import datetime, date, timedelta
from typing import Dict, List, Tuple, Optional
import argparse
import calendar
from pathlib import Path
import uuid

class PaymentsDataGenerator:
    def __init__(self, output_dir: str = "./raw_data"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # Business configuration
        self.config = {
            'initial_merchants': 1000,
            'monthly_growth_rate': 0.08,
            'monthly_churn_rate': 0.03,
            'daily_transaction_volume': 3000,
            'merchant_size_distribution': {
                'small': 0.70,    # 70% small merchants
                'medium': 0.25,   # 25% medium merchants
                'large': 0.05     # 5% large merchants
            }
        }
        
        # Merchant size configurations
        self.size_configs = {
            'small': {
                'monthly_volume_range': (500, 10000),
                'avg_transaction': 25.00,
                'mdr_rate_range': (0.029, 0.035),
                'daily_tx_range': (5, 50),
                'active_days_per_month': 22,
                'churn_multiplier': 1.5,
                'growth_multiplier': 1.2
            },
            'medium': {
                'monthly_volume_range': (10000, 100000),
                'avg_transaction': 75.00,
                'mdr_rate_range': (0.025, 0.029),
                'daily_tx_range': (20, 150),
                'active_days_per_month': 26,
                'churn_multiplier': 1.0,
                'growth_multiplier': 1.0
            },
            'large': {
                'monthly_volume_range': (100000, 1000000),
                'avg_transaction': 200.00,
                'mdr_rate_range': (0.020, 0.025),
                'daily_tx_range': (100, 500),
                'active_days_per_month': 28,
                'churn_multiplier': 0.3,
                'growth_multiplier': 0.8
            }
        }
        
        # Industries by merchant size
        self.industries = {
            'small': ['retail', 'restaurant', 'beauty', 'fitness', 'local_services'],
            'medium': ['retail', 'restaurant', 'healthcare', 'education', 'automotive'],
            'large': ['retail', 'healthcare', 'manufacturing', 'logistics', 'technology']
        }
        
        # Card configurations
        self.card_brands = ['visa', 'mastercard', 'amex', 'discover']
        self.card_types = ['debit', 'credit']
        self.card_issuers = [
            'Chase', 'Bank of America', 'Wells Fargo', 'Citi', 'Capital One',
            'American Express', 'Discover', 'US Bank', 'PNC', 'TD Bank'
        ]
        
        # Payment types and statuses
        self.payment_types = ['card_present', 'card_not_present']
        self.payment_statuses = ['approved', 'declined', 'cancelled']
        self.status_probabilities = {
            'approved': 0.85,
            'declined': 0.12,
            'cancelled': 0.03
        }
        
        # Transactional costs by card type/brand combination
        self.transactional_costs = {
            ('debit', 'visa'): 0.005,      # 0.5%
            ('debit', 'mastercard'): 0.006, # 0.6%
            ('credit', 'visa'): 0.015,     # 1.5%
            ('credit', 'mastercard'): 0.016, # 1.6%
            ('credit', 'amex'): 0.025,     # 2.5%
            ('credit', 'discover'): 0.018,  # 1.8%
            ('debit', 'amex'): 0.008,      # 0.8%
            ('debit', 'discover'): 0.007   # 0.7%
        }
        
        # State management
        self.state_file = self.output_dir / "data_state.json"
        self.state = self.load_state()

    def load_state(self) -> Dict:
        """Load generation state from file"""
        if self.state_file.exists():
            with open(self.state_file, 'r') as f:
                return json.load(f)
        return {
            'last_generated_date': None,
            'merchants': {},
            'merchant_counter': 1,
            'total_transactions': 0
        }

    def save_state(self):
        """Save current generation state"""
        with open(self.state_file, 'w') as f:
            json.dump(self.state, f, indent=2, default=str)

    def generate_merchant_id(self) -> str:
        """Generate unique merchant ID"""
        merchant_id = f"M{self.state['merchant_counter']:06d}"
        self.state['merchant_counter'] += 1
        return merchant_id

    def assign_merchant_size(self) -> str:
        """Assign merchant size based on distribution"""
        rand = random.random()
        cumulative = 0
        for size, prob in self.config['merchant_size_distribution'].items():
            cumulative += prob
            if rand <= cumulative:
                return size
        return 'small'

    def generate_merchant(self, merchant_id: str, creation_date: date, size: str = None) -> Dict:
        """Generate a single merchant record"""
        if size is None:
            size = self.assign_merchant_size()
        
        config = self.size_configs[size]
        
        # Generate merchant attributes
        merchant = {
            'merchant_id': merchant_id,
            'merchant_name': f"{random.choice(['Best', 'Elite', 'Prime', 'Super', 'Mega'])} {random.choice(['Store', 'Shop', 'Mart', 'Center', 'Plaza'])} {random.randint(1, 999)}",
            'industry': random.choice(self.industries[size]),
            'address': f"{random.randint(100, 9999)} {random.choice(['Main', 'Oak', 'First', 'Second', 'Park'])} St",
            'city': random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia']),
            'state': random.choice(['NY', 'CA', 'IL', 'TX', 'AZ', 'PA']),
            'zip_code': f"{random.randint(10000, 99999)}",
            'phone': f"{random.randint(200, 999)}-{random.randint(200, 999)}-{random.randint(1000, 9999)}",
            'email': f"contact@{merchant_id.lower()}.com",
            'mdr_rate': round(random.uniform(*config['mdr_rate_range']), 4),
            'size_category': size,
            'creation_date': creation_date,
            'status': 'active',
            'last_transaction_date': None
        }
        
        return merchant

    def generate_initial_merchants(self, start_date: date, count: int = None) -> List[Dict]:
        """Generate initial merchant base"""
        if count is None:
            count = self.config['initial_merchants']
        
        merchants = []
        for i in range(count):
            merchant_id = self.generate_merchant_id()
            merchant = self.generate_merchant(merchant_id, start_date)
            merchants.append(merchant)
            self.state['merchants'][merchant_id] = merchant
        
        return merchants

    def generate_new_merchants(self, target_date: date, count: int) -> List[Dict]:
        """Generate new merchants for growth"""
        new_merchants = []
        for _ in range(count):
            merchant_id = self.generate_merchant_id()
            merchant = self.generate_merchant(merchant_id, target_date)
            new_merchants.append(merchant)
            self.state['merchants'][merchant_id] = merchant
        
        return new_merchants

    def select_merchants_for_churn(self, target_date: date, churn_count: int) -> List[str]:
        """Select merchants for churn based on size and activity"""
        active_merchants = [
            mid for mid, m in self.state['merchants'].items() 
            if m['status'] == 'active'
        ]
        
        # Weight churn probability by size (small merchants more likely to churn)
        churn_weights = []
        for mid in active_merchants:
            merchant = self.state['merchants'][mid]
            size = merchant['size_category']
            weight = self.size_configs[size]['churn_multiplier']
            
            # Increase weight if merchant has been inactive
            days_since_last_tx = 0
            if merchant['last_transaction_date']:
                days_since_last_tx = (target_date - merchant['last_transaction_date']).days
            
            if days_since_last_tx > 30:
                weight *= 2  # Double churn probability for inactive merchants
            
            churn_weights.append(weight)
        
        # Select merchants based on weighted probability
        selected = np.random.choice(
            active_merchants, 
            size=min(churn_count, len(active_merchants)), 
            replace=False,
            p=np.array(churn_weights) / sum(churn_weights)
        )
        
        return selected.tolist()

    def process_merchant_changes(self, target_date: date) -> Tuple[List[Dict], List[str]]:
        """Process monthly merchant lifecycle changes"""
        # Calculate new merchants (growth)
        active_count = len([m for m in self.state['merchants'].values() if m['status'] == 'active'])
        new_count = int(active_count * self.config['monthly_growth_rate'])
        
        # Calculate churn
        churn_count = int(active_count * self.config['monthly_churn_rate'])
        
        # Generate new merchants
        new_merchants = self.generate_new_merchants(target_date, new_count)
        
        # Select merchants for churn
        churned_merchants = self.select_merchants_for_churn(target_date, churn_count)
        
        # Update churned merchants status
        for merchant_id in churned_merchants:
            self.state['merchants'][merchant_id]['status'] = 'churned'
            self.state['merchants'][merchant_id]['churn_date'] = target_date
        
        return new_merchants, churned_merchants

    def is_merchant_active_on_date(self, merchant: Dict, target_date: date) -> bool:
        """Determine if merchant should transact on a given date"""
        if merchant['status'] != 'active':
            return False
        
        size = merchant['size_category']
        config = self.size_configs[size]
        
        # Check if it's a day when merchant should be active
        active_days_per_month = config['active_days_per_month']
        days_in_month = calendar.monthrange(target_date.year, target_date.month)[1]
        activity_rate = active_days_per_month / days_in_month
        
        # Weekend adjustment
        if target_date.weekday() >= 5:  # Weekend
            activity_rate *= 0.7
        
        return random.random() < activity_rate

    def generate_transaction(self, merchant: Dict, target_date: date, target_time: datetime) -> Dict:
        """Generate a single transaction"""
        size = merchant['size_category']
        config = self.size_configs[size]
        
        # Transaction amount based on size
        if size == 'small':
            amount = round(random.uniform(5, 100), 2)
        elif size == 'medium':
            amount = round(random.uniform(25, 300), 2)
        else:  # large
            amount = round(random.uniform(100, 1000), 2)
        
        # Payment attributes
        card_type = random.choice(self.card_types)
        card_brand = random.choice(self.card_brands)
        card_issuer = random.choice(self.card_issuers)
        payment_type = random.choice(self.payment_types)
        
        # Payment status based on probabilities
        status = np.random.choice(
            list(self.status_probabilities.keys()),
            p=list(self.status_probabilities.values())
        )
        
        # Transactional cost
        cost_key = (card_type, card_brand)
        if cost_key in self.transactional_costs:
            cost_rate = self.transactional_costs[cost_key]
        else:
            cost_rate = 0.015  # Default 1.5%
        
        # Geographic coordinates (simplified US coordinates)
        lat = round(random.uniform(25.0, 49.0), 6)
        lng = round(random.uniform(-125.0, -66.0), 6)
        
        transaction = {
            'payment_id': str(uuid.uuid4()),
            'payment_timestamp': target_time.isoformat(),
            'payment_lat': lat,
            'payment_lng': lng,
            'payment_amount': amount,
            'payment_type': payment_type,
            'terminal_id': f"T{random.randint(1000, 9999)}" if payment_type == 'card_present' else None,
            'card_type': card_type,
            'card_issuer': card_issuer,
            'card_brand': card_brand,
            'payment_status': status,
            'merchant_id': merchant['merchant_id'],
            'transactional_cost_rate': cost_rate,
            'transactional_cost_amount': round(amount * cost_rate, 2),
            'mdr_amount': round(amount * merchant['mdr_rate'], 2),
            'net_profit': round(amount * merchant['mdr_rate'] - amount * cost_rate, 2)
        }
        
        return transaction

    def generate_daily_transactions(self, target_date: date) -> List[Dict]:
        """Generate all transactions for a given date"""
        transactions = []
        
        # Get active merchants
        active_merchants = [
            m for m in self.state['merchants'].values() 
            if m['status'] == 'active'
        ]
        
        # Generate transactions for each merchant
        for merchant in active_merchants:
            if not self.is_merchant_active_on_date(merchant, target_date):
                continue
            
            size = merchant['size_category']
            config = self.size_configs[size]
            
            # Number of transactions for this merchant today
            tx_count = random.randint(*config['daily_tx_range'])
            
            # Seasonal adjustment
            if target_date.month in [11, 12]:  # Holiday season
                tx_count = int(tx_count * 1.5)
            elif target_date.month in [6, 7]:  # Summer
                tx_count = int(tx_count * 1.2)
            
            # Generate transactions throughout the day
            for _ in range(tx_count):
                # Business hours: 8 AM to 10 PM
                hour = random.randint(8, 22)
                minute = random.randint(0, 59)
                second = random.randint(0, 59)
                
                transaction_time = datetime.combine(target_date, datetime.min.time()) + timedelta(
                    hours=hour, minutes=minute, seconds=second
                )
                
                transaction = self.generate_transaction(merchant, target_date, transaction_time)
                transactions.append(transaction)
                
                # Update merchant's last transaction date
                self.state['merchants'][merchant['merchant_id']]['last_transaction_date'] = target_date
        
        self.state['total_transactions'] += len(transactions)
        return transactions

    def generate_initial_data(self, start_date: date, end_date: date):
        """Generate initial dataset for date range"""
        print(f"🚀 Generating initial data from {start_date} to {end_date}")
        
        # Generate initial merchants
        print("👥 Generating initial merchant base...")
        initial_merchants = self.generate_initial_merchants(start_date)
        
        # Save initial merchants
        merchants_file = self.output_dir / f"merchants_initial_{start_date}_{end_date}.csv"
        pd.DataFrame(initial_merchants).to_csv(merchants_file, index=False)
        print(f"✅ Saved {len(initial_merchants)} merchants to {merchants_file}")
        
        # Generate data for each month in chunks
        current_date = start_date.replace(day=1)  # Start of first month
        total_transactions = 0
        transaction_files = []
        
        while current_date <= end_date:
            month_end = (current_date + timedelta(days=32)).replace(day=1) - timedelta(days=1)
            month_end = min(month_end, end_date)
            
            print(f"📅 Processing {current_date.strftime('%Y-%m')}...")
            
            # Process monthly merchant changes (growth/churn)
            if current_date.day == 1:  # First day of month
                new_merchants, churned_merchants = self.process_merchant_changes(current_date)
                print(f"   📈 Added {len(new_merchants)} new merchants")
                print(f"   📉 Churned {len(churned_merchants)} merchants")
            
            # Generate daily transactions for this month
            month_transactions = []
            daily_date = current_date
            while daily_date <= month_end:
                daily_transactions = self.generate_daily_transactions(daily_date)
                month_transactions.extend(daily_transactions)
                daily_date += timedelta(days=1)
            
            # Save monthly transactions to avoid memory accumulation
            month_file = self.output_dir / f"transactions_{current_date.strftime('%Y%m')}.csv"
            pd.DataFrame(month_transactions).to_csv(month_file, index=False)
            transaction_files.append(month_file)
            total_transactions += len(month_transactions)
            print(f"   💾 Saved {len(month_transactions)} transactions to {month_file}")
            
            current_date = (current_date + timedelta(days=32)).replace(day=1)
        
        # Combine all monthly files into a single file using streaming
        print("🔄 Combining monthly transaction files...")
        transactions_file = self.output_dir / f"transactions_initial_{start_date}_{end_date}.csv"
        
        with open(transactions_file, 'w', newline='') as outfile:
            writer = None
            for i, monthly_file in enumerate(transaction_files):
                with open(monthly_file, 'r') as infile:
                    if i == 0:
                        # First file: copy header and all data
                        outfile.write(infile.read())
                    else:
                        # Subsequent files: skip header, copy data only
                        next(infile)  # Skip header
                        outfile.write(infile.read())
        
        # Clean up monthly files
        for f in transaction_files:
            f.unlink()
        
        print(f"✅ Saved {total_transactions} transactions to {transactions_file}")
        
        # Update state
        self.state['last_generated_date'] = end_date
        self.save_state()
        
        print(f"🎉 Initial data generation complete!")
        print(f"   📊 Total merchants: {len(self.state['merchants'])}")
        print(f"   💳 Total transactions: {total_transactions}")

    def generate_incremental_data(self, target_date: date):
        """Generate incremental data for a specific date"""
        print(f"🔄 Generating incremental data for {target_date}")
        
        # Check if it's the first day of month (merchant changes)
        if target_date.day == 1:
            new_merchants, churned_merchants = self.process_merchant_changes(target_date)
            print(f"📈 Added {len(new_merchants)} new merchants")
            print(f"📉 Churned {len(churned_merchants)} merchants")
        
        # Generate transactions for the day
        daily_transactions = self.generate_daily_transactions(target_date)
        
        # Save incremental data
        transactions_file = self.output_dir / f"transactions_{target_date.strftime('%Y%m%d')}.csv"
        pd.DataFrame(daily_transactions).to_csv(transactions_file, index=False)
        print(f"✅ Saved {len(daily_transactions)} transactions to {transactions_file}")
        
        # Update state
        self.state['last_generated_date'] = target_date
        self.save_state()
        
        print(f"🎉 Incremental data generation complete!")

def main():
    parser = argparse.ArgumentParser(description='Generate payments aggregator raw data')
    parser.add_argument('--initial', action='store_true', help='Generate initial dataset')
    parser.add_argument('--incremental', action='store_true', help='Generate incremental data')
    parser.add_argument('--start-date', type=str, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='End date (YYYY-MM-DD)')
    parser.add_argument('--date', type=str, help='Specific date for incremental (YYYY-MM-DD)')
    parser.add_argument('--output-dir', type=str, default='./raw_data', help='Output directory')
    
    args = parser.parse_args()
    
    # Initialize generator
    generator = PaymentsDataGenerator(output_dir=args.output_dir)
    
    if args.initial:
        if not args.start_date or not args.end_date:
            print("❌ Initial generation requires --start-date and --end-date")
            return
        
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()
        
        generator.generate_initial_data(start_date, end_date)
    
    elif args.incremental:
        if args.date:
            target_date = datetime.strptime(args.date, '%Y-%m-%d').date()
        else:
            # Use next day from last generated
            if generator.state['last_generated_date']:
                last_date = datetime.strptime(generator.state['last_generated_date'], '%Y-%m-%d').date()
                target_date = last_date + timedelta(days=1)
            else:
                print("❌ No previous data found. Use --initial first.")
                return
        
        generator.generate_incremental_data(target_date)
    
    else:
        print("❌ Please specify --initial or --incremental")

if __name__ == "__main__":
    main()
