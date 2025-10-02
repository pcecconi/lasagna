#!/usr/bin/env python3
"""
Payments Aggregator Raw Data Generator V2

This is the refactored version of the data generator with the following improvements:
- Always generates both merchant and transaction files
- Supports merchant attribute updates with probabilistic changes
- Includes card profile system with card_profile_id and card_bin
- Maintains merchants.json as source of truth
- Consistent file naming with date ranges
- Confirmation prompts before generation

Usage:
    python new_data_generator.py --initial --start-date 2024-01-01 --end-date 2024-06-30
    python new_data_generator.py --incremental --date 2024-07-01
    python new_data_generator.py --incremental  # defaults to yesterday
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

# Import configuration
from config import (
    BUSINESS_CONFIG, MERCHANT_SIZE_CONFIGS, PAYMENT_CONFIG, 
    TRANSACTIONAL_COSTS, GEO_CONFIG, SEASONAL_CONFIG, DATA_CONFIG
)


class PaymentsDataGeneratorV2:
    def __init__(self, output_dir: str = "./raw_data", debug: bool = False):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.debug = debug
        
        # Configuration from config.py
        self.config = BUSINESS_CONFIG
        self.size_configs = MERCHANT_SIZE_CONFIGS
        self.payment_config = PAYMENT_CONFIG
        self.transactional_costs = TRANSACTIONAL_COSTS
        self.geo_config = GEO_CONFIG
        self.seasonal_config = SEASONAL_CONFIG
        self.data_config = DATA_CONFIG
        
        # State management
        self.merchants_file = self.output_dir / "merchants.json"
        self.state = self.load_state()
        
        # Initialize transaction counter for fast ID generation
        self.transaction_counter = self.state.get('transaction_counter', 0)
        
        # Define transaction columns for CSV writing
        self.transaction_columns = [
            'payment_id', 'payment_timestamp', 'payment_lat', 'payment_lng',
            'payment_amount', 'payment_type', 'terminal_id', 'card_type',
            'card_issuer', 'card_brand', 'card_profile_id', 'card_bin',
            'payment_status', 'merchant_id', 'transactional_cost_rate',
            'transactional_cost_amount', 'mdr_amount', 'net_profit'
        ]
        
        # Cache current merchants to avoid expensive lookups
        self._current_merchants_cache = {}
        self._rebuild_current_merchants_cache()
        
        # Initialize card profile cache
        self._cached_card_profile_keys = list(self.state['card_profiles'].keys())
        self._cached_card_profile_count = len(self.state['card_profiles'])
        
        # Merchant attribute change probabilities
        self.merchant_change_probabilities = {
            'mdr_rate': 0.02,      # 2% chance per period
            'phone': 0.01,        # 1% chance per period
            'email': 0.005,       # 0.5% chance per period
            'address': 0.01,      # 1% chance per period
            'city': 0.005,        # 0.5% chance per period
            'state': 0.003,       # 0.3% chance per period
            'zip_code': 0.01,     # 1% chance per period
        }
        
        if self.debug:
            print(f"🐛 DEBUG: Initialized PaymentsDataGeneratorV2")
            print(f"🐛 DEBUG: Output directory: {self.output_dir}")
            print(f"🐛 DEBUG: Merchants file: {self.merchants_file}")
            print(f"🐛 DEBUG: State loaded: {len(self.state.get('merchants', {}))} merchants")

    def load_state(self) -> Dict:
        """Load generation state from merchants.json file"""
        if self.merchants_file.exists():
            with open(self.merchants_file, 'r') as f:
                return json.load(f)
        return {
            'last_generated_date': None,
            'merchants': {},
            'merchant_counter': 1,
            'card_profiles': {},  # Simplified: just stores cards for reuse
            'total_transactions': 0
        }

    def save_state(self):
        """Save current generation state to merchants.json"""
        with open(self.merchants_file, 'w') as f:
            json.dump(self.state, f, indent=2, default=str)

    def initialize_state(self):
        """Initialize state if it doesn't exist"""
        if not self.merchants_file.exists():
            self.save_state()

    def generate_merchant_id(self) -> str:
        """Generate unique merchant ID"""
        merchant_id = f"M{self.state['merchant_counter']:06d}"
        self.state['merchant_counter'] += 1
        return merchant_id

# Removed complex card profile generation methods - using simplified approach

    def add_card_profile_to_state(self, card_profile: Dict):
        """Add card profile to state (simplified)"""
        profile_id = card_profile['card_profile_id']
        self.state['card_profiles'][profile_id] = card_profile

# Removed get_existing_card_profile - using simplified approach in generate_daily_transactions

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
            'industry': random.choice(config['industries']),
            'address': f"{random.randint(100, 9999)} {random.choice(['Main', 'Oak', 'First', 'Second', 'Park'])} St",
            'city': random.choice(self.geo_config['major_cities'])['name'],
            'state': random.choice(self.geo_config['major_cities'])['state'],
            'zip_code': f"{random.randint(10000, 99999)}",
            'phone': f"{random.randint(200, 999)}-{random.randint(200, 999)}-{random.randint(1000, 9999)}",
            'email': f"contact@{merchant_id.lower()}.com",
            'mdr_rate': round(random.uniform(*config['mdr_rate_range']), 4),
            'size_category': size,
            'creation_date': creation_date,
            'effective_date': creation_date,
            'status': 'active',
            'last_transaction_date': None,
            'version': 1,
            'change_type': 'initial',
            'churn_date': None
        }
        
        # Debug output removed for verbosity - only show counts
        
        return merchant

    def add_merchant_to_state(self, merchant: Dict):
        """Add merchant to state with versioning"""
        merchant_id = merchant['merchant_id']
        
        if merchant_id not in self.state['merchants']:
            self.state['merchants'][merchant_id] = []
        
        self.state['merchants'][merchant_id].append(merchant)
        
        # Update cache with the new current merchant
        self._current_merchants_cache[merchant_id] = merchant

    def get_merchant_history(self, merchant_id: str) -> List[Dict]:
        """Get merchant history (all versions)"""
        return self.state['merchants'].get(merchant_id, [])

    def _rebuild_current_merchants_cache(self):
        """Rebuild the current merchants cache"""
        self._current_merchants_cache = {}
        for merchant_id, history in self.state['merchants'].items():
            if history:
                # Get the latest version (highest version number)
                current = max(history, key=lambda x: x['version'])
                self._current_merchants_cache[merchant_id] = current

    def get_current_merchant(self, merchant_id: str) -> Optional[Dict]:
        """Get current (latest) version of merchant from cache"""
        return self._current_merchants_cache.get(merchant_id)

    def generate_merchant_update(self, merchant_id: str, effective_date: date, changes: Dict) -> Dict:
        """Generate merchant attribute update"""
        current_merchant = self.get_current_merchant(merchant_id)
        if not current_merchant:
            raise ValueError(f"Merchant {merchant_id} not found")
        
        # Ensure effective_date is different from current version's effective_date
        # If they're the same, increment by 1 day to avoid SCD Type 2 overlap
        current_effective_date = current_merchant['effective_date']
        if isinstance(current_effective_date, str):
            current_effective_date = datetime.strptime(current_effective_date, '%Y-%m-%d').date()
        
        if effective_date == current_effective_date:
            effective_date = effective_date + timedelta(days=1)
        
        # Create updated merchant
        updated_merchant = current_merchant.copy()
        updated_merchant.update(changes)
        updated_merchant['effective_date'] = effective_date
        updated_merchant['version'] = current_merchant['version'] + 1
        
        return updated_merchant

    def generate_merchant_updates(self, start_date: date, end_date: date) -> List[Dict]:
        """Generate probabilistic merchant updates for date range"""
        updates = []
        
        # Get active merchants
        active_merchants = []
        for merchant_id, history in self.state['merchants'].items():
            current = self.get_current_merchant(merchant_id)
            if current and current['status'] == 'active':
                active_merchants.append(merchant_id)
        
        # Process each day in the range
        current_date = start_date
        while current_date <= end_date:
            # Check for merchant attribute changes
            for merchant_id in active_merchants:
                current_merchant = self.get_current_merchant(merchant_id)
                if not current_merchant:
                    continue
                
                # Check each attribute for potential change
                changes = {}
                for attribute, probability in self.merchant_change_probabilities.items():
                    if random.random() < probability:
                        if attribute == 'mdr_rate':
                            size_config = self.size_configs[current_merchant['size_category']]
                            changes[attribute] = round(random.uniform(*size_config['mdr_rate_range']), 4)
                        elif attribute == 'phone':
                            changes[attribute] = f"{random.randint(200, 999)}-{random.randint(200, 999)}-{random.randint(1000, 9999)}"
                        elif attribute == 'email':
                            changes[attribute] = f"contact@{merchant_id.lower()}.com"
                        elif attribute == 'address':
                            changes[attribute] = f"{random.randint(100, 9999)} {random.choice(['Main', 'Oak', 'First', 'Second', 'Park'])} St"
                        elif attribute == 'city':
                            city_info = random.choice(self.geo_config['major_cities'])
                            changes[attribute] = city_info['name']
                            changes['state'] = city_info['state']
                        elif attribute == 'zip_code':
                            changes[attribute] = f"{random.randint(10000, 99999)}"
                
                if changes:
                    update = self.generate_merchant_update(merchant_id, current_date, changes)
                    update['change_type'] = 'attribute_change'
                    # Ensure churn_date is None for attribute changes (not churned)
                    if 'churn_date' not in update:
                        update['churn_date'] = None
                    updates.append(update)
                    self.add_merchant_to_state(update)
            
            # Move to next day
            current_date += timedelta(days=1)
        
        return updates

    def generate_new_merchants(self, target_date: date, count: int) -> List[Dict]:
        """Generate new merchants for growth"""
        new_merchants = []
        for _ in range(count):
            merchant_id = self.generate_merchant_id()
            merchant = self.generate_merchant(merchant_id, target_date)
            new_merchants.append(merchant)
            self.add_merchant_to_state(merchant)
        
        return new_merchants

    def select_merchants_for_churn(self, target_date: date, churn_count: int) -> List[str]:
        """Select merchants for churn based on size and activity"""
        active_merchants = []
        for merchant_id, history in self.state['merchants'].items():
            current = self.get_current_merchant(merchant_id)
            if current and current['status'] == 'active':
                active_merchants.append(merchant_id)
        
        if not active_merchants:
            return []
        
        # Weight churn probability by size
        churn_weights = []
        for merchant_id in active_merchants:
            current = self.get_current_merchant(merchant_id)
            size = current['size_category']
            weight = self.size_configs[size]['churn_multiplier']
            
            # Increase weight if merchant has been inactive
            days_since_last_tx = 0
            if current['last_transaction_date']:
                if isinstance(current['last_transaction_date'], str):
                    last_tx_date = datetime.strptime(current['last_transaction_date'], '%Y-%m-%d').date()
                else:
                    last_tx_date = current['last_transaction_date']
                days_since_last_tx = (target_date - last_tx_date).days
            
            if days_since_last_tx > 30:
                weight *= 2
            
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
        """Process monthly merchant lifecycle changes with caps"""
        # Calculate new merchants (growth) with caps
        active_count = len([
            merchant_id for merchant_id, history in self.state['merchants'].items()
            if self.get_current_merchant(merchant_id) and self.get_current_merchant(merchant_id)['status'] == 'active'
        ])
        
        # Cap monthly changes to reasonable numbers
        max_new_merchants = min(50, active_count // 20)  # Max 50 or 5% of existing
        max_churned_merchants = min(30, active_count // 30)  # Max 30 or 3% of existing
        
        new_count = min(int(active_count * self.config['monthly_growth_rate']), max_new_merchants)
        churn_count = min(int(active_count * self.config['monthly_churn_rate']), max_churned_merchants)
        
        # Generate new merchants
        new_merchants = self.generate_new_merchants(target_date, new_count)
        
        # Select merchants for churn
        churned_merchants = self.select_merchants_for_churn(target_date, churn_count)
        
        # Update churned merchants status
        for merchant_id in churned_merchants:
            current = self.get_current_merchant(merchant_id)
            if current:
                churn_update = self.generate_merchant_update(
                    merchant_id, 
                    target_date, 
                    {'status': 'churned', 'churn_date': target_date}
                )
                self.add_merchant_to_state(churn_update)
        
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
            activity_rate *= self.seasonal_config['weekend_multiplier']
        
        return random.random() < activity_rate

    def generate_transaction_with_card_profile(self, merchant: Dict, target_date: date, target_time: datetime) -> Dict:
        """Generate a single transaction with card profile"""
        # Handle case where merchant might be missing size_category (for testing)
        if 'size_category' not in merchant:
            merchant['size_category'] = 'medium'  # Default for testing
        
        size = merchant['size_category']
        config = self.size_configs[size]
        
        # Transaction amount based on size
        if size == 'small':
            amount = round(random.uniform(5, 100), 2)
        elif size == 'medium':
            amount = round(random.uniform(25, 300), 2)
        else:  # large
            amount = round(random.uniform(100, 1000), 2)
        
        # Simplified card profile generation
        card_profile_id = f"CARD{random.randint(100000, 999999)}"
        card_bin = f"{random.randint(100000, 999999)}"
        
        # Very low probability to store this card for future reuse
        if random.random() < 0.01:  # 1% chance to store
            card_profile = {
                'card_profile_id': card_profile_id,
                'card_bin': card_bin,
                'card_type': random.choice(['credit', 'debit']),
                'card_issuer': random.choice(['Chase', 'Bank of America', 'Wells Fargo', 'Citi', 'Capital One']),
                'card_brand': random.choice(['Visa', 'Mastercard', 'American Express', 'Discover'])
            }
            self.add_card_profile_to_state(card_profile)
        
        # Payment attributes
        payment_type = random.choice(self.payment_config['payment_types'])
        
        # Payment status based on probabilities (cache lists to avoid repeated list() calls)
        if not hasattr(self, '_cached_payment_statuses'):
            self._cached_payment_statuses = list(self.payment_config['payment_statuses'].keys())
            self._cached_payment_status_probs = list(self.payment_config['payment_statuses'].values())
        
        status = np.random.choice(
            self._cached_payment_statuses,
            p=self._cached_payment_status_probs
        )
        
        # Transactional cost (simplified)
        card_type = random.choice(['credit', 'debit'])
        card_brand = random.choice(['Visa', 'Mastercard', 'American Express', 'Discover'])
        cost_key = (card_type, card_brand)
        if cost_key in self.transactional_costs:
            cost_rate = self.transactional_costs[cost_key]
        else:
            cost_rate = 0.015  # Default 1.5%
        
        # Geographic coordinates
        lat = round(random.uniform(self.geo_config['us_bounds']['lat_min'], self.geo_config['us_bounds']['lat_max']), 6)
        lng = round(random.uniform(self.geo_config['us_bounds']['lng_min'], self.geo_config['us_bounds']['lng_max']), 6)
        
        # Generate fast transaction ID using counter
        self.transaction_counter += 1
        
        # Debug: Check if we're getting stuck here (less frequent)
        if self.transaction_counter % 50000 == 0:
            print(f"   Generated {self.transaction_counter} transactions so far...")
        
        transaction = {
            'payment_id': f"TXN{self.transaction_counter:010d}",
            'payment_timestamp': target_time.isoformat(),
            'payment_lat': lat,
            'payment_lng': lng,
            'payment_amount': amount,
            'payment_type': payment_type,
            'terminal_id': f"T{random.randint(1000, 9999)}" if payment_type == 'card_present' else None,
            'card_type': card_type,
            'card_issuer': random.choice(['Chase', 'Bank of America', 'Wells Fargo', 'Citi', 'Capital One']),
            'card_brand': card_brand,
            'card_profile_id': card_profile_id,
            'card_bin': card_bin,
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
        
        # Get active merchants from cache (much faster)
        active_merchants = [
            merchant for merchant in self._current_merchants_cache.values()
            if merchant['status'] == 'active'
        ]
        
        print(f"   Found {len(active_merchants)} active merchants")
        
        # Generate repeated card transactions first (simplified approach)
        repeated_transactions = 0
        if self.state['card_profiles']:
            repeat_percentage = random.uniform(0.05, 0.15)  # 5-15% of transactions use repeated cards
            num_repeated_cards = int(len(active_merchants) * repeat_percentage)
            
            if num_repeated_cards > 0:
                repeated_cards = random.sample(list(self.state['card_profiles'].values()), 
                                            min(num_repeated_cards, len(self.state['card_profiles'])))
                
                for card_profile in repeated_cards:
                    # Pick a random active merchant for this repeated card
                    merchant = random.choice(active_merchants)
                    if self.is_merchant_active_on_date(merchant, target_date):
                        # Generate exactly 1 transaction for this repeated card
                        transaction_time = datetime.combine(target_date, datetime.min.time()) + timedelta(
                            hours=random.randint(8, 22), 
                            minutes=random.randint(0, 59), 
                            seconds=random.randint(0, 59)
                        )
                        
                        transaction = self.generate_transaction_with_card_profile(merchant, target_date, transaction_time)
                        # Override with stored card details
                        transaction['card_profile_id'] = card_profile['card_profile_id']
                        transaction['card_bin'] = card_profile['card_bin']
                        transaction['card_type'] = card_profile['card_type']
                        transaction['card_issuer'] = card_profile['card_issuer']
                        transaction['card_brand'] = card_profile['card_brand']
                        
                        transactions.append(transaction)
                        repeated_transactions += 1
        
        if repeated_transactions > 0:
            print(f"   Generated {repeated_transactions} repeated card transactions")
        
        # Generate transactions for each merchant
        for i, merchant in enumerate(active_merchants):
            if i % 200 == 0:  # Progress every 200 merchants (less verbose)
                print(f"   Processing merchant {i+1}/{len(active_merchants)}...")
            if not self.is_merchant_active_on_date(merchant, target_date):
                continue
            
            size = merchant['size_category']
            config = self.size_configs[size]
            
            # Number of transactions for this merchant today
            tx_count = random.randint(*config['daily_tx_range'])
            
            # Seasonal adjustment
            if target_date.month in self.seasonal_config['holiday_months']:
                tx_count = int(tx_count * self.seasonal_config['holiday_multiplier'])
            elif target_date.month in self.seasonal_config['summer_months']:
                tx_count = int(tx_count * self.seasonal_config['summer_multiplier'])
            
            # Pre-generate time components for all transactions to reduce random calls
            if tx_count > 0:
                hours = [random.randint(8, 22) for _ in range(tx_count)]
                minutes = [random.randint(0, 59) for _ in range(tx_count)]
                seconds = [random.randint(0, 59) for _ in range(tx_count)]
                
                # Generate transactions throughout the day
                for j in range(tx_count):
                    transaction_time = datetime.combine(target_date, datetime.min.time()) + timedelta(
                        hours=hours[j], minutes=minutes[j], seconds=seconds[j]
                    )
                    
                    transaction = self.generate_transaction_with_card_profile(merchant, target_date, transaction_time)
                    transactions.append(transaction)
        
        # Update last transaction date for all merchants that had transactions (once per day, not per transaction)
        updated_merchants = set()
        for transaction in transactions:
            merchant_id = transaction['merchant_id']
            if merchant_id not in updated_merchants:
                current = self._current_merchants_cache.get(merchant_id)
                if current:
                    # Update in place without creating a new version
                    current['last_transaction_date'] = target_date
                    updated_merchants.add(merchant_id)
        
        self.state['total_transactions'] += len(transactions)
        return transactions

    def confirm_generation(self, mode: str, start_date: date, end_date: date, auto_confirm: bool = False) -> bool:
        """Ask for confirmation before generation"""
        print(f"\n🔄 {mode.title()} data generation")
        print(f"📅 Date range: {start_date} to {end_date}")
        print(f"📁 Output directory: {self.output_dir}")
        print(f"📄 Files to generate:")
        print(f"   - merchants_{start_date}_{end_date}.csv")
        print(f"   - transactions_{start_date}_{end_date}.csv")
        
        if auto_confirm:
            print("✅ Auto-confirmed for testing")
            return True
        
        response = input("\n❓ Proceed with generation? (y/N): ").strip().lower()
        return response in ['y', 'yes']

    def generate_initial_data(self, start_date: date, end_date: date, auto_confirm: bool = False):
        """Generate initial dataset for date range"""
        if self.debug:
            print("🔄 Starting generate_initial_data method...")
        
        if not start_date or not end_date:
            raise ValueError("Initial mode requires start_date and end_date")
        
        if self.debug:
            print("🔄 Calling confirm_generation...")
        
        if not self.confirm_generation("Initial", start_date, end_date, auto_confirm):
            print("❌ Generation cancelled by user")
            return
        
        if self.debug:
            print("🔄 confirm_generation returned: True")
        
        print(f"🚀 Generating initial data from {start_date} to {end_date}")
        
        if self.debug:
            print(f"🐛 DEBUG: Date range: {start_date} to {end_date}")
            print(f"🐛 DEBUG: Total days: {(end_date - start_date).days + 1}")
        
        # Initialize state
        if self.debug:
            print("🔄 Initializing state...")
        self.initialize_state()
        if self.debug:
            print("✅ State initialized")
        
        # Generate initial merchants
        print("👥 Generating initial merchant base...")
        initial_merchants = self.generate_initial_merchants(start_date)
        print(f"✅ Generated {len(initial_merchants)} initial merchants")
        
        # Generate data for each month in chunks to avoid memory issues
        current_date = start_date.replace(day=1)  # Start of first month
        total_transactions = 0
        transaction_files = []
        
        while current_date <= end_date:
            month_end = (current_date + timedelta(days=32)).replace(day=1) - timedelta(days=1)
            month_end = min(month_end, end_date)
            
            print(f"📅 Processing {current_date.strftime('%Y-%m')}...")
            
            # Process monthly merchant changes (growth/churn) on first day of month
            if current_date.day == 1:
                print("   🔄 Processing monthly merchant changes...")
                new_merchants, churned_merchants = self.process_merchant_changes(current_date)
                print(f"   ✅ Added {len(new_merchants)} new merchants, churned {len(churned_merchants)} merchants")
            
            # Generate daily transactions for this month - stream to disk immediately
            month_file = self.output_dir / f"transactions_{current_date.strftime('%Y%m')}.csv"
            month_transaction_count = 0
            daily_date = current_date
            days_in_month = (month_end - current_date).days + 1
            
            print(f"   📅 Processing {days_in_month} days in {current_date.strftime('%Y-%m')}...")
            
            # Initialize monthly file with header
            with open(month_file, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=self.transaction_columns)
                writer.writeheader()
            
            day_count = 0
            while daily_date <= month_end:
                day_count += 1
                # Generate merchant updates for this day
                print(f"   🔄 Day {day_count}/{days_in_month}: Generating merchant updates for {daily_date}...")
                merchant_updates = self.generate_merchant_updates(daily_date, daily_date)
                if merchant_updates:
                    print(f"   ✅ Generated {len(merchant_updates)} merchant updates")
                else:
                    print(f"   ✅ No merchant updates needed for {daily_date}")
                
                # Generate daily transactions and write immediately to monthly file
                print(f"   🔄 Day {day_count}/{days_in_month}: Generating daily transactions for {daily_date}...")
                daily_transactions = self.generate_daily_transactions(daily_date)
                
                # Append to monthly file immediately
                with open(month_file, 'a', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=self.transaction_columns)
                    writer.writerows(daily_transactions)
                
                month_transaction_count += len(daily_transactions)
                total_transactions += len(daily_transactions)
                print(f"   ✅ Day {day_count}/{days_in_month}: Generated and saved {len(daily_transactions)} transactions (Total: {total_transactions:,})")
                
                daily_date += timedelta(days=1)
            
            transaction_files.append(month_file)
            print(f"   💾 Monthly file complete: {month_transaction_count} transactions in {month_file}")
            
            # Move to next month
            current_date = (current_date + timedelta(days=32)).replace(day=1)
        
        # Combine all monthly files into a single file using streaming
        print("🔄 Combining monthly transaction files...")
        transactions_file = self.output_dir / f"transactions_{start_date}_{end_date}.csv"
        self.combine_monthly_files(transaction_files, transactions_file)
        
        # Clean up monthly files
        for f in transaction_files:
            f.unlink()
        
        print(f"✅ Saved {total_transactions} transactions to {transactions_file}")
        
        # Collect all merchant records for the period
        print("🔄 Collecting merchant records...")
        all_merchants = self.get_merchants_for_period(start_date, end_date)
        print(f"✅ Collected {len(all_merchants)} merchant records")
        
        # Save merchant file
        print("🔄 Saving merchant file...")
        merchants_file = self.output_dir / f"merchants_{start_date}_{end_date}.csv"
        self.save_merchants_to_csv(all_merchants, merchants_file)
        print(f"✅ Saved {len(all_merchants)} merchants to {merchants_file}")
        
        # Update state
        print("🔄 Updating state...")
        self.state['last_generated_date'] = end_date
        self.state['transaction_counter'] = self.transaction_counter
        self.save_state()
        print("✅ State updated")
        
        print(f"🎉 Initial data generation complete!")
        print(f"   📊 Total merchants: {len(all_merchants)}")
        print(f"   💳 Total transactions: {total_transactions}")

    def generate_initial_merchants(self, start_date: date, count: int = None) -> List[Dict]:
        """Generate initial merchant base with reasonable cap"""
        if count is None:
            count = min(self.config['initial_merchants'], 500)  # Cap at 500 merchants
        
        print(f"🔄 Generating {count} initial merchants...")
        merchants = []
        for i in range(count):
            if i % 100 == 0:  # Progress indicator every 100 merchants
                print(f"   Progress: {i}/{count} merchants generated...")
            merchant_id = self.generate_merchant_id()
            merchant = self.generate_merchant(merchant_id, start_date)
            merchants.append(merchant)
            self.add_merchant_to_state(merchant)
        
        print(f"✅ Completed generating {count} merchants")
        return merchants

    def generate_incremental_data(self, start_date: date, end_date: date, auto_confirm: bool = False):
        """Generate incremental data for a date range"""
        if not start_date or not end_date:
            raise ValueError("Incremental mode requires start_date and end_date")
        
        if not self.confirm_generation("Incremental", start_date, end_date, auto_confirm):
            print("❌ Generation cancelled by user")
            return
        
        print(f"🔄 Generating incremental data from {start_date} to {end_date}")
        
        # Generate data for each day in the range
        current_date = start_date
        total_transactions = 0
        all_transactions = []
        all_merchants = set()  # Use set to track unique merchants
        
        while current_date <= end_date:
            print(f"📅 Processing {current_date}...")
            
            # Check if it's the first day of month (merchant changes)
            if current_date.day == 1:
                print("   🔄 Processing monthly merchant changes...")
                new_merchants, churned_merchants = self.process_merchant_changes(current_date)
                print(f"   ✅ Added {len(new_merchants)} new merchants, churned {len(churned_merchants)} merchants")
            
            # Generate merchant updates for this day
            print(f"   🔄 Generating merchant updates for {current_date}...")
            merchant_updates = self.generate_merchant_updates(current_date, current_date)
            if merchant_updates:
                print(f"   ✅ Generated {len(merchant_updates)} merchant updates")
            else:
                print(f"   ✅ No merchant updates needed for {current_date}")
            
            # Generate transactions for the day
            print(f"   🔄 Generating daily transactions for {current_date}...")
            daily_transactions = self.generate_daily_transactions(current_date)
            all_transactions.extend(daily_transactions)
            total_transactions += len(daily_transactions)
            print(f"   ✅ Generated {len(daily_transactions)} transactions (Total: {total_transactions:,})")
            
            # Track merchants that had activity
            for transaction in daily_transactions:
                all_merchants.add(transaction['merchant_id'])
            
            current_date += timedelta(days=1)
        
        # Collect merchant records for the period
        print("🔄 Collecting merchant records...")
        merchants = self.get_merchants_for_period(start_date, end_date)
        print(f"✅ Collected {len(merchants)} merchant records")
        
        # Save files
        print("🔄 Saving data files...")
        self.save_data_files(start_date, end_date, merchants, all_transactions)
        print("✅ Data files saved")
        
        # Update state
        print("🔄 Updating state...")
        self.state['last_generated_date'] = end_date
        self.state['transaction_counter'] = self.transaction_counter
        self.save_state()
        print("✅ State updated")
        
        print(f"🎉 Incremental data generation complete!")
        print(f"   📊 Merchants: {len(merchants)}")
        print(f"   💳 Transactions: {total_transactions}")
        print(f"   📅 Date range: {start_date} to {end_date}")

    def get_merchants_for_period(self, start_date: date, end_date: date) -> List[Dict]:
        """Get all merchant records for a period (including historical versions)"""
        merchants = []
        
        # Get all versions of each merchant that were effective during this period
        for merchant_id, history in self.state['merchants'].items():
            for merchant_version in history:
                # Check if this version was effective during the period
                effective_date = merchant_version['effective_date']
                if isinstance(effective_date, str):
                    effective_date = datetime.strptime(effective_date, '%Y-%m-%d').date()
                
                # Include if effective date is within the period
                if start_date <= effective_date <= end_date:
                    merchants.append(merchant_version)
        
        return merchants

    def save_data_files(self, start_date: date, end_date: date, merchants: List[Dict], transactions: List[Dict]):
        """Save merchant and transaction data to CSV files"""
        # Generate file names
        date_suffix = f"{start_date}_{end_date}"
        merchants_file = self.output_dir / f"merchants_{date_suffix}.csv"
        transactions_file = self.output_dir / f"transactions_{date_suffix}.csv"
        
        # Save merchants
        if merchants:
            merchants_df = pd.DataFrame(merchants)
            merchants_df.to_csv(merchants_file, index=False)
            print(f"✅ Saved {len(merchants)} merchants to {merchants_file}")
        else:
            # Create empty file with headers
            empty_df = pd.DataFrame(columns=[
                'merchant_id', 'merchant_name', 'industry', 'address', 'city', 'state',
                'zip_code', 'phone', 'email', 'mdr_rate', 'size_category', 'creation_date',
                'effective_date', 'status', 'last_transaction_date', 'version'
            ])
            empty_df.to_csv(merchants_file, index=False)
            print(f"✅ Created empty merchants file: {merchants_file}")
        
        # Save transactions
        if transactions:
            transactions_df = pd.DataFrame(transactions)
            transactions_df.to_csv(transactions_file, index=False)
            print(f"✅ Saved {len(transactions)} transactions to {transactions_file}")
        else:
            # Create empty file with headers
            empty_df = pd.DataFrame(columns=[
                'payment_id', 'payment_timestamp', 'payment_lat', 'payment_lng',
                'payment_amount', 'payment_type', 'terminal_id', 'card_type',
                'card_issuer', 'card_brand', 'card_profile_id', 'card_bin',
                'payment_status', 'merchant_id', 'transactional_cost_rate',
                'transactional_cost_amount', 'mdr_amount', 'net_profit'
            ])
            empty_df.to_csv(transactions_file, index=False)
            print(f"✅ Created empty transactions file: {transactions_file}")
    
    def combine_monthly_files(self, monthly_files: List[Path], output_file: Path):
        """Combine monthly transaction files using streaming to avoid memory issues"""
        with open(output_file, 'w', newline='') as outfile:
            for i, monthly_file in enumerate(monthly_files):
                with open(monthly_file, 'r') as infile:
                    if i == 0:
                        # First file: copy header and all data
                        outfile.write(infile.read())
                    else:
                        # Subsequent files: skip header, copy data only
                        next(infile)  # Skip header
                        outfile.write(infile.read())
    
    def save_merchants_to_csv(self, merchants: List[Dict], file_path: Path):
        """Save merchants to CSV file"""
        if merchants:
            # Ensure all merchants have the required columns
            for merchant in merchants:
                if 'change_type' not in merchant:
                    merchant['change_type'] = None
                if 'churn_date' not in merchant:
                    merchant['churn_date'] = None
                # Convert None values to empty string for CSV compatibility
                if merchant['change_type'] is None:
                    merchant['change_type'] = ''
                if merchant['churn_date'] is None:
                    merchant['churn_date'] = ''
            
            merchants_df = pd.DataFrame(merchants)
            merchants_df.to_csv(file_path, index=False, na_rep='')
        else:
            # Create empty file with headers
            empty_df = pd.DataFrame(columns=[
                'merchant_id', 'merchant_name', 'industry', 'address', 'city', 'state',
                'zip_code', 'phone', 'email', 'mdr_rate', 'size_category', 'creation_date',
                'effective_date', 'status', 'last_transaction_date', 'version', 'change_type', 'churn_date'
            ])
            empty_df.to_csv(file_path, index=False)
    
    def save_transactions_to_csv(self, transactions: List[Dict], file_path: Path):
        """Save transactions to CSV file"""
        if transactions:
            transactions_df = pd.DataFrame(transactions)
            transactions_df.to_csv(file_path, index=False)
        else:
            # Create empty file with headers
            empty_df = pd.DataFrame(columns=[
                'payment_id', 'payment_timestamp', 'payment_lat', 'payment_lng',
                'payment_amount', 'payment_type', 'terminal_id', 'card_type',
                'card_issuer', 'card_brand', 'card_profile_id', 'card_bin',
                'payment_status', 'merchant_id', 'transactional_cost_rate',
                'transactional_cost_amount', 'mdr_amount', 'net_profit'
            ])
            empty_df.to_csv(file_path, index=False)


def main():
    parser = argparse.ArgumentParser(description='Generate payments aggregator raw data V2')
    parser.add_argument('--initial', action='store_true', help='Generate initial dataset')
    parser.add_argument('--incremental', action='store_true', help='Generate incremental data')
    parser.add_argument('--start-date', type=str, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='End date (YYYY-MM-DD)')
    parser.add_argument('--date', type=str, help='Specific date for incremental (YYYY-MM-DD)')
    parser.add_argument('--output-dir', type=str, default='./raw_data', help='Output directory')
    parser.add_argument('-f', '--force', action='store_true', help='Skip confirmation prompt')
    parser.add_argument('--debug', action='store_true', help='Enable debug output')
    
    args = parser.parse_args()
    
    # Initialize generator
    generator = PaymentsDataGeneratorV2(output_dir=args.output_dir, debug=args.debug)
    
    if not args.start_date or not args.end_date:
        print("❌ Initial generation requires --start-date and --end-date")
        return
    
    start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
    end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()

    if args.initial:
        
        generator.generate_initial_data(start_date, end_date, auto_confirm=args.force)
    
    elif args.incremental:
        
        generator.generate_incremental_data(start_date, end_date, auto_confirm=args.force)
    
    else:
        print("❌ Please specify --initial or --incremental")


if __name__ == "__main__":
    main()
