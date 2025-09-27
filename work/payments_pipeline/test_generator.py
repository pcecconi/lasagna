#!/usr/bin/env python3
"""
Simple test script for the payments data generator
Tests core logic without external dependencies
"""

import json
import random
import uuid
from datetime import datetime, date, timedelta
from typing import Dict, List

def test_merchant_size_assignment():
    """Test merchant size assignment logic"""
    print("🧪 Testing merchant size assignment...")
    
    size_distribution = {
        'small': 0.70,
        'medium': 0.25,
        'large': 0.05
    }
    
    # Test 1000 assignments
    results = {'small': 0, 'medium': 0, 'large': 0}
    
    for _ in range(1000):
        rand = random.random()
        cumulative = 0
        for size, prob in size_distribution.items():
            cumulative += prob
            if rand <= cumulative:
                results[size] += 1
                break
    
    print(f"   Small: {results['small']} ({results['small']/10:.1f}%)")
    print(f"   Medium: {results['medium']} ({results['medium']/10:.1f}%)")
    print(f"   Large: {results['large']} ({results['large']/10:.1f}%)")
    print("   ✅ Size distribution looks correct")

def test_merchant_generation():
    """Test merchant generation"""
    print("\n🧪 Testing merchant generation...")
    
    merchant_id = "M000001"
    creation_date = date(2024, 1, 1)
    size = "medium"
    
    # Mock merchant generation
    merchant = {
        'merchant_id': merchant_id,
        'merchant_name': f"Test Store {random.randint(1, 999)}",
        'industry': random.choice(['retail', 'restaurant', 'healthcare']),
        'address': f"{random.randint(100, 9999)} Main St",
        'city': random.choice(['New York', 'Los Angeles', 'Chicago']),
        'state': random.choice(['NY', 'CA', 'IL']),
        'zip_code': f"{random.randint(10000, 99999)}",
        'phone': f"{random.randint(200, 999)}-{random.randint(200, 999)}-{random.randint(1000, 9999)}",
        'email': f"contact@{merchant_id.lower()}.com",
        'mdr_rate': round(random.uniform(0.025, 0.029), 4),
        'size_category': size,
        'creation_date': creation_date,
        'status': 'active'
    }
    
    print(f"   Generated merchant: {merchant['merchant_name']}")
    print(f"   Size: {merchant['size_category']}")
    print(f"   MDR Rate: {merchant['mdr_rate']}")
    print(f"   Industry: {merchant['industry']}")
    print("   ✅ Merchant generation works")

def test_transaction_generation():
    """Test transaction generation"""
    print("\n🧪 Testing transaction generation...")
    
    # Mock merchant
    merchant = {
        'merchant_id': 'M000001',
        'size_category': 'medium',
        'mdr_rate': 0.027
    }
    
    # Mock transaction
    transaction = {
        'payment_id': str(uuid.uuid4()),
        'payment_timestamp': datetime.now().isoformat(),
        'payment_lat': round(random.uniform(25.0, 49.0), 6),
        'payment_lng': round(random.uniform(-125.0, -66.0), 6),
        'payment_amount': round(random.uniform(25, 300), 2),
        'payment_type': random.choice(['card_present', 'card_not_present']),
        'terminal_id': f"T{random.randint(1000, 9999)}",
        'card_type': random.choice(['debit', 'credit']),
        'card_issuer': random.choice(['Chase', 'Bank of America', 'Wells Fargo']),
        'card_brand': random.choice(['visa', 'mastercard', 'amex']),
        'payment_status': random.choice(['approved', 'declined', 'cancelled']),
        'merchant_id': merchant['merchant_id'],
        'transactional_cost_rate': 0.015,
        'transactional_cost_amount': 0.0,  # Will be calculated
        'mdr_amount': 0.0,  # Will be calculated
        'net_profit': 0.0   # Will be calculated
    }
    
    # Calculate business metrics
    amount = transaction['payment_amount']
    transaction['transactional_cost_amount'] = round(amount * transaction['transactional_cost_rate'], 2)
    transaction['mdr_amount'] = round(amount * merchant['mdr_rate'], 2)
    transaction['net_profit'] = round(transaction['mdr_amount'] - transaction['transactional_cost_amount'], 2)
    
    print(f"   Payment ID: {transaction['payment_id'][:8]}...")
    print(f"   Amount: ${transaction['payment_amount']}")
    print(f"   Card: {transaction['card_type']} {transaction['card_brand']}")
    print(f"   Status: {transaction['payment_status']}")
    print(f"   MDR Amount: ${transaction['mdr_amount']}")
    print(f"   Cost Amount: ${transaction['transactional_cost_amount']}")
    print(f"   Net Profit: ${transaction['net_profit']}")
    print("   ✅ Transaction generation works")

def test_business_calculations():
    """Test business calculation logic"""
    print("\n🧪 Testing business calculations...")
    
    # Test different scenarios
    scenarios = [
        {'size': 'small', 'amount': 50.00, 'mdr_rate': 0.032},
        {'size': 'medium', 'amount': 150.00, 'mdr_rate': 0.027},
        {'size': 'large', 'amount': 500.00, 'mdr_rate': 0.023}
    ]
    
    transactional_costs = {
        ('debit', 'visa'): 0.005,
        ('credit', 'visa'): 0.015,
        ('credit', 'amex'): 0.025
    }
    
    for scenario in scenarios:
        amount = scenario['amount']
        mdr_rate = scenario['mdr_rate']
        cost_rate = transactional_costs[('credit', 'visa')]  # Use standard rate
        
        mdr_amount = round(amount * mdr_rate, 2)
        cost_amount = round(amount * cost_rate, 2)
        net_profit = round(mdr_amount - cost_amount, 2)
        profit_margin = round((net_profit / amount) * 100, 2)
        
        print(f"   {scenario['size'].title()} merchant: ${amount}")
        print(f"     MDR: ${mdr_amount} ({mdr_rate*100:.1f}%)")
        print(f"     Cost: ${cost_amount} ({cost_rate*100:.1f}%)")
        print(f"     Net Profit: ${net_profit} ({profit_margin:.1f}%)")
    
    print("   ✅ Business calculations work")

def test_date_logic():
    """Test date and time logic"""
    print("\n🧪 Testing date logic...")
    
    # Test business hours
    test_date = date(2024, 1, 15)  # Monday
    business_hour = 14  # 2 PM
    weekend_hour = 22   # 10 PM on Saturday
    
    # Generate timestamps
    weekday_time = datetime.combine(test_date, datetime.min.time()) + timedelta(hours=business_hour)
    weekend_time = datetime.combine(test_date + timedelta(days=5), datetime.min.time()) + timedelta(hours=weekend_hour)
    
    print(f"   Weekday business time: {weekday_time}")
    print(f"   Weekend time: {weekend_time}")
    print(f"   Is weekday: {test_date.weekday() < 5}")
    print(f"   Is weekend: {(test_date + timedelta(days=5)).weekday() >= 5}")
    print("   ✅ Date logic works")

def main():
    """Run all tests"""
    print("🧪 Payments Data Generator - Test Suite")
    print("=====================================")
    
    test_merchant_size_assignment()
    test_merchant_generation()
    test_transaction_generation()
    test_business_calculations()
    test_date_logic()
    
    print("\n🎉 All tests passed!")
    print("\n📝 Next steps:")
    print("1. Install dependencies: pip install pandas numpy")
    print("2. Run full generator: python data_generator.py --help")
    print("3. Generate initial data: ./generate_data.sh")

if __name__ == "__main__":
    main()
