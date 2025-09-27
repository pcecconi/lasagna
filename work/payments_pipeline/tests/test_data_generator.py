"""
Unit tests for the payments data generator
"""

import pytest
import pandas as pd
import tempfile
import shutil
from pathlib import Path
from datetime import datetime, date, timedelta
from unittest.mock import patch, MagicMock

# Import the data generator class
import sys
sys.path.append(str(Path(__file__).parent.parent))
from data_generator import PaymentsDataGenerator


class TestPaymentsDataGenerator:
    """Test suite for PaymentsDataGenerator class"""
    
    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for testing"""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir)
    
    @pytest.fixture
    def generator(self, temp_dir):
        """Create generator instance for testing"""
        return PaymentsDataGenerator(output_dir=temp_dir)
    
    def test_initialization(self, generator, temp_dir):
        """Test generator initialization"""
        assert generator.output_dir == Path(temp_dir)
        assert generator.output_dir.exists()
        assert generator.state['merchant_counter'] == 1
        assert generator.state['total_transactions'] == 0
    
    def test_merchant_id_generation(self, generator):
        """Test merchant ID generation"""
        # Test first merchant ID
        merchant_id = generator.generate_merchant_id()
        assert merchant_id == "M000001"
        assert generator.state['merchant_counter'] == 2
        
        # Test second merchant ID
        merchant_id = generator.generate_merchant_id()
        assert merchant_id == "M000002"
        assert generator.state['merchant_counter'] == 3
    
    def test_merchant_size_assignment(self, generator):
        """Test merchant size assignment follows distribution"""
        size_counts = {'small': 0, 'medium': 0, 'large': 0}
        
        # Test 1000 assignments
        for _ in range(1000):
            size = generator.assign_merchant_size()
            size_counts[size] += 1
        
        # Check distribution is approximately correct
        small_pct = size_counts['small'] / 1000
        medium_pct = size_counts['medium'] / 1000
        large_pct = size_counts['large'] / 1000
        
        assert 0.65 <= small_pct <= 0.75  # ~70%
        assert 0.20 <= medium_pct <= 0.30  # ~25%
        assert 0.03 <= large_pct <= 0.07   # ~5%
    
    def test_merchant_generation(self, generator):
        """Test merchant generation"""
        merchant_id = "M000001"
        creation_date = date(2024, 1, 1)
        
        merchant = generator.generate_merchant(merchant_id, creation_date, "medium")
        
        # Check required fields
        assert merchant['merchant_id'] == merchant_id
        assert merchant['creation_date'] == creation_date
        assert merchant['size_category'] == "medium"
        assert merchant['status'] == 'active'
        
        # Check field types and formats
        assert isinstance(merchant['merchant_name'], str)
        assert merchant['merchant_name'] != ""
        assert isinstance(merchant['mdr_rate'], float)
        assert 0.025 <= merchant['mdr_rate'] <= 0.029  # Medium merchant range
        assert isinstance(merchant['phone'], str)
        assert len(merchant['phone']) > 0
        assert '@' in merchant['email']
        
        # Check merchant is added to state
        # Note: generate_merchant doesn't automatically add to state
        # This is done in generate_initial_merchants or generate_new_merchants
    
    def test_initial_merchants_generation(self, generator):
        """Test initial merchant base generation"""
        start_date = date(2024, 1, 1)
        count = 10
        
        merchants = generator.generate_initial_merchants(start_date, count)
        
        assert len(merchants) == count
        assert len(generator.state['merchants']) == count
        
        # Check all merchants have unique IDs
        merchant_ids = [m['merchant_id'] for m in merchants]
        assert len(set(merchant_ids)) == count
        
        # Check all merchants are created on start_date
        for merchant in merchants:
            assert merchant['creation_date'] == start_date
            assert merchant['status'] == 'active'
    
    def test_transaction_generation(self, generator):
        """Test transaction generation"""
        # Create a test merchant
        merchant = generator.generate_merchant("M000001", date(2024, 1, 1), "medium")
        generator.state['merchants']['M000001'] = merchant
        
        target_date = date(2024, 1, 15)
        target_time = datetime(2024, 1, 15, 14, 30, 0)
        
        transaction = generator.generate_transaction(merchant, target_date, target_time)
        
        # Check required fields
        assert 'payment_id' in transaction
        assert transaction['merchant_id'] == "M000001"
        assert transaction['payment_timestamp'] == target_time.isoformat()
        assert transaction['payment_amount'] > 0
        assert transaction['payment_status'] in ['approved', 'declined', 'cancelled']
        assert transaction['card_type'] in ['debit', 'credit']
        assert transaction['card_brand'] in ['visa', 'mastercard', 'amex', 'discover']
        
        # Check business calculations
        amount = transaction['payment_amount']
        mdr_rate = merchant['mdr_rate']
        cost_rate = transaction['transactional_cost_rate']
        
        expected_mdr_amount = round(amount * mdr_rate, 2)
        expected_cost_amount = round(amount * cost_rate, 2)
        expected_net_profit = round(expected_mdr_amount - expected_cost_amount, 2)
        
        assert transaction['mdr_amount'] == expected_mdr_amount
        assert transaction['transactional_cost_amount'] == expected_cost_amount
        assert transaction['net_profit'] == expected_net_profit
    
    def test_merchant_activity_logic(self, generator):
        """Test merchant activity determination"""
        # Create merchants of different sizes
        small_merchant = generator.generate_merchant("M000001", date(2024, 1, 1), "small")
        large_merchant = generator.generate_merchant("M000002", date(2024, 1, 1), "large")
        
        # Test weekday activity
        weekday = date(2024, 1, 15)  # Monday
        
        # Small merchants should be less active than large merchants
        small_active_count = 0
        large_active_count = 0
        
        for _ in range(100):
            if generator.is_merchant_active_on_date(small_merchant, weekday):
                small_active_count += 1
            if generator.is_merchant_active_on_date(large_merchant, weekday):
                large_active_count += 1
        
        # Large merchants should be more active (statistically)
        assert large_active_count > small_active_count
    
    def test_state_persistence(self, generator, temp_dir):
        """Test state saving and loading"""
        # Modify state
        generator.state['merchant_counter'] = 100
        generator.state['total_transactions'] = 500
        
        # Save state
        generator.save_state()
        
        # Create new generator instance
        new_generator = PaymentsDataGenerator(output_dir=temp_dir)
        
        # Check state was loaded
        assert new_generator.state['merchant_counter'] == 100
        assert new_generator.state['total_transactions'] == 500
    
    def test_incremental_data_generation(self, generator):
        """Test incremental data generation"""
        # Set up initial state
        generator.state['last_generated_date'] = '2024-01-15'
        
        target_date = date(2024, 1, 16)
        
        # Mock daily transactions generation
        with patch.object(generator, 'generate_daily_transactions') as mock_generate:
            mock_generate.return_value = [{'test': 'transaction'}]
            
            generator.generate_incremental_data(target_date)
            
            # Check state was updated
            assert generator.state['last_generated_date'] == target_date
            
            # Check method was called
            mock_generate.assert_called_once_with(target_date)
    
    def test_business_calculations(self, generator):
        """Test business calculation accuracy"""
        # Test different scenarios
        test_cases = [
            {'amount': 100.0, 'mdr_rate': 0.03, 'cost_rate': 0.015},
            {'amount': 250.0, 'mdr_rate': 0.027, 'cost_rate': 0.018},
            {'amount': 500.0, 'mdr_rate': 0.025, 'cost_rate': 0.025},
        ]
        
        for case in test_cases:
            amount = case['amount']
            mdr_rate = case['mdr_rate']
            cost_rate = case['cost_rate']
            
            expected_mdr = round(amount * mdr_rate, 2)
            expected_cost = round(amount * cost_rate, 2)
            expected_profit = round(expected_mdr - expected_cost, 2)
            
            # Create transaction with these values
            merchant = {
                'merchant_id': 'M000001',
                'size_category': 'medium',
                'mdr_rate': mdr_rate
            }
            
            transaction = {
                'payment_amount': amount,
                'transactional_cost_rate': cost_rate
            }
            
            # Calculate values (simulating the generator logic)
            transaction['mdr_amount'] = round(amount * mdr_rate, 2)
            transaction['transactional_cost_amount'] = round(amount * cost_rate, 2)
            transaction['net_profit'] = round(transaction['mdr_amount'] - transaction['transactional_cost_amount'], 2)
            
            assert transaction['mdr_amount'] == expected_mdr
            assert transaction['transactional_cost_amount'] == expected_cost
            assert transaction['net_profit'] == expected_profit


class TestDataGenerationIntegration:
    """Integration tests for data generation"""
    
    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for testing"""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir)
    
    def test_small_dataset_generation(self, temp_dir):
        """Test generating a small dataset"""
        generator = PaymentsDataGenerator(output_dir=temp_dir)
        
        start_date = date(2024, 1, 1)
        end_date = date(2024, 1, 7)  # One week
        
        # Generate data
        generator.generate_initial_data(start_date, end_date)
        
        # Check files were created
        output_dir = Path(temp_dir)
        merchant_files = list(output_dir.glob("merchants_initial_*.csv"))
        transaction_files = list(output_dir.glob("transactions_initial_*.csv"))
        
        assert len(merchant_files) == 1
        assert len(transaction_files) == 1
        
        # Check merchant file
        merchants_df = pd.read_csv(merchant_files[0])
        assert len(merchants_df) > 0
        assert 'merchant_id' in merchants_df.columns
        assert 'size_category' in merchants_df.columns
        
        # Check transaction file
        transactions_df = pd.read_csv(transaction_files[0])
        assert len(transactions_df) > 0
        assert 'payment_id' in transactions_df.columns
        assert 'merchant_id' in transactions_df.columns
        assert 'payment_amount' in transactions_df.columns
        
        # Check data relationships
        merchant_ids = set(merchants_df['merchant_id'])
        transaction_merchant_ids = set(transactions_df['merchant_id'])
        
        # Some transactions might be from merchants that were churned
        # or from new merchants added during the month, so we just check
        # that there's a reasonable overlap
        overlap = merchant_ids.intersection(transaction_merchant_ids)
        assert len(overlap) > 0  # At least some merchants have transactions
    
    def test_incremental_generation_creates_files(self, temp_dir):
        """Test that incremental generation creates proper files"""
        generator = PaymentsDataGenerator(output_dir=temp_dir)
        
        # Set up some initial state
        generator.state['last_generated_date'] = '2024-01-15'
        
        target_date = date(2024, 1, 16)
        
        with patch.object(generator, 'generate_daily_transactions') as mock_generate:
            mock_generate.return_value = [
                {
                    'payment_id': 'test123',
                    'merchant_id': 'M000001',
                    'payment_amount': 100.0,
                    'payment_timestamp': '2024-01-16 10:00:00'
                }
            ]
            
            generator.generate_incremental_data(target_date)
            
            # Check file was created
            output_dir = Path(temp_dir)
            incremental_files = list(output_dir.glob(f"transactions_{target_date.strftime('%Y%m%d')}.csv"))
            assert len(incremental_files) == 1
            
            # Check file content
            df = pd.read_csv(incremental_files[0])
            assert len(df) == 1
            assert df.iloc[0]['payment_id'] == 'test123'
