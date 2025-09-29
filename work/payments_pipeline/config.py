"""
Configuration file for Payments Aggregator Data Generator
"""

# Business Configuration
BUSINESS_CONFIG = {
    'initial_merchants': 1000,
    'monthly_growth_rate': 0.08,  # 8% monthly growth
    'monthly_churn_rate': 0.03,   # 3% monthly churn
    'daily_transaction_volume': 3000,
    'merchant_size_distribution': {
        'small': 0.70,    # 70% small merchants
        'medium': 0.25,   # 25% medium merchants
        'large': 0.05     # 5% large merchants
    }
}

# Merchant Size Configurations
MERCHANT_SIZE_CONFIGS = {
    'small': {
        'monthly_volume_range': (500, 10000),
        'avg_transaction': 25.00,
        'mdr_rate_range': (0.029, 0.035),
        'daily_tx_range': (5, 50),
        'active_days_per_month': 22,
        'churn_multiplier': 1.5,
        'growth_multiplier': 1.2,
        'industries': ['retail', 'restaurant', 'beauty', 'fitness', 'local_services']
    },
    'medium': {
        'monthly_volume_range': (10000, 100000),
        'avg_transaction': 75.00,
        'mdr_rate_range': (0.025, 0.029),
        'daily_tx_range': (20, 150),
        'active_days_per_month': 26,
        'churn_multiplier': 1.0,
        'growth_multiplier': 1.0,
        'industries': ['retail', 'restaurant', 'healthcare', 'education', 'automotive']
    },
    'large': {
        'monthly_volume_range': (100000, 1000000),
        'avg_transaction': 200.00,
        'mdr_rate_range': (0.020, 0.025),
        'daily_tx_range': (100, 500),
        'active_days_per_month': 28,
        'churn_multiplier': 0.3,
        'growth_multiplier': 0.8,
        'industries': ['retail', 'healthcare', 'manufacturing', 'logistics', 'technology']
    }
}

# Payment Configuration
PAYMENT_CONFIG = {
    'card_brands': ['visa', 'mastercard', 'amex', 'discover'],
    'card_types': ['debit', 'credit'],
    'card_issuers': [
        'Chase', 'Bank of America', 'Wells Fargo', 'Citi', 'Capital One',
        'American Express', 'Discover', 'US Bank', 'PNC', 'TD Bank'
    ],
    'payment_types': ['card_present', 'card_not_present'],
    'payment_statuses': {
        'approved': 0.85,
        'declined': 0.12,
        'cancelled': 0.03
    }
}

# Transactional Costs by Card Type/Brand Combination
TRANSACTIONAL_COSTS = {
    ('debit', 'visa'): 0.005,      # 0.5%
    ('debit', 'mastercard'): 0.006, # 0.6%
    ('credit', 'visa'): 0.015,     # 1.5%
    ('credit', 'mastercard'): 0.016, # 1.6%
    ('credit', 'amex'): 0.025,     # 2.5%
    ('credit', 'discover'): 0.018,  # 1.8%
    ('debit', 'amex'): 0.008,      # 0.8%
    ('debit', 'discover'): 0.007   # 0.7%
}

# Geographic Configuration
GEO_CONFIG = {
    'us_bounds': {
        'lat_min': 25.0,
        'lat_max': 49.0,
        'lng_min': -125.0,
        'lng_max': -66.0
    },
    'major_cities': [
        {'name': 'New York', 'state': 'NY', 'lat': 40.7128, 'lng': -74.0060},
        {'name': 'Los Angeles', 'state': 'CA', 'lat': 34.0522, 'lng': -118.2437},
        {'name': 'Chicago', 'state': 'IL', 'lat': 41.8781, 'lng': -87.6298},
        {'name': 'Houston', 'state': 'TX', 'lat': 29.7604, 'lng': -95.3698},
        {'name': 'Phoenix', 'state': 'AZ', 'lat': 33.4484, 'lng': -112.0740},
        {'name': 'Philadelphia', 'state': 'PA', 'lat': 39.9526, 'lng': -75.1652}
    ]
}

# Seasonal Patterns
SEASONAL_CONFIG = {
    'holiday_months': [11, 12],  # November, December
    'holiday_multiplier': 1.5,
    'summer_months': [6, 7],     # June, July
    'summer_multiplier': 1.2,
    'weekend_multiplier': 0.7
}

# Data Generation Settings
DATA_CONFIG = {
    'output_format': 'csv',
    'include_headers': True,
    'date_format': '%Y-%m-%d',
    'timestamp_format': '%Y-%m-%d %H:%M:%S',
    'decimal_places': 2
}

