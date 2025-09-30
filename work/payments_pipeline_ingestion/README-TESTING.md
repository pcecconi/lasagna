# Testing Guide for Payments Pipeline Ingestion

This document provides comprehensive information about testing the payments pipeline ingestion system.

## Test Structure

The test suite is organized into the following structure:

```
tests/
├── conftest.py                 # Pytest configuration and fixtures
├── unit/                       # Unit tests
│   ├── test_atomic_updates.py  # Tests for AtomicSilverUpdater
│   ├── test_data_quality.py    # Tests for DataQualityChecker
│   └── test_silver_ingestion.py # Tests for SilverIngestionJob
├── integration/                # Integration tests
│   └── test_silver_pipeline.py # End-to-end pipeline tests
└── __init__.py
```

## Test Categories

### Unit Tests
- **AtomicSilverUpdater**: Tests for atomic update functionality, SCD Type 2 logic, data validation
- **DataQualityChecker**: Tests for all data quality checks and validation logic
- **SilverIngestionJob**: Tests for pipeline orchestration and job management

### Integration Tests
- **Complete Pipeline Flow**: End-to-end testing from bronze to silver layer
- **Data Consistency**: Verification of data integrity across layers
- **Performance**: Testing with larger datasets
- **Error Handling**: Testing failure scenarios and recovery

## Running Tests

### Prerequisites

Install test dependencies:
```bash
pip install -r requirements-test.txt
```

### Basic Test Execution

Run all tests:
```bash
python scripts/run_tests.py
```

Run specific test types:
```bash
# Unit tests only
python scripts/run_tests.py --type unit

# Integration tests only
python scripts/run_tests.py --type integration
```

### Advanced Test Options

Run with coverage:
```bash
python scripts/run_tests.py --coverage
```

Run in parallel:
```bash
python scripts/run_tests.py --parallel
```

Verbose output:
```bash
python scripts/run_tests.py --verbose
```

### Using Pytest Directly

Run specific test files:
```bash
pytest tests/unit/test_atomic_updates.py -v
```

Run specific test methods:
```bash
pytest tests/unit/test_atomic_updates.py::TestAtomicSilverUpdater::test_init -v
```

Run tests with markers:
```bash
pytest -m "unit" -v
pytest -m "integration" -v
```

## Test Fixtures

### Core Fixtures

- **spark_session**: Real Spark session for integration tests
- **pipeline_config**: Test configuration
- **sample_merchants_data**: Sample merchant data
- **sample_transactions_data**: Sample transaction data
- **sample_silver_merchants_data**: Sample silver merchant data
- **sample_silver_payments_data**: Sample silver payment data

### Mock Fixtures

- **mock_spark_session**: Mock Spark session for unit tests
- **mock_logger**: Mock logger for testing
- **temp_dir**: Temporary directory for test files

## Test Data

### Sample Data Generation

The test suite includes utilities for generating test data:

```python
# Generate merchant data
merchant_data = generate_merchant_data("M000001", industry="retail")

# Generate transaction data
transaction_data = generate_transaction_data("pay_001", "M000001", amount=100.0)
```

### Test Data Schemas

- **Merchants**: Includes all required fields for bronze layer
- **Transactions**: Includes all required fields for bronze layer
- **Silver Merchants**: SCD Type 2 structure with surrogate keys
- **Silver Payments**: Fact table structure with foreign keys

## Test Coverage

### Coverage Areas

1. **Atomic Updates**
   - SCD Type 2 logic
   - Data validation
   - Atomic swaps
   - Error handling

2. **Data Quality**
   - Completeness checks
   - Consistency checks
   - Referential integrity
   - Business rules

3. **Pipeline Orchestration**
   - Job execution
   - Error handling
   - Configuration management
   - Logging

4. **Integration**
   - End-to-end flows
   - Data consistency
   - Performance
   - Error recovery

### Coverage Targets

- **Unit Tests**: >90% line coverage
- **Integration Tests**: >80% line coverage
- **Overall**: >85% line coverage

## Test Best Practices

### Unit Testing

1. **Isolation**: Each test should be independent
2. **Mocking**: Use mocks for external dependencies
3. **Assertions**: Clear and specific assertions
4. **Naming**: Descriptive test names
5. **Setup/Teardown**: Proper test lifecycle management

### Integration Testing

1. **Real Data**: Use realistic test data
2. **End-to-End**: Test complete workflows
3. **Data Validation**: Verify data integrity
4. **Performance**: Test with larger datasets
5. **Error Scenarios**: Test failure cases

### Test Data Management

1. **Fixtures**: Reusable test data
2. **Cleanup**: Proper test data cleanup
3. **Isolation**: Tests don't interfere with each other
4. **Realism**: Test data should reflect production patterns

## Continuous Integration

### GitHub Actions

The test suite is designed to run in CI/CD pipelines:

```yaml
- name: Run Tests
  run: |
    python scripts/run_tests.py --all --coverage
```

### Test Reports

- **Coverage Reports**: HTML and terminal output
- **Test Results**: JUnit XML format
- **Performance**: Duration reporting
- **Quality**: Linting and formatting checks

## Troubleshooting

### Common Issues

1. **Spark Session**: Ensure Spark is properly configured
2. **Test Data**: Verify test data schemas match expectations
3. **Dependencies**: Install all required test dependencies
4. **Permissions**: Ensure write permissions for test directories

### Debug Mode

Run tests in debug mode:
```bash
pytest --pdb tests/unit/test_atomic_updates.py::TestAtomicSilverUpdater::test_init
```

### Verbose Output

Get detailed test output:
```bash
pytest -v -s tests/unit/test_atomic_updates.py
```

## Performance Testing

### Load Testing

Test with larger datasets:
```python
# Generate 10,000 merchants and 100,000 transactions
large_merchants = generate_large_merchant_dataset(10000)
large_transactions = generate_large_transaction_dataset(100000)
```

### Benchmarking

Measure test execution time:
```bash
pytest --durations=10 tests/
```

## Maintenance

### Adding New Tests

1. **Unit Tests**: Add to appropriate test file
2. **Integration Tests**: Add to integration test file
3. **Fixtures**: Add new fixtures to conftest.py
4. **Documentation**: Update this guide

### Test Maintenance

1. **Regular Updates**: Keep tests current with code changes
2. **Performance**: Monitor test execution time
3. **Coverage**: Maintain coverage targets
4. **Quality**: Run linting and formatting

## Resources

- [Pytest Documentation](https://docs.pytest.org/)
- [Spark Testing Guide](https://spark.apache.org/docs/latest/testing.html)
- [Python Testing Best Practices](https://docs.python.org/3/library/unittest.html)
