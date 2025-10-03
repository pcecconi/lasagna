#!/usr/bin/env python3
"""
Test runner for workspace container environment
This script is designed to run inside the workspace container where all dependencies are available
"""

import sys
import subprocess
import os
from pathlib import Path


def run_tests():
    """Run tests in container environment"""
    
    # Add src to Python path
    src_path = Path(__file__).parent / "src"
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))
    
    # Test directory
    test_dir = Path(__file__).parent / "tests"
    
    print("🧪 Running Tests for Payments Pipeline Modular Architecture")
    print("=" * 65)
    print("📋 Environment: Workspace Container")
    print("📦 Dependencies: PySpark, PyYAML, pytest available")
    print("=" * 65)
    
    # Run pytest with coverage - test all new modular architecture components
    modular_tests = [
        "test_base_pipeline.py",
        "test_data_quality.py", 
        "test_pipeline_config.py",
        "test_pipeline_orchestrator.py",
        "test_schema_manager.py"
    ]
    
    test_files = [str(test_dir / "unit" / test) for test in modular_tests]
    
    # Set PYTHONPATH environment variable
    env = os.environ.copy()
    env['PYTHONPATH'] = str(src_path)
    
    cmd = [
        "python3", "-m", "pytest",
    ] + test_files + [
        "-v",
        "--cov=payments_pipeline.common",
        "--cov-report=term-missing",
        "--cov-report=html:htmlcov",
        "--cov-fail-under=80",  # Aim for 80%+ coverage on new components
        "--tb=short",
        # Note: Cleanup is handled by fixtures in conftest.py
    ]
    
    try:
        print(f"🚀 Running command: {' '.join(cmd)}")
        print()
        
        result = subprocess.run(cmd, capture_output=False, text=True, env=env)
        
        if result.returncode == 0:
            print("\n✅ All tests passed!")
            print("📊 Coverage report generated in htmlcov/")
            return True
        else:
            print(f"\n❌ Tests failed with return code: {result.returncode}")
            return False
            
    except FileNotFoundError:
        print("❌ pytest not found. Please install test dependencies:")
        print("   pip install -r requirements-test.txt")
        return False
    except Exception as e:
        print(f"❌ Error running tests: {e}")
        return False


if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)
