#!/usr/bin/env python3
"""
Test runner script for payments pipeline ingestion
"""

import sys
import os
import subprocess
import argparse
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent.parent / "src"))


def run_tests(test_type="all", coverage=False, verbose=False, parallel=False):
    """Run tests with specified options"""
    
    # Base pytest command
    cmd = ["python3", "-m", "pytest"]
    
    # Add test paths based on type
    if test_type == "unit":
        cmd.append("tests/unit/")
    elif test_type == "integration":
        cmd.append("tests/integration/")
    elif test_type == "all":
        cmd.append("tests/")
    else:
        print(f"Unknown test type: {test_type}")
        return False
    
    # Add coverage if requested
    if coverage:
        cmd.extend(["--cov=payments_pipeline", "--cov-report=html", "--cov-report=term"])
    
    # Add verbosity
    if verbose:
        cmd.append("-v")
    
    # Add parallel execution
    if parallel:
        cmd.extend(["-n", "auto"])
    
    # Add other options
    cmd.extend([
        "--tb=short",
        "--strict-markers",
        "--disable-warnings",
        "--color=yes",
        "--durations=10"
    ])
    
    print(f"Running command: {' '.join(cmd)}")
    
    # Run tests
    try:
        result = subprocess.run(cmd, cwd=Path(__file__).parent.parent)
        return result.returncode == 0
    except Exception as e:
        print(f"Error running tests: {e}")
        return False


def run_linting():
    """Run code linting"""
    print("Running code linting...")
    
    # Run pylint
    pylint_cmd = ["python", "-m", "pylint", "src/payments_pipeline/"]
    try:
        subprocess.run(pylint_cmd, cwd=Path(__file__).parent.parent)
    except Exception as e:
        print(f"Error running pylint: {e}")
    
    # Run flake8
    flake8_cmd = ["python", "-m", "flake8", "src/payments_pipeline/"]
    try:
        subprocess.run(flake8_cmd, cwd=Path(__file__).parent.parent)
    except Exception as e:
        print(f"Error running flake8: {e}")


def run_formatting():
    """Run code formatting"""
    print("Running code formatting...")
    
    # Run black
    black_cmd = ["python", "-m", "black", "src/payments_pipeline/"]
    try:
        subprocess.run(black_cmd, cwd=Path(__file__).parent.parent)
    except Exception as e:
        print(f"Error running black: {e}")
    
    # Run isort
    isort_cmd = ["python", "-m", "isort", "src/payments_pipeline/"]
    try:
        subprocess.run(isort_cmd, cwd=Path(__file__).parent.parent)
    except Exception as e:
        print(f"Error running isort: {e}")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Test runner for payments pipeline")
    parser.add_argument("--type", choices=["unit", "integration", "all"], 
                       default="all", help="Type of tests to run")
    parser.add_argument("--coverage", action="store_true", 
                       help="Run with coverage reporting")
    parser.add_argument("--verbose", "-v", action="store_true", 
                       help="Verbose output")
    parser.add_argument("--parallel", "-p", action="store_true", 
                       help="Run tests in parallel")
    parser.add_argument("--lint", action="store_true", 
                       help="Run code linting")
    parser.add_argument("--format", action="store_true", 
                       help="Run code formatting")
    parser.add_argument("--all", action="store_true", 
                       help="Run all checks (tests, linting, formatting)")
    
    args = parser.parse_args()
    
    success = True
    
    if args.all:
        # Run formatting
        run_formatting()
        
        # Run linting
        run_linting()
        
        # Run tests
        success = run_tests(
            test_type=args.type,
            coverage=args.coverage,
            verbose=args.verbose,
            parallel=args.parallel
        )
    else:
        if args.format:
            run_formatting()
        
        if args.lint:
            run_linting()
        
        if not args.format and not args.lint:
            # Run tests by default
            success = run_tests(
                test_type=args.type,
                coverage=args.coverage,
                verbose=args.verbose,
                parallel=args.parallel
            )
    
    if success:
        print("✅ All checks passed!")
        sys.exit(0)
    else:
        print("❌ Some checks failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
