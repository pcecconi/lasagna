#!/usr/bin/env python3
"""
Data Quality Runner Script

Runs comprehensive data quality checks on the payments pipeline.
Provides detailed reports and validation results.

Usage:
    python scripts/run_data_quality.py [--format json|text] [--output file]
    
Options:
    --format     Output format (json or text, default: text)
    --output     Output file path (default: stdout)
"""

import argparse
import sys
import json
from pathlib import Path

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from payments_pipeline.silver.silver_ingestion import SilverIngestionJob
from payments_pipeline.utils.spark import get_spark_session
from payments_pipeline.utils.config import PipelineConfig


def run_data_quality_checks(format='text', output_file=None):
    """Run data quality checks and return results"""
    print("🔍 Running Data Quality Checks...")
    print("=" * 50)
    
    # Initialize components
    config = PipelineConfig()
    spark = get_spark_session()
    silver_job = SilverIngestionJob(config, spark)
    
    try:
        # Run all data quality checks
        results = silver_job.data_quality_checker.run_all_checks()
        
        # Generate report
        report = silver_job.data_quality_checker.generate_report(results)
        
        # Output results
        if output_file:
            with open(output_file, 'w') as f:
                if format == 'json':
                    json.dump(results, f, indent=2, default=str)
                else:
                    f.write(report)
            print(f"✅ Results written to {output_file}")
        else:
            if format == 'json':
                print(json.dumps(results, indent=2, default=str))
            else:
                print(report)
        
        # Print summary
        if 'summary' in results:
            summary = results['summary']
            total_checks = summary.get('total_checks', 0)
            failed_checks = summary.get('failed_checks', 0)
            
            print(f"\n📊 Summary:")
            print(f"  Total checks: {total_checks}")
            print(f"  Failed checks: {failed_checks}")
            print(f"  Status: {'✅ PASS' if failed_checks == 0 else '❌ FAIL'}")
            
            if failed_checks > 0:
                print(f"\n⚠️  {failed_checks} checks failed. Review the details above.")
                return False
            else:
                print(f"\n🎉 All {total_checks} checks passed!")
                return True
        
        return True
        
    except Exception as e:
        print(f"❌ Data quality checks failed: {e}")
        return False


def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Run Data Quality Checks')
    parser.add_argument('--format', choices=['json', 'text'], default='text',
                       help='Output format (default: text)')
    parser.add_argument('--output', type=str,
                       help='Output file path (default: stdout)')
    
    args = parser.parse_args()
    
    success = run_data_quality_checks(args.format, args.output)
    
    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()