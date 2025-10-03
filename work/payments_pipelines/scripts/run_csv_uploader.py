#!/usr/bin/env python3
"""
Run CSV Uploader Pipeline

This script runs the CSVUploaderPipeline to handle CSV file upload and state management.
This is the first step in the modular pipeline chain - it uploads files to S3/MinIO
and tracks which files have been processed.
"""

import sys
import logging
from pathlib import Path

# Add the src directory to Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pyspark.sql import SparkSession
from payments_pipeline.common.pipeline_orchestrator import PipelineOrchestrator
from payments_pipeline.bronze.csv_uploader_pipeline import CSVUploaderPipeline
from payments_pipeline.utils.spark import get_spark_session
from payments_pipeline.common.pipeline_config import PipelineConfigManager

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_csv_uploader_pipeline(spark: SparkSession, config_manager: PipelineConfigManager):
    """Runs the CSV uploader pipeline using the orchestrator."""
    logger.info("📤 Running CSV Uploader Pipeline via Orchestrator")
    
    orchestrator = PipelineOrchestrator(spark_session=spark, config_manager=config_manager)
    orchestrator.register_pipeline_class("CSVUploaderPipeline", CSVUploaderPipeline)

    # Load configuration for the development environment
    orchestrator.load_configuration(environment="development")

    # Execute the CSV uploader pipeline
    results = orchestrator.execute_pipelines(pipeline_names=["csv_uploader"])

    summary = orchestrator.get_execution_summary()
    logger.info(f"CSV Uploader Pipeline Execution Summary:\n{summary}")

    if results["csv_uploader"].success:
        logger.info("✅ CSV Uploader Pipeline completed successfully.")
        
        # Get upload statistics
        logger.info(f"📊 Upload Statistics:")
        logger.info(f"   Pipeline completed successfully: {results['csv_uploader'].success}")
        logger.info(f"   Message: {results['csv_uploader'].message}")
        logger.info(f"   Duration: {results['csv_uploader'].duration_seconds:.2f} seconds")
        
        return True
    else:
        logger.error("❌ CSV Uploader Pipeline failed.")
        return False


def run_csv_uploader_with_dependencies(spark: SparkSession, config_manager: PipelineConfigManager):
    """Runs the CSV uploader pipeline followed by dependent pipelines."""
    logger.info("🔄 Running CSV Uploader Pipeline with Dependencies")
    
    orchestrator = PipelineOrchestrator(spark_session=spark, config_manager=config_manager)
    orchestrator.register_pipeline_class("CSVUploaderPipeline", CSVUploaderPipeline)
    
    # Note: We would also register MerchantsBronzePipeline here if we want to run the full chain
    # from payments_pipeline.bronze.merchants_pipeline import MerchantsBronzePipeline
    # orchestrator.register_pipeline_class(MerchantsBronzePipeline)

    # Load configuration for the development environment
    orchestrator.load_configuration(environment="development")

    # Execute CSV uploader and its dependent pipelines
    results = orchestrator.execute_pipelines(pipeline_names=["csv_uploader"])

    summary = orchestrator.get_execution_summary()
    logger.info(f"CSV Uploader Pipeline with Dependencies Execution Summary:\n{summary}")

    return results["csv_uploader"].success


def main():
    """Main function to run the CSV uploader pipeline."""
    spark = None
    try:
        spark = get_spark_session("RunCSVUploader")
        config_manager = PipelineConfigManager(config_dir=str(Path(__file__).parent.parent / "pipeline_configs"))

        logger.info("🚀 Starting CSV Uploader Pipeline")
        logger.info("=" * 60)

        # Run the CSV uploader pipeline
        success = run_csv_uploader_pipeline(spark, config_manager)

        if success:
            logger.info("🎉 CSV Uploader Pipeline completed successfully!")
            logger.info("📋 Next steps:")
            logger.info("   1. Files are now available in S3/MinIO for downstream pipelines")
            logger.info("   2. Upload state has been persisted")
            logger.info("   3. Run dependent pipelines (e.g., modular_merchants_bronze)")
        else:
            logger.error("❌ CSV Uploader Pipeline failed.")
            return 1

        return 0

    except Exception as e:
        logger.error(f"An error occurred during CSV uploader pipeline execution: {e}", exc_info=True)
        return 1
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped.")


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
