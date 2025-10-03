#!/usr/bin/env python3
"""
Run Full Pipeline Chain

This script demonstrates the complete modular pipeline chain:
CSVUploaderPipeline → MerchantsBronzePipeline

This showcases how the modular architecture enables clean separation of concerns
and dependency management between focused, reusable pipeline components.
"""

import sys
import logging
from pathlib import Path

# Add the src directory to Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pyspark.sql import SparkSession
from payments_pipeline.common.pipeline_orchestrator import PipelineOrchestrator
from payments_pipeline.bronze.csv_uploader_pipeline import CSVUploaderPipeline
from payments_pipeline.bronze.merchants_pipeline import MerchantsBronzePipeline
from payments_pipeline.utils.spark import get_spark_session
from payments_pipeline.common.pipeline_config import PipelineConfigManager

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_full_pipeline_chain(spark: SparkSession, config_manager: PipelineConfigManager):
    """Runs the complete pipeline chain with dependency management."""
    logger.info("🔗 Running Full Pipeline Chain: CSV Upload → Merchants Bronze")
    
    orchestrator = PipelineOrchestrator(spark_session=spark, config_manager=config_manager)
    
    # Register pipeline classes (dependencies first)
    orchestrator.register_pipeline_class("CSVUploaderPipeline", CSVUploaderPipeline)
    orchestrator.register_pipeline_class("MerchantsBronzePipeline", MerchantsBronzePipeline)

    # Load configuration for the development environment
    orchestrator.load_configuration(environment="development")

    # Execute the full pipeline chain
    # The orchestrator will automatically resolve dependencies and run in correct order
    results = orchestrator.execute_pipelines(pipeline_names=["csv_uploader", "modular_merchants_bronze"])

    # Get execution summary
    summary = orchestrator.get_execution_summary()
    logger.info(f"Full Pipeline Chain Execution Summary:\n{summary}")

    # Check results
    csv_upload_success = results["csv_uploader"].success
    merchants_success = results["modular_merchants_bronze"].success

    if csv_upload_success and merchants_success:
        logger.info("✅ Full pipeline chain completed successfully!")
        return True
    else:
        logger.error("❌ Pipeline chain failed:")
        if not csv_upload_success:
            logger.error("   - CSV Uploader Pipeline failed")
        if not merchants_success:
            logger.error("   - Merchants Bronze Pipeline failed")
        return False


def run_csv_upload_only(spark: SparkSession, config_manager: PipelineConfigManager):
    """Runs only the CSV uploader pipeline."""
    logger.info("📤 Running CSV Uploader Pipeline Only")
    
    orchestrator = PipelineOrchestrator(spark_session=spark, config_manager=config_manager)
    orchestrator.register_pipeline_class("CSVUploaderPipeline", CSVUploaderPipeline)

    # Load configuration for the development environment
    orchestrator.load_configuration(environment="development")

    # Execute only the CSV uploader
    results = orchestrator.execute_pipelines(pipeline_names=["csv_uploader"])

    summary = orchestrator.get_execution_summary()
    logger.info(f"CSV Uploader Execution Summary:\n{summary}")

    return results["csv_uploader"].success


def run_merchants_only(spark: SparkSession, config_manager: PipelineConfigManager):
    """Runs only the merchants bronze pipeline (assumes CSV upload already completed)."""
    logger.info("🏪 Running Merchants Bronze Pipeline Only")
    
    orchestrator = PipelineOrchestrator(spark_session=spark, config_manager=config_manager)
    orchestrator.register_pipeline_class("CSVUploaderPipeline", CSVUploaderPipeline)  # Still register for dependency resolution
    orchestrator.register_pipeline_class(MerchantsBronzePipeline)

    # Load configuration for the development environment
    orchestrator.load_configuration(environment="development")

    # Execute only the merchants bronze pipeline
    # The orchestrator will check dependencies and skip CSV upload if already completed
    results = orchestrator.execute_pipelines(pipeline_names=["modular_merchants_bronze"])

    summary = orchestrator.get_execution_summary()
    logger.info(f"Merchants Bronze Execution Summary:\n{summary}")

    return results["modular_merchants_bronze"].success


def main():
    """Main function to run the pipeline chain."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Run modular pipeline chain")
    parser.add_argument("--mode", choices=["full", "csv-only", "merchants-only"], 
                       default="full", help="Pipeline execution mode")
    args = parser.parse_args()
    
    spark = None
    try:
        spark = get_spark_session("RunFullPipelineChain")
        config_manager = PipelineConfigManager(config_dir=str(Path(__file__).parent.parent / "pipeline_configs"))

        logger.info("🚀 Starting Modular Pipeline Chain")
        logger.info("=" * 60)
        logger.info(f"📋 Execution mode: {args.mode}")

        if args.mode == "full":
            success = run_full_pipeline_chain(spark, config_manager)
        elif args.mode == "csv-only":
            success = run_csv_upload_only(spark, config_manager)
        elif args.mode == "merchants-only":
            success = run_merchants_only(spark, config_manager)
        else:
            logger.error(f"Unknown mode: {args.mode}")
            return 1

        if success:
            logger.info("🎉 Pipeline execution completed successfully!")
            logger.info("📋 Architecture Benefits Demonstrated:")
            logger.info("   ✅ Separation of Concerns: CSV upload vs. data ingestion")
            logger.info("   ✅ Dependency Management: Automatic execution order")
            logger.info("   ✅ Reusability: CSV uploader can serve multiple pipelines")
            logger.info("   ✅ State Management: Tracked file processing state")
            logger.info("   ✅ Modularity: Each pipeline has focused responsibility")
        else:
            logger.error("❌ Pipeline execution failed.")
            return 1

        return 0

    except Exception as e:
        logger.error(f"An error occurred during pipeline execution: {e}", exc_info=True)
        return 1
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped.")


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
