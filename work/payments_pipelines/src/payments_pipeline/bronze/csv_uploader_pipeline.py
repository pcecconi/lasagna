#!/usr/bin/env python3
"""
CSV Uploader Pipeline

This pipeline handles CSV file upload and state management, providing a focused
service for file discovery, upload to S3/MinIO, and state tracking.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional

from pyspark.sql import SparkSession

from payments_pipeline.common.base_pipeline import BasePipeline, DataIngestionMixin
from payments_pipeline.utils.s3_uploader import S3Uploader


class CSVUploaderPipeline(BasePipeline, DataIngestionMixin):
    """
    Pipeline for uploading CSV files to S3/MinIO storage with state management.
    
    This pipeline handles:
    - File discovery based on patterns
    - State management (tracking processed files)
    - Incremental upload (only new/changed files)
    - S3/MinIO upload coordination
    - State persistence
    """
    
    def __init__(self, config: Dict[str, Any], spark_session: SparkSession, pipeline_name: str = None):
        super().__init__(
            config=config,
            spark_session=spark_session,
            pipeline_name=pipeline_name or "CSVUploaderPipeline"
        )
        self.logger = logging.getLogger(self.pipeline_name)
        self.s3_uploader = S3Uploader()
        
        # Configuration
        self.source_config = self.config.get("source_config", {})
        self.base_path = self.source_config.get("base_path")
        self.file_patterns = self.source_config.get("file_patterns", ["*.csv"])
        self.s3_status_prefix = self.config.get("s3_status_prefix", "s3a://warehouse/pipeline_status/")
        self.cleanup_after_upload = self.config.get("cleanup_after_upload", True)
        self.keep_days = self.config.get("keep_days", 0)  # 0 = remove all after upload
        
    def execute(self) -> 'PipelineResult':
        """
        Execute the CSV upload pipeline.
        """
        self.logger.info(f"Starting {self.pipeline_name}")
        
        try:
            # 1. Validate inputs
            if not self._validate_inputs():
                return self._end_execution(False, "Input validation failed")
            
            # 2. Discover files
            all_files, new_files = self._discover_files()
            if not new_files:
                self.logger.info("No new files to upload.")
                return self._end_execution(True, "No new files to upload")
            
            self.logger.info(f"Found {len(all_files)} total files, {len(new_files)} new/changed files to upload")
            
            # 3. Clean up old S3 files if configured
            if self.cleanup_after_upload:
                self.logger.info("Cleaning up old S3 files...")
                self.s3_uploader.cleanup_old_files("payments", keep_days=self.keep_days)
                self.s3_uploader.cleanup_database_files("payments_bronze")
            
            # 4. Upload new files
            upload_results = self._upload_files(new_files)
            
            # 5. Update state with successfully uploaded files
            self._update_state(upload_results["successful_uploads"])
            
            # 6. Clean up S3 files after upload if configured
            if self.cleanup_after_upload and upload_results["successful_uploads"]:
                self.logger.info("Cleaning up S3 files after successful upload...")
                self.s3_uploader.cleanup_old_files("payments", keep_days=self.keep_days)
                self.s3_uploader.cleanup_database_files("payments_bronze")
            
            success_count = len(upload_results["successful_uploads"])
            failed_count = len(upload_results["failed_uploads"])
            
            if failed_count == 0:
                self.logger.info(f"✅ Successfully uploaded {success_count} files")
                return self._end_execution(True, f"Successfully uploaded {success_count} files")
            else:
                self.logger.warning(f"⚠️ Uploaded {success_count} files, {failed_count} failed")
                return self._end_execution(True, f"Uploaded {success_count} files, {failed_count} failed")
                
        except Exception as e:
            self.logger.error(f"Pipeline failed with error: {e}", exc_info=True)
            return self._end_execution(False, f"Pipeline failed: {e}")
    
    def _validate_inputs(self) -> bool:
        """Validate input configuration."""
        if not self.base_path:
            self.logger.error("Source base_path not configured.")
            return False
            
        if not Path(self.base_path).exists():
            self.logger.error(f"Source path does not exist: {self.base_path}")
            return False
            
        if not self.file_patterns:
            self.logger.error("No file patterns configured.")
            return False
            
        return True
    
    def _discover_files(self) -> tuple[List[Path], List[Path]]:
        """
        Discover files matching patterns and identify new/changed files.
        
        Returns:
            tuple: (all_files, new_files)
        """
        data_path = Path(self.base_path)
        all_files = []
        
        # Find all files matching any pattern
        for pattern in self.file_patterns:
            pattern_files = list(data_path.glob(pattern))
            all_files.extend(pattern_files)
        
        # Remove duplicates and sort
        all_files = sorted(set(all_files))
        
        # Check which files need upload by looking for S3 markers
        new_files = []
        for file_path in all_files:
            filename = file_path.name
            current_hash = self._get_file_hash(file_path)
            
            # Check if file needs upload by looking for markers in S3
            if self._needs_upload(filename, current_hash):
                new_files.append(file_path)
                # File needs upload (logging handled at summary level)
            else:
                self.logger.debug(f"⏭️ Skipping already uploaded file: {filename}")
        
        return all_files, new_files
    
    def _upload_files(self, files: List[Path]) -> Dict[str, List]:
        """
        Upload files to S3/MinIO.
        
        Args:
            files: List of file paths to upload
            
        Returns:
            dict: Upload results with 'successful_uploads' and 'failed_uploads' lists
        """
        successful_uploads = []
        failed_uploads = []
        
        self.logger.info(f"📤 Uploading {len(files)} files to S3...")
        
        try:
            # Use the existing S3Uploader.upload_payments_data method
            s3_paths = self.s3_uploader.upload_payments_data(str(Path(self.base_path)), files)
            
            # Track successful uploads
            for file_path in files:
                filename = file_path.name
                # Check if file was successfully uploaded by looking at S3 paths
                if any(filename in path for path in s3_paths.get("all_files", [])):
                    successful_uploads.append(file_path)
                else:
                    failed_uploads.append(file_path)
                    self.logger.error(f"❌ Failed to upload: {filename}")
            
            self.logger.info(f"📊 Upload summary:")
            self.logger.info(f"   Total files processed: {len(files)}")
            self.logger.info(f"   Successful uploads: {len(successful_uploads)}")
            self.logger.info(f"   Failed uploads: {len(failed_uploads)}")
            
            if s3_paths:
                self.logger.info(f"   S3 paths generated:")
                for category, paths in s3_paths.items():
                    if category != "all_files" and paths:
                        self.logger.info(f"     {category}: {len(paths)} files")
            
        except Exception as e:
            self.logger.error(f"❌ Upload failed: {e}")
            # Mark all files as failed
            failed_uploads.extend(files)
        
        return {
            "successful_uploads": successful_uploads,
            "failed_uploads": failed_uploads,
            "s3_paths": s3_paths if 's3_paths' in locals() else {}
        }
    
    def _needs_upload(self, filename: str, current_hash: str) -> bool:
        """Check if a file needs to be uploaded by looking for S3 markers."""
        try:
            # Check if upload marker exists in S3
            marker_path = f"{self.s3_status_prefix}{filename}.uploaded"
            
            # Check if marker file exists using S3 uploader
            try:
                if self.s3_uploader.file_exists(marker_path):
                    # Marker exists, try to read it
                    marker_df = self.spark.read.format("json").option("multiline", "true").load(marker_path)
                    count = marker_df.count()
                    
                    if count > 0:
                        # Marker exists and has data
                        marker_data = marker_df.collect()[0].asDict()
                        stored_hash = marker_data.get("hash", "")
                        
                        # If hash matches, file doesn't need upload
                        if stored_hash == current_hash:
                            return False
                        else:
                            return True
                    else:
                        # Marker exists but is empty (from cleanup), treat as no marker
                        return True
                else:
                    # Marker doesn't exist, file needs upload
                    return True
                    
            except Exception:
                # Error reading marker, assume file needs upload
                return True
                
        except Exception as e:
            self.logger.warning(f"Error checking upload status for {filename}: {e}")
            return True  # Default to uploading if we can't determine status
    
    def _create_upload_marker(self, filename: str, file_hash: str, file_size: int):
        """Create an upload marker file in S3."""
        try:
            marker_data = {
                "filename": filename,
                "hash": file_hash,
                "file_size": file_size,
                "uploaded_at": datetime.now().isoformat(),
                "status": "uploaded"
            }
            
            # Create marker file in S3
            marker_path = f"{self.s3_status_prefix}{filename}.uploaded"
            
            # Convert to JSON and write to S3
            import json
            marker_json = json.dumps(marker_data, indent=2)
            
            # Use Spark to write the marker file
            marker_df = self.spark.createDataFrame([marker_data])
            marker_df.coalesce(1).write.mode("overwrite").format("json").save(marker_path)
            
            self.logger.debug(f"Created upload marker: {marker_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to create upload marker for {filename}: {e}")
    
    def _get_file_hash(self, file_path: Path) -> str:
        """Get a simple hash of file for change detection."""
        try:
            stat = file_path.stat()
            # Use file size and modification time as a simple change indicator
            return f"{stat.st_size}_{stat.st_mtime}"
        except OSError:
            return "unknown"
    
    def _update_state(self, successfully_uploaded: List[Path]):
        """Create upload markers for successfully uploaded files."""
        if not successfully_uploaded:
            return
        
        self.logger.info(f"💾 Creating upload markers for {len(successfully_uploaded)} files...")
        
        for file_path in successfully_uploaded:
            filename = file_path.name
            file_hash = self._get_file_hash(file_path)
            file_size = file_path.stat().st_size
            
            # Create upload marker in S3
            self._create_upload_marker(filename, file_hash, file_size)
    
    def get_uploaded_files_info(self) -> Dict[str, Any]:
        """
        Get information about uploaded files from S3 markers.
        
        Returns:
            dict: Information about uploaded files
        """
        try:
            # List all upload markers in S3
            markers_path = f"{self.s3_status_prefix}*.uploaded"
            
            # Try to read marker files
            try:
                marker_df = self.spark.read.format("json").load(markers_path)
                marker_count = marker_df.count()
                
                if marker_count > 0:
                    # Get the latest upload timestamp
                    latest_upload = marker_df.agg({"uploaded_at": "max"}).collect()[0][0]
                    return {
                        "total_uploaded": marker_count,
                        "latest_upload": latest_upload,
                        "status_prefix": self.s3_status_prefix
                    }
                else:
                    return {
                        "total_uploaded": 0,
                        "latest_upload": None,
                        "status_prefix": self.s3_status_prefix
                    }
            except Exception:
                return {
                    "total_uploaded": 0,
                    "latest_upload": None,
                    "status_prefix": self.s3_status_prefix
                }
        except Exception as e:
            self.logger.warning(f"Error getting uploaded files info: {e}")
            return {
                "total_uploaded": 0,
                "latest_upload": None,
                "status_prefix": self.s3_status_prefix
            }


if __name__ == "__main__":
    # This block is for direct execution, typically orchestrated by PipelineOrchestrator
    from payments_pipeline.utils.spark import get_spark_session
    
    spark_session = get_spark_session("CSVUploaderPipeline_DirectRun")
    config_example = {
        "source_config": {
            "base_path": "/usr/local/spark_dev/work/payments_data_source/raw_data",
            "file_patterns": ["merchants_*.csv", "transactions_*.csv"]
        },
        "state_file_name": "upload_state.json",
        "cleanup_after_upload": True,
        "keep_days": 0,
        "enable_debug_logging": True
    }
    
    pipeline = CSVUploaderPipeline(config_example, spark_session)
    result = pipeline.execute()
    print(f"Direct run result: {result.success} - {result.message}")
    spark_session.stop()
