"""
Input Manager for Pipeline File Processing

This module provides the InputManager class for managing file discovery,
state tracking, and processing coordination for pipeline inputs.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import input_file_name, lit

from .base_pipeline import BasePipeline


class InputManager:
    """
    Manages input file discovery, state tracking, and processing coordination.
    
    This class abstracts the common logic for:
    - Discovering uploaded files from S3 markers
    - Tracking processing state to avoid reprocessing
    - Creating processing markers for successful operations
    - Filtering files by patterns
    """
    
    def __init__(self, spark_session: SparkSession, s3_status_prefix: str, pipeline_name: str):
        """
        Initialize InputManager
        
        Args:
            spark_session: Spark session for S3 operations
            s3_status_prefix: S3 path prefix for status markers (e.g., "s3a://warehouse/pipeline_status/")
            pipeline_name: Name of the pipeline using this manager
        """
        self.spark = spark_session
        self.s3_status_prefix = s3_status_prefix.rstrip('/') + '/'
        self.pipeline_name = pipeline_name
        
    def get_uploaded_files(self, file_pattern: Optional[str] = None) -> List[str]:
        """
        Get list of files that have been uploaded (have upload markers)
        
        Args:
            file_pattern: Optional pattern to filter files (e.g., "merchants_", "transactions_")
            
        Returns:
            List of filenames that have been uploaded
        """
        try:
            # List all upload markers in S3
            markers_path = f"{self.s3_status_prefix}*.uploaded"
            
            # Try to read marker files
            try:
                marker_df = self.spark.read.format("json").load(markers_path)
                if marker_df.count() > 0:
                    # Extract filenames from markers
                    filenames = [row.filename for row in marker_df.select("filename").collect()]
                    
                    # Filter by pattern if provided
                    if file_pattern:
                        filenames = [f for f in filenames if f.startswith(file_pattern)]
                    
                    return filenames
                else:
                    return []
            except Exception:
                return []
                
        except Exception as e:
            raise RuntimeError(f"Failed to get uploaded files: {e}")
    
    def get_next_file_to_process(self, file_pattern: Optional[str] = None, 
                                s3_data_prefix: Optional[str] = None) -> Optional[str]:
        """
        Get the next S3 file that needs processing (uploaded but not yet processed)
        
        Args:
            file_pattern: Optional pattern to filter files (e.g., "merchants_", "transactions_")
            s3_data_prefix: S3 path prefix for data files (e.g., "s3a://warehouse/payments/")
            
        Returns:
            S3 path for the next file to process, or None if no files need processing
        """
        try:
            # Get all uploaded files
            uploaded_files = self.get_uploaded_files(file_pattern)
            
            for filename in uploaded_files:
                # Check if file was already processed
                if not self.has_processing_marker(filename):
                    # Construct S3 path for the file
                    if s3_data_prefix:
                        s3_path = f"{s3_data_prefix.rstrip('/')}/{filename}"
                    else:
                        s3_path = filename  # Assume filename is already a full S3 path
                    return s3_path
            
            return None  # No files need processing
            
        except Exception as e:
            raise RuntimeError(f"Failed to get next file to process: {e}")
    
    def get_new_files_to_process(self, file_pattern: Optional[str] = None, 
                                s3_data_prefix: Optional[str] = None) -> List[str]:
        """
        Get list of S3 files that need processing (uploaded but not yet processed)
        
        Args:
            file_pattern: Optional pattern to filter files (e.g., "merchants_", "transactions_")
            s3_data_prefix: S3 path prefix for data files (e.g., "s3a://warehouse/payments/")
            
        Returns:
            List of S3 paths for files that need processing
        """
        try:
            # Get all uploaded files
            uploaded_files = self.get_uploaded_files(file_pattern)
            
            s3_files_to_process = []
            for filename in uploaded_files:
                # Check if file was already processed
                if not self.has_processing_marker(filename):
                    # Construct S3 path for the file
                    if s3_data_prefix:
                        s3_path = f"{s3_data_prefix.rstrip('/')}/{filename}"
                    else:
                        s3_path = filename  # Assume filename is already a full S3 path
                    s3_files_to_process.append(s3_path)
            
            return s3_files_to_process
            
        except Exception as e:
            raise RuntimeError(f"Failed to get files to process: {e}")
    
    def has_processing_marker(self, filename: str) -> bool:
        """
        Check if file has a processing marker in S3
        
        Args:
            filename: Name of the file to check
            
        Returns:
            True if file has processing marker, False otherwise
        """
        try:
            marker_path = f"{self.s3_status_prefix}{filename}.processed"
            marker_df = self.spark.read.format("json").load(marker_path)
            count = marker_df.count()
            return count > 0
        except Exception:
            return False
    
    def mark_files_as_processed(self, filenames: List[str], row_counts: List[int]) -> None:
        """
        Mark files as successfully processed by creating processing markers
        
        Args:
            filenames: List of filenames that were processed
            row_counts: List of row counts for each processed file
        """
        if len(filenames) != len(row_counts):
            raise ValueError("filenames and row_counts must have the same length")
        
        for filename, row_count in zip(filenames, row_counts):
            try:
                self._create_processing_marker(filename, row_count)
            except Exception as e:
                raise RuntimeError(f"Failed to create processing marker for {filename}: {e}")
    
    def _create_processing_marker(self, filename: str, row_count: int) -> None:
        """
        Create a processing marker file in S3
        
        Args:
            filename: Name of the processed file
            row_count: Number of rows processed
        """
        try:
            marker_data = {
                "filename": filename,
                "processed_at": datetime.now().isoformat(),
                "rows_processed": row_count,
                "status": "processed",
                "pipeline": self.pipeline_name
            }
            
            # Create marker file in S3
            marker_path = f"{self.s3_status_prefix}{filename}.processed"
            
            # Use Spark to write the marker file
            marker_df = self.spark.createDataFrame([marker_data])
            marker_df.coalesce(1).write.mode("overwrite").format("json").save(marker_path)
            
        except Exception as e:
            raise RuntimeError(f"Failed to create processing marker for {filename}: {e}")
    
    def load_data_from_file(self, s3_file: str, file_format: str = "csv", 
                           options: Optional[Dict[str, str]] = None) -> DataFrame:
        """
        Load data from a single S3 file using Spark
        
        Args:
            s3_file: S3 file path
            file_format: File format (csv, json, parquet, etc.)
            options: Optional options for the file reader
            
        Returns:
            Spark DataFrame with data from the file
        """
        if not s3_file:
            raise ValueError("No file provided for loading")
        
        try:
            # Build the reader
            reader = self.spark.read.format(file_format)
            
            # Add options if provided
            if options:
                for key, value in options.items():
                    reader = reader.option(key, value)
            
            # Load the file - use csv() method for CSV files
            if file_format.lower() == "csv":
                df = reader.csv(s3_file)
            else:
                df = reader.load(s3_file)
            
            # Add metadata columns
            df = df.withColumn("source_file", input_file_name())
            
            return df
            
        except Exception as e:
            raise RuntimeError(f"Failed to load data from file {s3_file}: {e}")
    
    def get_processing_stats(self, file_pattern: Optional[str] = None) -> Dict[str, Any]:
        """
        Get processing statistics for files
        
        Args:
            file_pattern: Optional pattern to filter files
            
        Returns:
            Dictionary with processing statistics
        """
        try:
            # Get uploaded files
            uploaded_files = self.get_uploaded_files(file_pattern)
            
            # Count processed files
            processed_count = 0
            total_rows = 0
            
            for filename in uploaded_files:
                if self.has_processing_marker(filename):
                    processed_count += 1
                    # Try to get row count from marker
                    try:
                        marker_path = f"{self.s3_status_prefix}{filename}.processed"
                        marker_df = self.spark.read.format("json").load(marker_path)
                        if marker_df.count() > 0:
                            row_count = marker_df.select("rows_processed").collect()[0]["rows_processed"]
                            total_rows += row_count
                    except Exception:
                        pass  # Skip if can't read marker
            
            return {
                "total_uploaded": len(uploaded_files),
                "processed": processed_count,
                "pending": len(uploaded_files) - processed_count,
                "total_rows_processed": total_rows
            }
            
        except Exception as e:
            raise RuntimeError(f"Failed to get processing stats: {e}")
