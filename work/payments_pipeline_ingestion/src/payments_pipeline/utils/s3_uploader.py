"""
S3/MinIO Upload Utility

Handles uploading CSV files from local filesystem to S3/MinIO for Spark access.
"""

import os
import logging
from pathlib import Path
from typing import List, Optional
import boto3
from botocore.exceptions import ClientError


class S3Uploader:
    """
    S3/MinIO Uploader for CSV files
    
    Uploads local CSV files to S3/MinIO so Spark workers can access them.
    """
    
    def __init__(
        self,
        endpoint_url: str = "http://minio:9000",
        access_key: str = "admin",
        secret_key: str = "password",
        bucket_name: str = "warehouse"
    ):
        self.endpoint_url = endpoint_url
        self.bucket_name = bucket_name
        self.logger = logging.getLogger(__name__)
        
        # Create S3 client
        self.s3_client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name='us-east-1'
        )
        
        # Ensure bucket exists
        self._ensure_bucket_exists()
    
    def _ensure_bucket_exists(self):
        """Ensure the S3 bucket exists"""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            self.logger.info(f"✅ Bucket {self.bucket_name} exists")
        except ClientError:
            try:
                self.s3_client.create_bucket(Bucket=self.bucket_name)
                self.logger.info(f"✅ Created bucket {self.bucket_name}")
            except Exception as e:
                self.logger.error(f"❌ Failed to create bucket: {e}")
                raise
    
    def upload_file(self, local_file_path: str, s3_key: str) -> str:
        """
        Upload a single file to S3
        
        Args:
            local_file_path: Path to local file
            s3_key: S3 key (path) for the uploaded file
            
        Returns:
            S3 path (s3a://bucket/key)
        """
        try:
            # Check if local file exists
            if not os.path.exists(local_file_path):
                raise FileNotFoundError(f"Local file not found: {local_file_path}")
            
            # Upload file
            with open(local_file_path, 'rb') as f:
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=s3_key,
                    Body=f
                )
            
            s3_path = f"s3a://{self.bucket_name}/{s3_key}"
            self.logger.info(f"✅ Uploaded {local_file_path} to {s3_path}")
            
            return s3_path
            
        except Exception as e:
            self.logger.error(f"❌ Failed to upload {local_file_path}: {e}")
            raise
    
    def upload_directory(self, local_dir: str, s3_prefix: str = "payments") -> List[str]:
        """
        Upload all CSV files from a directory to S3
        
        Args:
            local_dir: Local directory containing CSV files
            s3_prefix: S3 prefix for uploaded files
            
        Returns:
            List of S3 paths
        """
        local_path = Path(local_dir)
        s3_paths = []
        
        self.logger.info(f"📁 Uploading CSV files from {local_dir}")
        
        # Find all CSV files
        csv_files = list(local_path.glob("*.csv"))
        
        if not csv_files:
            self.logger.warning(f"⚠️ No CSV files found in {local_dir}")
            return s3_paths
        
        for csv_file in csv_files:
            # Create S3 key
            s3_key = f"{s3_prefix}/{csv_file.name}"
            
            # Upload file
            s3_path = self.upload_file(str(csv_file), s3_key)
            s3_paths.append(s3_path)
        
        self.logger.info(f"✅ Uploaded {len(s3_paths)} files to S3")
        return s3_paths
    
    def upload_payments_data(self, data_directory: str) -> dict:
        """
        Upload payments pipeline data files to S3
        
        Args:
            data_directory: Directory containing payments data files
            
        Returns:
            Dictionary with S3 paths for different file types
        """
        data_path = Path(data_directory)
        s3_paths = {
            "merchants": [],
            "transactions": [],
            "all_files": []
        }
        
        self.logger.info(f"📁 Uploading payments data from {data_directory}")
        
        # Find merchant files
        merchant_files = list(data_path.glob("merchants_*.csv"))
        for file_path in merchant_files:
            s3_key = f"payments/{file_path.name}"
            s3_path = self.upload_file(str(file_path), s3_key)
            s3_paths["merchants"].append(s3_path)
            s3_paths["all_files"].append(s3_path)
        
        # Find transaction files
        transaction_files = list(data_path.glob("transactions_*.csv"))
        for file_path in transaction_files:
            s3_key = f"payments/{file_path.name}"
            s3_path = self.upload_file(str(file_path), s3_key)
            s3_paths["transactions"].append(s3_path)
            s3_paths["all_files"].append(s3_path)
        
        self.logger.info(f"✅ Uploaded {len(s3_paths['all_files'])} files:")
        self.logger.info(f"   Merchants: {len(s3_paths['merchants'])} files")
        self.logger.info(f"   Transactions: {len(s3_paths['transactions'])} files")
        
        return s3_paths
    
    def list_uploaded_files(self, s3_prefix: str = "payments") -> List[str]:
        """
        List files uploaded to S3
        
        Args:
            s3_prefix: S3 prefix to list
            
        Returns:
            List of S3 keys
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=s3_prefix
            )
            
            if 'Contents' in response:
                keys = [obj['Key'] for obj in response['Contents']]
                self.logger.info(f"📋 Found {len(keys)} files in s3://{self.bucket_name}/{s3_prefix}")
                return keys
            else:
                self.logger.info(f"📋 No files found in s3://{self.bucket_name}/{s3_prefix}")
                return []
                
        except Exception as e:
            self.logger.error(f"❌ Failed to list S3 files: {e}")
            return []
    
    def delete_file(self, s3_key: str):
        """
        Delete a file from S3
        
        Args:
            s3_key: S3 key to delete
        """
        try:
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=s3_key)
            self.logger.info(f"✅ Deleted s3://{self.bucket_name}/{s3_key}")
        except Exception as e:
            self.logger.error(f"❌ Failed to delete {s3_key}: {e}")
    
    def cleanup_old_files(self, s3_prefix: str = "payments", keep_days: int = 30):
        """
        Clean up old files from S3
        
        Args:
            s3_prefix: S3 prefix to clean up
            keep_days: Number of days to keep files
        """
        import datetime
        
        try:
            cutoff_date = datetime.datetime.now() - datetime.timedelta(days=keep_days)
            
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=s3_prefix
            )
            
            if 'Contents' in response:
                files_deleted = 0
                for obj in response['Contents']:
                    if obj['LastModified'].replace(tzinfo=None) < cutoff_date:
                        self.delete_file(obj['Key'])
                        files_deleted += 1
                
                self.logger.info(f"🧹 Cleaned up {files_deleted} old files")
            
        except Exception as e:
            self.logger.error(f"❌ Failed to cleanup old files: {e}")


def main():
    """Main function for S3 uploader"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Upload CSV files to S3/MinIO')
    parser.add_argument('--data-dir', required=True, help='Directory containing CSV files')
    parser.add_argument('--bucket', default='warehouse', help='S3 bucket name')
    parser.add_argument('--prefix', default='payments', help='S3 prefix')
    parser.add_argument('--list', action='store_true', help='List uploaded files')
    parser.add_argument('--cleanup', action='store_true', help='Clean up old files')
    
    args = parser.parse_args()
    
    # Initialize uploader
    uploader = S3Uploader(bucket_name=args.bucket)
    
    if args.list:
        # List files
        files = uploader.list_uploaded_files(args.prefix)
        for file in files:
            print(f"  {file}")
    
    elif args.cleanup:
        # Cleanup old files
        uploader.cleanup_old_files(args.prefix)
    
    else:
        # Upload files
        s3_paths = uploader.upload_payments_data(args.data_dir)
        
        print("✅ Upload completed!")
        print("📊 Uploaded files:")
        for category, paths in s3_paths.items():
            if paths:
                print(f"   {category}: {len(paths)} files")
                for path in paths:
                    print(f"     {path}")


if __name__ == "__main__":
    main()
