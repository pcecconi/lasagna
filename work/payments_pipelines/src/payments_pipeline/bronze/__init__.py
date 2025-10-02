"""
Bronze Layer - Raw Data Ingestion

ELT approach: Minimal transformation, preserve raw data structure
"""

from .ingestion import BronzeIngestionJob
from .validation import DataQualityValidator

__all__ = ['BronzeIngestionJob', 'DataQualityValidator']

