"""
Clustera YouTube Ingest - YouTube data ingestion pipeline for Clustera platform.

This package provides SDK and CLI tools for extracting YouTube video metadata
and transcripts for storage in PostgreSQL.
"""

__version__ = "0.1.0"

from .sdk import YouTubeIngestor, init_database, validate_environment, get_database_status
from .database import DatabaseManager
from .kafka_publisher import KafkaPublisher

__all__ = ["YouTubeIngestor", "DatabaseManager", "KafkaPublisher", "init_database", "validate_environment", "get_database_status"] 