"""Kafka publisher for streaming YouTube ingest data."""

import os
import json
import logging
from typing import Dict, Any, Optional
from kafka import KafkaProducer
from kafka.errors import KafkaError


logger = logging.getLogger(__name__)


class KafkaPublisher:
    """Publisher for sending YouTube ingest data to Kafka topics."""
    
    def __init__(self):
        """Initialize Kafka publisher with environment-based configuration."""
        self.host = os.getenv('KAFKA_HOST', 'localhost')
        self.port = int(os.getenv('KAFKA_PORT', '9092'))
        self.access_key = os.getenv('KAFKA_ACCESS_KEY')
        self.access_cert = os.getenv('KAFKA_ACCESS_CERT')
        self.ca_cert = os.getenv('KAFKA_CA_CERT')
        
        # Topic names from environment
        self.ingestion_control_topic = os.getenv('KAFKA_TOPIC_INGESTION_CONTROL', 'clustera-ingestion-control')
        self.raw_records_topic = os.getenv('KAFKA_TOPIC_RAW_RECORDS', 'clustera-raw-records')
        
        self.producer = None
        self._initialize_producer()
    
    def _initialize_producer(self) -> None:
        """Initialize the Kafka producer with SSL configuration if certificates are provided."""
        try:
            config = {
                'bootstrap_servers': f'{self.host}:{self.port}',
                'value_serializer': lambda v: json.dumps(v).encode('utf-8'),
                'key_serializer': lambda k: k.encode('utf-8') if k else None,
                'acks': 'all',
                'retries': 3,
                'retry_backoff_ms': 100,
            }
            
            # Add SSL configuration if certificates are provided
            if self.access_key and self.access_cert and self.ca_cert:
                config.update({
                    'security_protocol': 'SSL',
                    'ssl_keyfile': self.access_key,
                    'ssl_certfile': self.access_cert,
                    'ssl_cafile': self.ca_cert,
                    'ssl_check_hostname': True,
                })
                logger.info("Kafka producer configured with SSL")
            else:
                logger.warning("Kafka SSL certificates not provided, using plaintext connection")
            
            self.producer = KafkaProducer(**config)
            logger.info(f"Kafka producer initialized for {self.host}:{self.port}")
            
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")
            self.producer = None
    
    def publish_ingestion_control(self, control_data: Dict[str, Any], key: Optional[str] = None) -> bool:
        """
        Publish ingestion control message to trigger pipeline processing.
        
        Args:
            control_data: Control data dictionary (e.g., source URLs, ingestion commands)
            key: Optional message key (defaults to source_url if available)
            
        Returns:
            bool: True if message was sent successfully, False otherwise
        """
        if not key and 'source_url' in control_data:
            key = control_data['source_url']
        
        return self.publish_video_data(self.ingestion_control_topic, control_data, key)
    
    def publish_raw_record(self, record_data: Dict[str, Any], key: Optional[str] = None) -> bool:
        """
        Publish raw ingested record data.
        
        Args:
            record_data: Raw record data dictionary from ingestion process
            key: Optional message key (defaults to video_id or record_id if available)
            
        Returns:
            bool: True if message was sent successfully, False otherwise
        """
        if not key:
            # Try to use video_id first, then record_id as key
            if 'video_id' in record_data:
                key = record_data['video_id']
            elif 'record_id' in record_data:
                key = record_data['record_id']
        
        return self.publish_video_data(self.raw_records_topic, record_data, key)
    
    def publish_video_data(self, topic: str, video_data: Dict[str, Any], key: Optional[str] = None) -> bool:
        """
        Publish video data to a Kafka topic.
        
        Args:
            topic: Kafka topic name
            video_data: Video data dictionary to publish
            key: Optional message key (defaults to video_id if available)
            
        Returns:
            bool: True if message was sent successfully, False otherwise
        """
        if not self.producer:
            logger.error("Kafka producer not initialized, cannot publish message")
            return False
        
        if not video_data:
            logger.warning("Empty video data provided, skipping publish")
            return False
        
        # Use video_id as key if not provided
        if not key and 'video_id' in video_data:
            key = video_data['video_id']
        
        try:
            future = self.producer.send(topic, value=video_data, key=key)
            record_metadata = future.get(timeout=10)
            
            logger.info(f"Message published to topic '{topic}' partition {record_metadata.partition} "
                       f"offset {record_metadata.offset}")
            return True
            
        except KafkaError as e:
            logger.error(f"Failed to publish message to Kafka topic '{topic}': {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error publishing to Kafka: {e}")
            return False
    
    def publish_channel_data(self, topic: str, channel_data: Dict[str, Any], key: Optional[str] = None) -> bool:
        """
        Publish channel data to a Kafka topic.
        
        Args:
            topic: Kafka topic name
            channel_data: Channel data dictionary to publish
            key: Optional message key (defaults to channel_id if available)
            
        Returns:
            bool: True if message was sent successfully, False otherwise
        """
        if not key and 'channel_id' in channel_data:
            key = channel_data['channel_id']
        
        return self.publish_video_data(topic, channel_data, key)
    
    def publish_transcript_data(self, topic: str, transcript_data: Dict[str, Any], key: Optional[str] = None) -> bool:
        """
        Publish transcript data to a Kafka topic.
        
        Args:
            topic: Kafka topic name
            transcript_data: Transcript data dictionary to publish
            key: Optional message key (defaults to video_id if available)
            
        Returns:
            bool: True if message was sent successfully, False otherwise
        """
        if not key and 'video_id' in transcript_data:
            key = transcript_data['video_id']
        
        return self.publish_video_data(topic, transcript_data, key)
    
    def flush(self) -> None:
        """Flush any pending messages."""
        if self.producer:
            try:
                self.producer.flush(timeout=10)
                logger.info("Kafka producer flushed successfully")
            except Exception as e:
                logger.error(f"Failed to flush Kafka producer: {e}")
    
    def close(self) -> None:
        """Close the Kafka producer connection."""
        if self.producer:
            try:
                self.producer.close(timeout=10)
                logger.info("Kafka producer closed successfully")
            except Exception as e:
                logger.error(f"Failed to close Kafka producer: {e}")
            finally:
                self.producer = None
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close() 