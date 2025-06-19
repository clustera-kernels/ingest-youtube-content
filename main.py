#!/usr/bin/env python3
"""
Template Kafka Kernel for Clustera Platform

This is a template for creating new Clustera kernels. It demonstrates:
- Kafka consumer setup with SSL authentication
- Message processing pattern
- Optional Kafka producer functionality
- Error handling and logging best practices

To create a new kernel:
1. Update KERNEL_NAME to identify your kernel
2. Set DEFAULT_TOPIC to your kernel's input topic
3. Implement process_message() with your business logic
4. Optionally configure OUTPUT_TOPIC for producer functionality
"""

import os
import sys
import json
import logging
import base64
import tempfile
from typing import Optional, Dict, Any
from kafka import KafkaConsumer, KafkaProducer

# ========== KERNEL CONFIGURATION ==========
# TODO: Update these values for your kernel
KERNEL_NAME = "template"  # Unique identifier for your kernel
DEFAULT_TOPIC = "template-input"  # Input topic to consume from
OUTPUT_TOPIC = "template-output"  # Output topic (optional, set to None if not needed)

# Consumer configuration
DEFAULT_CONSUMER_GROUP = f"{KERNEL_NAME}-consumer-group"
MAX_POLL_RECORDS = 1  # Process one message at a time for reliability

# ========== SSL CERTIFICATE HANDLING ==========
def get_cert_data(env_var_name: str) -> bytes:
    """Get and decode base64 certificate from environment variable"""
    cert_b64 = os.getenv(env_var_name)
    if not cert_b64:
        raise ValueError(f"Environment variable {env_var_name} not found")
    return base64.b64decode(cert_b64)

def write_cert_to_temp_file(cert_data: bytes) -> str:
    """Write certificate data to a temporary file and return the path"""
    temp_file = tempfile.NamedTemporaryFile(mode='wb', delete=False)
    temp_file.write(cert_data)
    temp_file.close()
    return temp_file.name

# ========== MESSAGE PROCESSING ==========
def process_message(message: Any, producer: Optional[KafkaProducer] = None) -> bool:
    """
    Process a single Kafka message.
    
    This is where your kernel's main logic goes. The template implementation
    demonstrates basic message handling and optional producing.
    
    Args:
        message: Kafka message object with key, value, headers, etc.
        producer: Optional KafkaProducer instance for sending output messages
        
    Returns:
        bool: True if processing succeeded, False otherwise
    """
    try:
        # Decode message value
        if message.value is None:
            logging.warning(f"Received null message at offset {message.offset}")
            return True
            
        try:
            # Try to decode as JSON
            data = json.loads(message.value.decode('utf-8'))
            logging.info(f"Received message: {data}")
        except (json.JSONDecodeError, UnicodeDecodeError):
            # Handle non-JSON messages
            data = {"raw": message.value.decode('utf-8', errors='replace')}
            logging.info(f"Received non-JSON message: {data['raw'][:100]}...")
        
        # ========== YOUR BUSINESS LOGIC HERE ==========
        # TODO: Replace this example logic with your kernel's functionality
        
        # Example: Simple transformation
        processed_data = {
            "kernel": KERNEL_NAME,
            "timestamp": message.timestamp,
            "offset": message.offset,
            "original": data,
            "processed": f"Hello from {KERNEL_NAME} kernel! Processed: {str(data)[:50]}..."
        }
        
        # Example: Send to output topic if configured
        if producer and OUTPUT_TOPIC:
            output_message = json.dumps(processed_data).encode('utf-8')
            
            # Preserve original message key if present
            key = message.key if message.key else None
            
            future = producer.send(
                OUTPUT_TOPIC,
                value=output_message,
                key=key
            )
            
            # Wait for send to complete
            record_metadata = future.get(timeout=10)
            logging.info(
                f"Sent processed message to {record_metadata.topic} "
                f"partition {record_metadata.partition} offset {record_metadata.offset}"
            )
        
        # ========== END OF BUSINESS LOGIC ==========
        
        return True
        
    except Exception as e:
        logging.error(f"Error processing message at offset {message.offset}: {e}")
        return False

# ========== KAFKA SETUP ==========
def create_kafka_config() -> Dict[str, Any]:
    """Create Kafka configuration with SSL certificates"""
    # Get SSL certificate data from environment variables
    ca_data = get_cert_data("KAFKA_CA_CERT")
    cert_data = get_cert_data("KAFKA_CLIENT_CERT")
    key_data = get_cert_data("KAFKA_CLIENT_KEY")
    
    # Write certificate data to temporary files
    ca_file = write_cert_to_temp_file(ca_data)
    cert_file = write_cert_to_temp_file(cert_data)
    key_file = write_cert_to_temp_file(key_data)
    
    return {
        'bootstrap_servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS", ""),
        'security_protocol': "SSL",
        'ssl_cafile': ca_file,
        'ssl_certfile': cert_file,
        'ssl_keyfile': key_file,
        'ssl_check_hostname': True,
        'ssl_files': [ca_file, cert_file, key_file]  # Track for cleanup
    }

def create_consumer(kafka_config: Dict[str, Any], topic: str) -> KafkaConsumer:
    """Create and configure Kafka consumer"""
    config = kafka_config.copy()
    ssl_files = config.pop('ssl_files', [])
    
    consumer = KafkaConsumer(
        topic,
        **config,
        client_id=os.getenv("KAFKA_CLIENT_ID", f"{KERNEL_NAME}-{os.getpid()}"),
        group_id=os.getenv("KAFKA_CONSUMER_GROUP", DEFAULT_CONSUMER_GROUP),
        auto_offset_reset='earliest',  # Start from beginning if no committed offset
        enable_auto_commit=False,       # Manual commit for reliability
        max_poll_records=MAX_POLL_RECORDS,
        session_timeout_ms=30000,       # 30 seconds
        heartbeat_interval_ms=10000,    # 10 seconds  
        max_poll_interval_ms=300000,    # 5 minutes
    )
    
    return consumer

def create_producer(kafka_config: Dict[str, Any]) -> Optional[KafkaProducer]:
    """Create Kafka producer if output topic is configured"""
    if not OUTPUT_TOPIC:
        return None
        
    config = kafka_config.copy()
    config.pop('ssl_files', [])
    
    producer = KafkaProducer(
        **config,
        client_id=os.getenv("KAFKA_CLIENT_ID", f"{KERNEL_NAME}-producer-{os.getpid()}"),
        acks='all',  # Wait for all replicas
        retries=3,
        max_in_flight_requests_per_connection=1,  # Ensure ordering
    )
    
    return producer

# ========== MAIN CONSUMER LOOP ==========
def main():
    """Main entry point for the kernel"""
    # Configure logging
    logging.basicConfig(
        level=logging.INFO, 
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger(KERNEL_NAME)
    logger.info(f"Starting {KERNEL_NAME} kernel")
    
    # Get topic from environment or use default
    topic = os.getenv("KAFKA_TOPIC", DEFAULT_TOPIC)
    logger.info(f"Consuming from topic: {topic}")
    
    if OUTPUT_TOPIC:
        logger.info(f"Producing to topic: {OUTPUT_TOPIC}")
    
    # Track SSL files for cleanup
    ssl_files = []
    
    try:
        # Create Kafka configuration
        kafka_config = create_kafka_config()
        ssl_files = kafka_config.get('ssl_files', [])
        
        # Create consumer
        consumer = create_consumer(kafka_config, topic)
        
        # Create producer if needed
        producer = create_producer(kafka_config)
        
        # Subscribe with partition assignment logging
        from kafka import ConsumerRebalanceListener
        
        class PartitionListener(ConsumerRebalanceListener):
            def on_partitions_assigned(self, assigned):
                partition_info = [f"{tp.topic}:{tp.partition}" for tp in assigned]
                logger.info(f"Partitions assigned: {', '.join(partition_info)}")
            
            def on_partitions_revoked(self, revoked):
                partition_info = [f"{tp.topic}:{tp.partition}" for tp in revoked]
                logger.info(f"Partitions revoked: {', '.join(partition_info)}")
        
        consumer.subscribe([topic], listener=PartitionListener())
        
        # Main processing loop
        logger.info("Starting message processing loop")
        
        while True:
            try:
                # Poll for messages
                message_batch = consumer.poll(timeout_ms=1000)
                
                if not message_batch:
                    continue
                
                # Process messages
                for topic_partition, messages in message_batch.items():
                    logger.info(f"Received {len(messages)} messages from {topic_partition}")
                    
                    for message in messages:
                        logger.debug(f"Processing message at offset {message.offset}")
                        
                        # Process the message
                        success = process_message(message, producer)
                        
                        if success:
                            # Commit offset after successful processing
                            consumer.commit()
                            logger.debug(f"Committed offset {message.offset + 1}")
                        else:
                            logger.error(f"Failed to process message at offset {message.offset}")
                            # TODO: Implement your error handling strategy
                            # Options: retry, dead letter queue, skip, etc.
                            
            except KeyboardInterrupt:
                logger.info("Received interrupt signal, shutting down...")
                break
                
            except Exception as e:
                logger.error(f"Error in main loop: {e}", exc_info=True)
                # Continue processing unless it's a critical error
                
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
        
    finally:
        # Cleanup
        logger.info("Shutting down kernel")
        
        # Close producer if exists
        if 'producer' in locals() and producer:
            producer.close()
            
        # Close consumer if exists  
        if 'consumer' in locals():
            consumer.close()
            
        # Clean up SSL certificate files
        for cert_file in ssl_files:
            try:
                os.unlink(cert_file)
            except Exception:
                pass
                
        logger.info(f"{KERNEL_NAME} kernel stopped")

if __name__ == "__main__":
    main()
