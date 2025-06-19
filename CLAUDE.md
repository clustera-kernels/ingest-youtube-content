# CLAUDE.md

This file provides guidance to Claude (or other AI assistants) when working with this Clustera kernel.

## Architecture

This is a Clustera kernel - a single-responsibility microservice that processes messages from Apache Kafka. The kernel follows a standard pattern:

1. **Kafka Consumer**: Connects to Kafka using SSL certificates stored as base64-encoded environment variables
2. **Message Processing**: Processes messages one at a time with manual offset commits for reliability
3. **Optional Producer**: Can send processed messages to output topics
4. **Error Handling**: Comprehensive error handling with logging and graceful shutdown

The kernel is designed to be:
- **Focused**: Single responsibility principle - do one thing well
- **Reliable**: Manual offset commits ensure no message loss
- **Scalable**: Multiple instances can run in parallel (same consumer group)
- **Observable**: Detailed logging for monitoring and debugging

## Key Components

### Configuration (top of main.py)
```python
KERNEL_NAME = "template"  # Identifies this kernel
DEFAULT_TOPIC = "template-input"  # Input topic
OUTPUT_TOPIC = "template-output"  # Output topic (or None)
```

### Core Function (process_message)
This is where the kernel's business logic lives. It receives:
- `message`: Kafka message with value, key, headers, offset, etc.
- `producer`: Optional KafkaProducer for sending output messages

Returns:
- `True`: Message processed successfully (offset will be committed)
- `False`: Processing failed (message will be retried)

### SSL Certificate Handling
Certificates are stored as base64-encoded strings in environment variables and written to temporary files for Kafka client use.

## Development Guidelines

### When modifying this kernel:

1. **Keep it focused**: Each kernel should have a single, clear purpose
2. **Handle errors gracefully**: Decide whether to retry (return False) or skip (return True) failed messages
3. **Log appropriately**: Use logging levels correctly (DEBUG for details, INFO for important events, ERROR for failures)
4. **Test thoroughly**: Include unit tests for process_message() and integration tests with Kafka

### Common modifications:

**Adding database access:**
```python
import psycopg2
from contextlib import closing

def process_message(message, producer):
    with closing(psycopg2.connect(os.getenv("DATABASE_URL"))) as conn:
        with conn.cursor() as cur:
            # Database operations
            conn.commit()
```

**Adding HTTP calls:**
```python
import requests

def process_message(message, producer):
    response = requests.post(
        "https://api.example.com/endpoint",
        json=data,
        timeout=30
    )
    response.raise_for_status()
```

**Batch processing:**
```python
# Change MAX_POLL_RECORDS to process multiple messages
MAX_POLL_RECORDS = 100

# Modify process_message to handle batches
def process_messages(messages, producer):
    # Process all messages in batch
    pass
```

## Environment Variables

Required:
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka broker addresses
- `KAFKA_CA_CERT`: Base64-encoded CA certificate
- `KAFKA_CLIENT_CERT`: Base64-encoded client certificate
- `KAFKA_CLIENT_KEY`: Base64-encoded client key

Optional:
- `KAFKA_CLIENT_ID`: Client identifier (defaults to kernel-name-pid)
- `KAFKA_CONSUMER_GROUP`: Consumer group (defaults to kernel-name-consumer-group)
- `KAFKA_TOPIC`: Override default input topic

## Testing

### Local testing setup:
```bash
# Copy and configure environment
cp .env.template .env
# Edit .env with your values

# Install dependencies
uv sync

# Run tests
pytest

# Run locally
uv run python main.py
```

### Writing tests:
```python
from unittest.mock import Mock, patch
import json

def test_process_message_success():
    # Mock message
    message = Mock()
    message.value = json.dumps({"test": "data"}).encode()
    message.offset = 123
    
    # Test processing
    result = process_message(message, None)
    assert result == True
```

## Deployment

The kernel is designed to run in containerized environments:

1. **Docker**: Dockerfile included for building images
2. **Railway**: Can be deployed directly from GitHub
3. **Kubernetes**: Can run as a Deployment with horizontal scaling

### Resource considerations:
- Memory: Depends on message size and processing complexity
- CPU: Generally low, scales with message throughput
- Network: Kafka connections and any external API calls

## Monitoring

Key metrics to track:
- Consumer lag (messages behind latest)
- Processing rate (messages/second)
- Error rate (failed messages)
- Processing duration (time per message)

Key logs to monitor:
- ERROR: Processing failures
- WARNING: Skipped or invalid messages
- INFO: Partition assignments, commits

## Common Issues

### SSL Certificate Problems
- Ensure certificates are properly base64-encoded
- Check certificate expiration dates
- Verify certificate chain is complete

### Consumer Lag
- Optimize process_message() performance
- Increase MAX_POLL_RECORDS for batch processing
- Scale horizontally (more instances)

### Memory Issues
- Reduce MAX_POLL_RECORDS
- Stream large data instead of loading into memory
- Check for memory leaks in process_message()

## Best Practices

1. **Idempotency**: Make processing idempotent where possible
2. **Timeouts**: Set timeouts on all external calls
3. **Circuit Breakers**: Implement circuit breakers for external dependencies
4. **Health Checks**: Add health check endpoint if running in Kubernetes
5. **Metrics**: Export metrics for monitoring systems
6. **Structured Logging**: Use structured logging for better searchability