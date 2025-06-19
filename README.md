# Clustera Kernel Template

This is a template for creating Clustera kernels - single-responsibility microservices that process messages from Apache Kafka as part of the Clustera platform.

## What is a Kernel?

A kernel is a focused, single-purpose service that:
- Consumes messages from one or more Kafka topics
- Processes messages according to its specific business logic
- Optionally produces messages to other Kafka topics
- Runs independently and can be scaled horizontally

## Quick Start

### 1. Create Your Kernel

From the parent repository, run:
```bash
uv run new-kernel
# or
make new-kernel
```

This will create a new kernel repository from this template.

### 2. Configure Your Kernel

Edit `main.py` and update these values:

```python
KERNEL_NAME = "your-kernel-name"  # Unique identifier
DEFAULT_TOPIC = "your-input-topic"  # Topic to consume from
OUTPUT_TOPIC = "your-output-topic"  # Optional: topic to produce to
```

### 3. Implement Your Logic

Replace the `process_message()` function with your business logic:

```python
def process_message(message: Any, producer: Optional[KafkaProducer] = None) -> bool:
    """
    Process a single Kafka message.
    
    Args:
        message: Kafka message with key, value, headers, etc.
        producer: Optional producer for sending output messages
        
    Returns:
        bool: True if successful, False otherwise
    """
    # Your implementation here
    return True
```

### 4. Set Up Environment

Copy and configure the environment template:
```bash
cp .env.template .env
```

Required environment variables:
- `KAFKA_BOOTSTRAP_SERVERS` - Kafka broker address
- `KAFKA_CA_CERT` - Base64-encoded CA certificate
- `KAFKA_CLIENT_CERT` - Base64-encoded client certificate
- `KAFKA_CLIENT_KEY` - Base64-encoded client key

### 5. Install Dependencies

```bash
uv sync
```

### 6. Run Your Kernel

```bash
python main.py
# or
uv run python main.py
```

## Template Structure

```
your-kernel/
├── main.py              # Main kernel implementation
├── pyproject.toml       # Python dependencies
├── Dockerfile           # Container configuration
├── .env.template        # Environment variable template
├── README.md            # This file (update for your kernel)
├── CLAUDE.md            # AI assistant documentation
└── docs/
    └── message-spec.md  # Clustera message specification
```

## Common Patterns

### Consumer-Only Kernel

For kernels that only consume messages:

```python
OUTPUT_TOPIC = None  # Disable producer

def process_message(message: Any, producer: Optional[KafkaProducer] = None) -> bool:
    data = json.loads(message.value.decode('utf-8'))
    
    # Process data (e.g., store in database, call API)
    store_in_database(data)
    
    return True
```

### Transform Kernel

For kernels that transform and forward messages:

```python
def process_message(message: Any, producer: Optional[KafkaProducer] = None) -> bool:
    input_data = json.loads(message.value.decode('utf-8'))
    
    # Transform data
    output_data = {
        "processed_at": datetime.now().isoformat(),
        "original_id": input_data.get("id"),
        "transformed": transform_logic(input_data)
    }
    
    # Send to output topic
    if producer:
        producer.send(
            OUTPUT_TOPIC,
            value=json.dumps(output_data).encode('utf-8'),
            key=message.key
        )
    
    return True
```

### Enrichment Kernel

For kernels that enrich messages with external data:

```python
def process_message(message: Any, producer: Optional[KafkaProducer] = None) -> bool:
    data = json.loads(message.value.decode('utf-8'))
    
    # Enrich with external data
    enriched = {
        **data,
        "user_details": fetch_user_details(data["user_id"]),
        "location_info": geocode_address(data["address"])
    }
    
    # Forward enriched message
    if producer:
        producer.send(OUTPUT_TOPIC, value=json.dumps(enriched).encode('utf-8'))
    
    return True
```

## Error Handling

The template includes basic error handling. Enhance it based on your needs:

```python
def process_message(message: Any, producer: Optional[KafkaProducer] = None) -> bool:
    try:
        # Your processing logic
        return True
    except ValidationError as e:
        # Log and skip invalid messages
        logging.warning(f"Invalid message: {e}")
        return True  # Mark as processed to move on
    except TemporaryError as e:
        # Retry temporary failures
        logging.error(f"Temporary failure: {e}")
        return False  # Will not commit offset
    except Exception as e:
        # Handle unexpected errors
        logging.error(f"Unexpected error: {e}", exc_info=True)
        # Decide: skip (return True) or retry (return False)
        return False
```

## Testing

Create tests for your kernel:

```python
# test_kernel.py
import json
from unittest.mock import Mock
from main import process_message

def test_process_message():
    # Create mock message
    message = Mock()
    message.value = json.dumps({"test": "data"}).encode('utf-8')
    message.key = b"test-key"
    message.offset = 123
    
    # Create mock producer
    producer = Mock()
    
    # Test processing
    result = process_message(message, producer)
    
    assert result == True
    # Assert producer was called correctly
    producer.send.assert_called_once()
```

## Deployment

### Docker

Build and run with Docker:

```bash
docker build -t clustera-your-kernel .
docker run --env-file .env clustera-your-kernel
```

### Railway

1. Connect your kernel's GitHub repository to Railway
2. Add environment variables from `.env`
3. Deploy with automatic restarts on failure

## Monitoring

The kernel logs important events:
- Startup and shutdown
- Partition assignments
- Message processing (with offsets)
- Errors and warnings

Monitor these logs in your deployment environment.

## Advanced Configuration

### Batch Processing

To process messages in batches, modify `MAX_POLL_RECORDS`:

```python
MAX_POLL_RECORDS = 100  # Process up to 100 messages at once
```

### Custom Serialization

For non-JSON messages, implement custom serialization:

```python
import pickle
import msgpack

def process_message(message: Any, producer: Optional[KafkaProducer] = None) -> bool:
    # Deserialize based on content type
    if message.headers.get("content-type") == "application/msgpack":
        data = msgpack.unpackb(message.value)
    elif message.headers.get("content-type") == "application/pickle":
        data = pickle.loads(message.value)
    else:
        data = json.loads(message.value.decode('utf-8'))
    
    # Process data...
```

### Multiple Topics

To consume from multiple topics:

```python
TOPICS = ["topic1", "topic2", "topic3"]

# In main():
consumer.subscribe(TOPICS, listener=PartitionListener())
```

## Troubleshooting

### SSL Certificate Issues

If you get SSL errors:
1. Ensure certificates are properly base64-encoded
2. Check certificate expiration
3. Verify certificate permissions

### Consumer Lag

If your kernel falls behind:
1. Increase `MAX_POLL_RECORDS` for batch processing
2. Optimize `process_message()` performance
3. Scale horizontally (run multiple instances)

### Memory Issues

For memory-intensive processing:
1. Process messages one at a time (`MAX_POLL_RECORDS = 1`)
2. Implement streaming processing
3. Use external storage for large data

## Next Steps

1. Update this README with your kernel's specific documentation
2. Add integration tests
3. Set up monitoring and alerting
4. Configure auto-scaling based on lag
5. Implement health checks

For more information about the Clustera platform and message specifications, see `docs/message-spec.md`.