# Claude Agent Instructions for Clustera YouTube Ingest

## 🎯 Project Overview

You are working on **Clustera YouTube Ingest**, a robust data ingestion pipeline that extracts YouTube video metadata and transcripts for storage in PostgreSQL. This component feeds the Clustera platform's agentic memories system ("Snowballs") with YouTube content data.

### Key Objectives
- Extract YouTube video metadata and transcripts via Apify actors
- Store data in PostgreSQL with proper schema and relationships
- Provide CLI and SDK interfaces for pipeline operations
- Support multi-stage processing with error resilience
- Enable downstream Kafka streaming for real-time analytics

## 🏗️ Architecture & Design Principles

### Core Architecture
```
┌─────────────┐     ┌──────────────┐     ┌──────────────┐
│   CLI/SDK   │────▶│  Ingestion   │────▶│  PostgreSQL  │
│  Interface  │     │   Pipeline   │     │   Database   │
└─────────────┘     └──────────────┘     └──────────────┘
                            │                      │
                            ▼                      ▼
                    ┌──────────────┐     ┌──────────────┐
                    │ Apify Actors │     │    Kafka     │
                    └──────────────┘     └──────────────┘
```

### Design Principles
1. **SDK-First**: All business logic in SDK, CLI only orchestrates
2. **Defensive Programming**: Always validate inputs and handle failures gracefully
3. **Async Architecture**: Use async/await for all I/O operations
4. **Database Abstraction**: All DB operations through `DatabaseManager`
5. **Error Isolation**: Individual failures don't break batch processing
6. **Configuration-Driven**: Environment variables control behavior

## 📁 Project Structure

```
modules/ingest-youtube-content/
├── src/clustera_youtube_ingest/
│   ├── __init__.py
│   ├── sdk.py                  # Main orchestrator (YouTubeIngestor)
│   ├── cli.py                  # Click-based CLI interface
│   ├── database.py             # DatabaseManager for all DB operations
│   ├── models.py               # SQLAlchemy ORM models
│   ├── apify_client.py         # Async Apify API wrapper
│   ├── kafka_publisher.py      # Kafka streaming integration
│   ├── source_manager.py       # YouTube source CRUD operations
│   ├── sync_orchestrator.py    # Stage 1 sync coordination
│   ├── list_ingestion.py       # Stage 2 video metadata extraction
│   ├── transcript_ingestion.py # Stage 3 transcript processing
│   ├── processors.py           # Data parsing and validation
│   └── url_utils.py           # YouTube URL validation
├── migrations/
│   └── 000-create-tables.sql   # Initial schema
├── docs/
│   ├── SPECIFICATION.md        # Technical specification
│   ├── ABSTRACT.md            # High-level overview
│   └── developer-log.md       # Development decisions
├── pyproject.toml             # Project configuration
└── .env.example               # Environment template
```

## 🔄 Pipeline Stages

### Stage 0: Preparation
- Initialize database schema
- Create control and dataset tables
- Set up indexes for performance

### Stage 1: Source Synchronization
- Manage YouTube channels/playlists in `ctrl_youtube_lists`
- Orchestrate sync operations based on frequency
- Queue sources for Stage 2 processing

### Stage 2: Video Metadata Ingestion
- Use Apify `streamers/youtube-scraper` actor
- Extract channel and video metadata
- Store in `dataset_youtube_video` and `dataset_youtube_channel`
- Queue videos for Stage 3 transcript processing

### Stage 3: Transcript Processing
- Use Apify `pintostudio/youtube-transcript-scraper` actor
- Validate transcript quality (segments, language, completeness)
- Store in `transcript` and `transcript_text` fields
- Publish complete records to Kafka

## 💻 Working with the Codebase

### Command Patterns

When implementing new features or fixing bugs, follow these patterns:

1. **Database Operations** - Always use DatabaseManager:
```python
# ✅ Good
async def get_videos(self, resource_pool: str):
    return await self.db_manager.get_videos_by_resource_pool(resource_pool)

# ❌ Bad - Direct database access
async def get_videos(self):
    return self.session.query(YouTubeVideo).all()
```

2. **Error Handling** - Use defensive programming:
```python
# ✅ Good
try:
    result = await self.apify_client.run_actor(...)
    if not result or 'error' in result:
        logger.error(f"Actor failed: {result}")
        return None
    return self.process_result(result)
except Exception as e:
    logger.error(f"Unexpected error: {e}")
    return None

# ❌ Bad - No error handling
result = await self.apify_client.run_actor(...)
return self.process_result(result)
```

3. **Async Patterns** - Use async throughout:
```python
# ✅ Good
async def process_batch(self, items: List[Dict]):
    tasks = [self.process_item(item) for item in items]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    return [r for r in results if not isinstance(r, Exception)]

# ❌ Bad - Blocking operations
def process_batch(self, items: List[Dict]):
    results = []
    for item in items:
        results.append(self.process_item(item))  # Blocking!
    return results
```

### CLI Command Structure

When adding CLI commands:

1. Use Click decorators and groups
2. Include help text and examples
3. Add progress indicators for long operations
4. Use emojis for visual feedback
5. Validate inputs early

Example:
```python
@cli.command()
@click.option('--resource-pool', required=True, help='Resource pool identifier')
@click.option('--limit', type=int, default=50, help='Number of videos to process')
def process_videos(resource_pool: str, limit: int):
    """Process videos from the specified resource pool.
    
    Example:
        uv run clustera-youtube-ingest process-videos --resource-pool prod --limit 100
    """
    click.echo("🎬 Processing videos...")
    # Implementation
```

### Database Schema Conventions

1. **Table Naming**:
   - Control tables: `ctrl_` prefix
   - Dataset tables: `dataset_` prefix
   
2. **Field Conventions**:
   - Use `_at` suffix for timestamps
   - Use `_id` suffix for foreign keys
   - JSONB for structured data (transcripts, links)
   - Arrays for lists (tags)

3. **Resource Pool Field**:
   - Always include in new queries
   - Pass through all pipeline stages
   - Use for data isolation

### Testing Approach

When writing tests:

1. Mock external services (Apify, Kafka)
2. Use async test fixtures
3. Test error conditions
4. Verify database state changes
5. Check logging output

## 🔧 Common Tasks

### Adding a New Pipeline Stage

1. Create a new manager class in its own file
2. Add stage methods to `sdk.py`
3. Add CLI commands in `cli.py`
4. Update `DatabaseManager` with new operations
5. Add configuration to `.env.example`
6. Document in `developer-log.md`

### Modifying Database Schema

1. Create migration file in `migrations/`
2. Update SQLAlchemy models in `models.py`
3. Update `DatabaseManager` methods
4. Test migration on fresh database
5. Document schema changes

### Integrating New Apify Actors

1. Add actor configuration to `apify_client.py`
2. Create processor in `processors.py`
3. Add actor ID to environment config
4. Implement retry logic
5. Add data validation

## ⚠️ Important Considerations

### Performance
- Use batch operations for database writes
- Implement connection pooling
- Add indexes for frequently queried fields
- Use async operations for parallelism

### Security
- Never log sensitive data (API tokens)
- Validate all external inputs
- Use parameterized queries (via SQLAlchemy)
- Store credentials in environment variables

### Error Handling
- Log errors with context
- Use specific exception types
- Implement retry with exponential backoff
- Gracefully degrade functionality

### Data Quality
- Validate YouTube URLs before processing
- Check transcript quality scores
- Handle missing or incomplete data
- Track processing statistics

## 📝 Code Style Guidelines

1. **Imports**: Group by standard library, third-party, local
2. **Type Hints**: Use for all function parameters and returns
3. **Docstrings**: Include for all public methods
4. **Logging**: Use appropriate log levels (debug, info, warning, error)
5. **Constants**: Define at module level in UPPER_CASE

## 🚀 Quick Reference

### Environment Variables
```bash
CLUSTERA_DATABASE_URL=postgresql://user:pass@localhost:5432/clustera
APIFY_TOKEN=your_token
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
MAX_CONCURRENT_SYNCS=3
BATCH_SIZE_VIDEOS=50
```

### Common CLI Commands
```bash
# Initialize database
uv run clustera-youtube-ingest init

# Add a YouTube source
uv run clustera-youtube-ingest add-source --url "https://www.youtube.com/@channel" --resource-pool prod

# Run complete pipeline
uv run clustera-youtube-ingest pipeline --url "https://www.youtube.com/@channel" --resource-pool prod

# Check statistics
uv run clustera-youtube-ingest stats
```

### Key Database Tables
- `ctrl_youtube_lists` - YouTube sources to monitor
- `dataset_youtube_video` - Video metadata and transcripts
- `dataset_youtube_channel` - Channel information
- `ctrl_ingestion_log` - Processing history

## 🤝 Integration Points

### Kafka Publishing
- Topic: `clustera-raw-records`
- Format: JSON with video metadata and transcripts
- Triggered after successful transcript processing

### Apify Actors
- Video scraping: `streamers/youtube-scraper`
- Transcript extraction: `pintostudio/youtube-transcript-scraper`
- Use residential proxies for reliability

### PostgreSQL
- Connection via SQLAlchemy 2.0
- Support for both `postgres://` and `postgresql://` URLs
- JSONB fields for structured data

## 💡 Best Practices

1. **Always use resource_pool** for data isolation
2. **Check existing sources** before adding duplicates
3. **Monitor Apify quotas** to avoid rate limits
4. **Validate transcript quality** before storing
5. **Log processing statistics** for monitoring
6. **Handle partial failures** gracefully
7. **Use async for I/O operations**
8. **Follow existing patterns** in the codebase

## 🐛 Debugging Tips

1. Enable debug logging: `export LOG_LEVEL=DEBUG`
2. Check `ctrl_ingestion_log` for processing history
3. Verify Apify actor responses in logs
4. Monitor Kafka publishing success rates
5. Use `--dry-run` for sync operations
6. Check database connection with `status` command

Remember: This is a production system handling real data. Always test changes thoroughly and follow the established patterns for consistency and reliability. 