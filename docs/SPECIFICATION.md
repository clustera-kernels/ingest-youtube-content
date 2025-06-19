# Clustera YouTube Ingest - Technical Specification

## 1. Executive Summary

Clustera YouTube Ingest is a data ingestion pipeline component of the Clustera platform, designed to extract YouTube video metadata and transcripts for storage in PostgreSQL. This component feeds the platform's agentic memories system ("Snowballs") with YouTube content data.

## 2. System Architecture

### 2.1 Component Overview
- **Ingestion SDK**: Core business logic implementation
- **CLI Tool**: Command-line interface leveraging the SDK
- **Database**: PostgreSQL for persistent storage
- **External Services**: Apify actors for YouTube data extraction

### 2.2 Technology Stack
- **Language**: Python 3.8+
- **Package Manager**: UV (for fast Python package management)
- **Database**: PostgreSQL 12+
- **ORM**: SQLAlchemy
- **Data Processing**: Pandas
- **External API**: Apify
- **Future Migration Tool**: Alembic (planned)

## 3. Database Schema

### 3.1 Naming Conventions
- Control tables: `ctrl_` prefix
- Dataset tables: `dataset_` prefix
- All tables in `public` schema

### 3.2 Table Definitions

#### ctrl_youtube_lists
```sql
CREATE TABLE ctrl_youtube_lists (
    id SERIAL PRIMARY KEY,
    source_type VARCHAR(50) NOT NULL, -- 'channel' or 'playlist'
    source_url TEXT NOT NULL UNIQUE,
    source_name TEXT,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_sync_at TIMESTAMP,
    sync_frequency_hours INTEGER DEFAULT 24
);
```

#### dataset_youtube_video
```sql
CREATE TABLE dataset_youtube_video (
    id SERIAL PRIMARY KEY,
    video_id VARCHAR(20) NOT NULL UNIQUE,
    video_url TEXT NOT NULL,
    title TEXT,
    description TEXT,
    channel_id VARCHAR(50),
    channel_name TEXT,
    channel_url TEXT,
    playlist_id VARCHAR(50),
    playlist_name TEXT,
    duration TEXT, -- Format: "HH:MM:SS" or "MM:SS"
    duration_seconds INTEGER,
    view_count BIGINT,
    like_count BIGINT,
    comment_count BIGINT,
    published_at TEXT, -- Can be relative like "2 years ago" or date
    published_date DATE, -- Parsed date when available
    transcript JSONB, -- Array of {start, dur, text} objects
    transcript_text TEXT, -- Full concatenated transcript
    transcript_language VARCHAR(10),
    thumbnail_url TEXT,
    tags TEXT[], -- Array of tags
    category VARCHAR(100),
    is_live_content BOOLEAN DEFAULT false,
    is_monetized BOOLEAN,
    comments_turned_off BOOLEAN,
    location TEXT,
    description_links JSONB, -- Array of {url, text} objects
    subtitles JSONB, -- Additional subtitle formats if available
    from_yt_url TEXT, -- Source URL this video was found from
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    transcript_ingested_at TIMESTAMP,
    metadata_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_list_id INTEGER REFERENCES ctrl_youtube_lists(id)
);
```

#### dataset_youtube_channel
```sql
CREATE TABLE dataset_youtube_channel (
    id SERIAL PRIMARY KEY,
    channel_id VARCHAR(50) NOT NULL UNIQUE,
    channel_name TEXT,
    channel_url TEXT,
    channel_description TEXT,
    channel_description_links JSONB, -- Array of {url, text} objects
    channel_joined_date TEXT,
    channel_location TEXT,
    channel_total_videos INTEGER,
    channel_total_views TEXT, -- Can be formatted like "1,710,167,563"
    channel_total_views_numeric BIGINT,
    number_of_subscribers BIGINT,
    is_monetized BOOLEAN,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### ctrl_ingestion_log
```sql
CREATE TABLE ctrl_ingestion_log (
    id SERIAL PRIMARY KEY,
    stage_name VARCHAR(50) NOT NULL,
    source_type VARCHAR(50),
    source_identifier TEXT,
    status VARCHAR(20) NOT NULL, -- 'started', 'completed', 'failed'
    error_message TEXT,
    records_processed INTEGER DEFAULT 0,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    apify_run_id VARCHAR(100),
    apify_dataset_id VARCHAR(100)
);
```

## 4. Pipeline Stages

### 4.0 Preparation Stage
**Purpose**: Initialize database schema

**Process**:
1. Execute SQL migrations from `migrations/000-create-tables.sql`
2. Verify table creation
3. Create indexes for performance optimization:
   - Index on `video_id` in `dataset_youtube_video`
   - Index on `channel_id` in `dataset_youtube_channel`
   - Index on `source_list_id` in `dataset_youtube_video`

**Error Handling**: 
- Check if tables already exist before creation
- Log all DDL operations

### 4.1 Stage 1: Sync Sources
**Purpose**: Orchestrate ingestion for all monitored sources

**Input**: Records from `ctrl_youtube_lists` where `is_active = true`

**Process**:
1. Query active sources from control table
2. Check `last_sync_at` and `sync_frequency_hours`
3. Queue eligible sources for Stage 2 processing
4. Update `last_sync_at` after successful processing

**Error Handling**:
- Continue processing other sources if one fails
- Log failures to `ctrl_ingestion_log`

### 4.2 Stage 2: List Ingestion
**Purpose**: Extract video metadata from channels/playlists

**Input**: YouTube channel or playlist URL

**Process**:
1. Call Apify actor `streamers/youtube-scraper`
2. Parse response for video metadata
3. Extract and store channel information in `dataset_youtube_channel`
4. Upsert video records to `dataset_youtube_video`
5. Queue new videos for Stage 3 transcript ingestion

**Apify Configuration**:
```json
{
    "startUrls": [{"url": "<channel_or_playlist_url>"}],
    "maxResults": 100,
    "resultsPerPage": 50,
    "handleRequestTimeoutSecs": 300,
    "proxyConfiguration": {
        "useApifyProxy": true,
        "apifyProxyGroups": ["RESIDENTIAL"]
    }
}
```

**Data Mapping**:
- Extract all available fields from actor output
- Parse relative dates (e.g., "2 years ago") when possible
- Store channel metadata separately for reuse
- Preserve all links and social media references

**Deduplication**: Check `video_id` uniqueness before insert

### 4.3 Stage 3: Video Transcript Ingestion
**Purpose**: Extract and store video transcripts

**Input**: Individual YouTube video URL/ID

**Process**:
1. Check if transcript already exists
2. Call Apify actor `pintostudio/youtube-transcript-scraper`
3. Store raw transcript array in `transcript` JSONB column
4. Generate concatenated text in `transcript_text` column
5. Set `transcript_ingested_at` timestamp

**Apify Configuration**:
```json
{
    "videoUrl": "https://www.youtube.com/watch?v=<video_id>"
}
```

**Transcript Processing**:
- Store raw transcript segments with timestamps
- Create searchable full-text version
- Calculate total transcript duration

## 5. SDK Design

### 5.1 Core Classes

```python
class YouTubeIngestor:
    """Main orchestrator for the ingestion pipeline"""
    
class ApifyClient:
    """Wrapper for Apify API interactions"""
    
class DatabaseManager:
    """Handles all database operations using SQLAlchemy"""
    
class VideoProcessor:
    """Processes and validates video data"""
    
class TranscriptProcessor:
    """Handles transcript parsing and formatting"""
    
class DateParser:
    """Converts relative dates to absolute dates"""
```

### 5.2 Key Interfaces

```python
# SDK Interface
async def sync_all_sources() -> Dict[str, Any]
async def ingest_source(source_url: str) -> List[str]
async def ingest_video_transcript(video_id: str) -> bool
async def parse_channel_data(channel_data: Dict) -> Dict
async def parse_video_data(video_data: Dict) -> Dict
```

## 6. CLI Design

**Note**: All commands should be executed using UV for proper environment management.

```bash
# Initialize database
uv run clustera-youtube-ingest init

# Sync all sources
uv run clustera-youtube-ingest sync --all

# Add new source
uv run clustera-youtube-ingest add-source --url <URL> --type <channel|playlist>

# Ingest specific source
uv run clustera-youtube-ingest ingest --url <URL>

# Ingest transcripts for existing videos
uv run clustera-youtube-ingest transcripts --missing-only

# Check ingestion status
uv run clustera-youtube-ingest status --source <URL>

# Export data
uv run clustera-youtube-ingest export --format <json|csv> --output <file>
```

**Development Commands**:
```bash
# Install dependencies
uv sync

# Add new dependency
uv add <package-name>

# Add development dependency
uv add --dev <package-name>

# Update dependencies
uv lock --upgrade

# Run tests
uv run pytest

# Format code
uv run black .
uv run isort .
```

## 7. Configuration

### 7.0 Project Setup and Dependencies

**Package Management**: This project uses UV for fast Python package management and virtual environment handling.

**Setup Commands**:
```bash
# Install UV (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create project with UV
uv init clustera-youtube-ingest
cd clustera-youtube-ingest

# Add dependencies
uv add sqlalchemy pandas apify-client
uv add --dev pytest black isort mypy

# Install project in development mode
uv pip install -e .

# Run commands with UV
uv run clustera-youtube-ingest --help
```

**Dependency Files**:
- `pyproject.toml`: Project configuration and dependencies
- `uv.lock`: Locked dependency versions for reproducible builds
- `.python-version`: Python version specification for UV

**Virtual Environment**:
UV automatically manages virtual environments. Use `uv run` to execute commands within the project environment.

### 7.1 Environment Variables
```env
# Database
DATABASE_URL=postgresql://user:pass@host:port/dbname

# Apify
APIFY_API_TOKEN=<token>
APIFY_YOUTUBE_SCRAPER_ID=streamers~youtube-scraper
APIFY_TRANSCRIPT_SCRAPER_ID=pintostudio~youtube-transcript-scraper

# Pipeline
MAX_CONCURRENT_REQUESTS=5
REQUEST_TIMEOUT_SECONDS=300
RETRY_ATTEMPTS=3
BATCH_SIZE=50

# Proxy
USE_PROXY=true
PROXY_TYPE=RESIDENTIAL
```

## 8. Error Handling & Monitoring

### 8.1 Error Categories
- **Network Errors**: Retry with exponential backoff
- **API Rate Limits**: Queue for later processing
- **Data Validation Errors**: Log and skip record
- **Database Errors**: Rollback transaction and retry
- **Transcript Not Available**: Mark and skip, retry later

### 8.2 Monitoring Metrics
- Videos ingested per hour
- Transcript success rate
- Average processing time per video
- Failed ingestion attempts
- API usage and costs
- Storage growth rate

## 9. Performance Considerations

### 9.1 Optimizations
- Batch database operations using Pandas
- Implement connection pooling
- Use async operations for API calls
- Index frequently queried columns
- Compress transcript data if needed

### 9.2 Scalability
- Horizontal scaling via multiple workers
- Queue-based processing for stages
- Partitioning strategy for large datasets
- Implement caching for channel metadata

## 10. Security Considerations

- Store API tokens securely (environment variables)
- Implement SQL injection prevention
- Validate and sanitize all external data
- Use least-privilege database user
- Encrypt sensitive data at rest

## 11. Data Quality & Validation

### 11.1 Validation Rules
- Verify video_id format (11 characters)
- Validate URLs before storage
- Check transcript segment integrity
- Ensure required fields are present

### 11.2 Data Enrichment
- Parse relative dates to absolute when possible
- Extract hashtags from descriptions
- Normalize view/subscriber counts
- Detect video language from transcript

## 12. Future Enhancements

1. **Alembic Integration**: Automated schema migrations
2. **Incremental Updates**: Update only changed metadata
3. **Multi-language Support**: Transcripts in multiple languages
4. **Webhook Support**: Real-time notifications for new videos
5. **Analytics Dashboard**: Ingestion metrics visualization
6. **Comment Extraction**: Add comment scraping capability
7. **Shorts Support**: Special handling for YouTube Shorts
8. **Batch Processing**: Process multiple videos in single Apify run

## 13. Questions for Clarification

1. **Snowball Integration**: How does this data feed into the Snowballs system?
2. **Update Frequency**: Should we re-fetch metadata for existing videos?
3. **Storage Limits**: Any constraints on transcript length or total storage?
4. **Language Priority**: Which languages should be prioritized for transcripts?
5. **Channel Discovery**: Should we auto-discover related channels?
6. **Data Retention**: How long should we keep historical data?
7. **API Quotas**: What are the Apify usage limits and budget?
8. **Transcript Formats**: Do we need SRT/VTT export capabilities?
9. **Search Functionality**: Should we implement full-text search on transcripts? 