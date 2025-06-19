# Clustera YouTube Ingest

A robust YouTube data ingestion pipeline for the Clustera platform, designed to extract video metadata and transcripts for storage in PostgreSQL.

## Features

- **Stage 0**: Database schema initialization and management
- **Stage 1**: YouTube source discovery and sync orchestration  
- **Stage 2**: Video metadata extraction from channels/playlists
- **Stage 3**: Video transcript ingestion and processing
- **CLI Interface**: Complete command-line tool for all operations
- **SDK**: Programmatic interface for integration

## Quick Start

### Installation

```bash
# Install UV (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install the package
uv sync
```

### Environment Setup

```bash
# Copy environment template
cp .env.example .env

# Edit with your configuration
export CLUSTERA_DATABASE_URL="postgresql://user:pass@localhost:5432/clustera"
export APIFY_TOKEN="your_apify_token"
```

### Initialize Database

```bash
uv run clustera-youtube-ingest init
```

### Add YouTube Sources

```bash
# Add a channel
uv run clustera-youtube-ingest add-source --url "https://www.youtube.com/@channelname"

# Add a playlist
uv run clustera-youtube-ingest add-source --url "https://www.youtube.com/playlist?list=PLxxxxxx"
```

### Sync Sources

```bash
# Sync all sources
uv run clustera-youtube-ingest sync --all

# Sync specific source
uv run clustera-youtube-ingest sync --source-id 1
```

### Ingest Videos

```bash
# Ingest from specific URL
uv run clustera-youtube-ingest ingest --url "https://www.youtube.com/@channelname"

# Process transcripts
uv run clustera-youtube-ingest transcripts --missing-only --limit 50
```

## CLI Commands

- `init` - Initialize database schema
- `status` - Check system status
- `add-source` - Add YouTube source for monitoring
- `list-sources` - List configured sources
- `remove-source` - Remove a source
- `sync` - Sync sources (Stage 1)
- `ingest` - Ingest videos from URL (Stage 2)
- `transcripts` - Process video transcripts (Stage 3)
- `stats` - Show ingestion statistics

## Architecture

The system follows a multi-stage pipeline architecture:

1. **Stage 0**: Database initialization and schema management
2. **Stage 1**: Source synchronization and orchestration
3. **Stage 2**: Video metadata extraction using Apify actors
4. **Stage 3**: Transcript extraction and processing

## Configuration

Key environment variables:

- `CLUSTERA_DATABASE_URL` - PostgreSQL connection string
- `APIFY_TOKEN` - Apify API token for data extraction
- `MAX_CONCURRENT_SYNCS` - Maximum concurrent sync operations
- `BATCH_SIZE_VIDEOS` - Video processing batch size

## Development

```bash
# Install development dependencies
uv sync --dev

# Run tests
uv run pytest

# Format code
uv run black .
uv run isort .
```

## License

MIT License - see LICENSE file for details. 