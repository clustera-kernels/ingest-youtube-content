# Migration Management Guide

This guide covers how to use Alembic migrations with the Clustera YouTube Ingest project.

## Overview

We've migrated from manual SQL files to Alembic for version-controlled database schema management. This provides:

- **Version Control**: Track all schema changes with revision history
- **Forward/Backward Migrations**: Upgrade and downgrade database schemas safely  
- **Auto-generation**: Automatically detect model changes and generate migrations
- **Environment Support**: Different configurations for development, staging, and production

## Quick Start

### 1. Check Migration Status

```bash
# Check current migration status
uv run clustera-youtube-ingest migrate status

# Output shows current revision, pending migrations, etc.
```

### 2. Upgrade Database

```bash
# Upgrade to latest migration
uv run clustera-youtube-ingest migrate upgrade

# This is equivalent to the old 'init' command
```

### 3. View Migration History

```bash
# See all available migrations
uv run clustera-youtube-ingest migrate history
```

## Migration Commands

### Status Commands

```bash
# Check migration status
uv run clustera-youtube-ingest migrate status

# Shows:
# - Current revision in database
# - Latest available revision  
# - Pending migrations
# - Schema existence
```

### Upgrade Commands

```bash
# Upgrade to latest migration
uv run clustera-youtube-ingest migrate upgrade

# The system automatically handles:
# - Fresh databases (runs all migrations)
# - Existing databases (stamps then upgrades)
# - Already up-to-date databases (no action)
```

### Development Commands

```bash
# Create a new migration (auto-generate from model changes)
uv run clustera-youtube-ingest migrate create -m "Add new column to videos table"

# Create a blank migration (manual changes)
uv run clustera-youtube-ingest migrate create -m "Custom data migration" --no-autogenerate

# View migration history
uv run clustera-youtube-ingest migrate history

# Stamp database (mark as specific revision without running migrations)
uv run clustera-youtube-ingest migrate stamp --revision head
```

## Migration Scenarios

### Fresh Database Setup

For a completely new database:

```bash
# 1. Set up environment variables
export CLUSTERA_DATABASE_URL="postgresql://user:pass@localhost:5432/clustera"

# 2. Run initial migration (creates all tables)
uv run clustera-youtube-ingest init
# OR
uv run clustera-youtube-ingest migrate upgrade
```

### Existing Database (Pre-Alembic)

For databases created with the old SQL file method:

```bash
# 1. Check status (should show existing schema without Alembic version table)
uv run clustera-youtube-ingest migrate status

# 2. Run init/upgrade (automatically stamps existing database)
uv run clustera-youtube-ingest init
# OR  
uv run clustera-youtube-ingest migrate upgrade
```

### Development Workflow

When you modify models in `models.py`:

```bash
# 1. Create auto-generated migration
uv run clustera-youtube-ingest migrate create -m "Add user preferences table"

# 2. Review the generated migration file in migrations/versions/

# 3. Apply the migration
uv run clustera-youtube-ingest migrate upgrade

# 4. Test your changes
```

## File Structure

```
clustera-youtube-ingest/
├── alembic.ini                    # Alembic configuration
├── migrations/
│   ├── env.py                     # Alembic environment setup
│   ├── script.py.mako            # Migration template
│   └── versions/                  # Individual migration files
│       └── 20241220_initial_schema.py
├── src/clustera_youtube_ingest/
│   ├── migration_manager.py      # Migration management integration
│   ├── models.py                 # SQLAlchemy models (source of truth)
│   └── database.py               # Updated to use Alembic
└── 000-create-tables.sql         # Legacy SQL file (kept for reference)
```

## Environment Configuration

### Required Environment Variables

```bash
# Database connection (required)
export CLUSTERA_DATABASE_URL="postgresql://user:pass@localhost:5432/clustera"

# Optional: Override Alembic configuration
export ALEMBIC_CONFIG="path/to/custom/alembic.ini"
```

### Database URL Formats

The system automatically handles different PostgreSQL URL formats:

```bash
# These are all equivalent:
postgresql://user:pass@host:5432/dbname
postgres://user:pass@host:5432/dbname
postgresql+psycopg2://user:pass@host:5432/dbname
```

## Migration Best Practices

### 1. Model Changes

Always modify `models.py` first, then generate migrations:

```python
# Add new column to existing model
class DatasetYouTubeVideo(Base):
    # ... existing columns ...
    new_column = Column(String(100), nullable=True)  # New column
```

```bash
# Generate migration
uv run clustera-youtube-ingest migrate create -m "Add new_column to videos"
```

### 2. Review Generated Migrations

Always review auto-generated migrations before applying:

```python
# migrations/versions/xxx_add_new_column_to_videos.py
def upgrade() -> None:
    # Review this carefully
    op.add_column('dataset_youtube_video', sa.Column('new_column', sa.String(100), nullable=True))

def downgrade() -> None:
    # Make sure downgrade works
    op.drop_column('dataset_youtube_video', 'new_column')
```

### 3. Data Migrations

For complex data migrations, create custom migrations:

```bash
# Create blank migration
uv run clustera-youtube-ingest migrate create -m "Migrate old data format" --no-autogenerate
```

```python
# Edit the migration file manually
def upgrade() -> None:
    # Custom data migration logic
    connection = op.get_bind()
    connection.execute("UPDATE table SET column = 'new_value' WHERE condition")

def downgrade() -> None:
    # Reverse data migration
    connection = op.get_bind()
    connection.execute("UPDATE table SET column = 'old_value' WHERE condition")
```

### 4. Backup Before Major Changes

```bash
# Backup database before major migrations
pg_dump $CLUSTERA_DATABASE_URL > backup_before_migration.sql

# Run migration
uv run clustera-youtube-ingest migrate upgrade

# If issues occur, restore from backup
psql $CLUSTERA_DATABASE_URL < backup_before_migration.sql
```

## Troubleshooting

### Common Issues

#### 1. "alembic_version table doesn't exist"

This happens when you have an existing database created before Alembic:

```bash
# Solution: Run init or migrate upgrade (auto-stamps existing database)
uv run clustera-youtube-ingest init
```

#### 2. "Migration fails with table already exists"

Usually happens when trying to run initial migration on existing database:

```bash
# Check status first
uv run clustera-youtube-ingest migrate status

# If needed, stamp the database
uv run clustera-youtube-ingest migrate stamp --revision 001_initial_schema
```

#### 3. "Can't locate revision"

Migration files are missing or corrupted:

```bash
# Check migration history
uv run clustera-youtube-ingest migrate history

# Recreate missing migrations or restore from backup
```

### Debug Commands

```bash
# Check detailed migration status
uv run clustera-youtube-ingest migrate status

# View all available migrations
uv run clustera-youtube-ingest migrate history

# Check database status (includes migration info)
uv run clustera-youtube-ingest status
```

## Integration with Existing Code

### Backward Compatibility

The existing `init` command still works and now uses Alembic under the hood:

```bash
# Still works (now uses Alembic)
uv run clustera-youtube-ingest init

# Equivalent to
uv run clustera-youtube-ingest migrate upgrade
```

### SDK Integration

The `YouTubeIngestor.init_database()` method now uses Alembic:

```python
from clustera_youtube_ingest import YouTubeIngestor

ingestor = YouTubeIngestor()
result = ingestor.init_database()  # Now uses Alembic migrations

# Result includes migration information
print(f"Migration method: {result['migration_method']}")  # "alembic"
print(f"Previous revision: {result['previous_revision']}")
print(f"New revision: {result['new_revision']}")
```

## Advanced Usage

### Manual Alembic Commands

You can also use Alembic directly:

```bash
# Set environment variable
export CLUSTERA_DATABASE_URL="postgresql://..."

# Use Alembic directly
cd clustera-youtube-ingest
uv run alembic current
uv run alembic upgrade head
uv run alembic history
uv run alembic downgrade -1
```

### Custom Migration Template

Modify `migrations/script.py.mako` to customize migration file template.

### Multiple Environments

Create different `alembic.ini` files for different environments:

```bash
# Development
uv run alembic -c alembic-dev.ini upgrade head

# Production  
uv run alembic -c alembic-prod.ini upgrade head
```

## Migration History

### Version 1: Initial Schema (001_initial_schema)

- Creates all initial tables: `ctrl_youtube_lists`, `dataset_youtube_video`, `dataset_youtube_channel`, `ctrl_ingestion_log`
- Creates all indexes including GIN index for full-text search
- Replaces the original `000-create-tables.sql`

### Future Migrations

All future schema changes will be tracked as individual migration files with proper version control. 