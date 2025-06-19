"""
Command-line interface for Clustera YouTube Ingest.

Provides CLI commands for database initialization and ingestion operations.
"""

import sys
import os
from pathlib import Path
from typing import Optional

import click
from dotenv import load_dotenv

from .sdk import YouTubeIngestor
from .database import DatabaseManager


def load_environment() -> None:
    """Load environment variables from .env file if it exists."""
    # Look for .env file in current directory and parent directories
    current_dir = Path.cwd()
    for path in [current_dir] + list(current_dir.parents):
        env_file = path / '.env'
        if env_file.exists():
            load_dotenv(env_file)
            click.echo(f"Loaded environment from: {env_file}")
            return
    
    # Also check for .env in the project root
    project_root = Path(__file__).parent.parent.parent.parent
    env_file = project_root / '.env'
    if env_file.exists():
        load_dotenv(env_file)
        click.echo(f"Loaded environment from: {env_file}")


@click.group()
@click.version_option(version="0.1.0", prog_name="clustera-youtube-ingest")
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose logging')
def main(verbose: bool) -> None:
    """
    Clustera YouTube Ingest - YouTube data ingestion pipeline.
    
    Extract YouTube video metadata and transcripts for storage in PostgreSQL.
    """
    # Load environment variables
    load_environment()
    
    if verbose:
        import logging
        logging.getLogger().setLevel(logging.DEBUG)
        click.echo("Verbose logging enabled")


@main.command()
@click.option(
    '--database-url', 
    envvar='CLUSTERA_DATABASE_URL',
    help='PostgreSQL connection string (or set CLUSTERA_DATABASE_URL env var)'
)
@click.option(
    '--check-only', 
    is_flag=True, 
    help='Only check database status without initializing'
)
def init(database_url: Optional[str], check_only: bool) -> None:
    """
    Initialize database schema (Stage 0).
    
    Creates all required tables and indexes for YouTube data ingestion.
    """
    click.echo("🚀 Clustera YouTube Ingest - Database Initialization")
    click.echo("=" * 60)
    
    try:
        # Validate environment first
        ingestor = YouTubeIngestor(database_url)
        env_validation = ingestor.validate_environment()
        
        click.echo("\n📋 Environment Validation:")
        if env_validation["valid"]:
            click.echo(click.style("✅ All required environment variables configured", fg='green'))
            for var in env_validation["configured_vars"]:
                click.echo(f"   ✓ {var}")
        else:
            click.echo(click.style("❌ Missing required environment variables:", fg='red'))
            for missing in env_validation["missing_required"]:
                click.echo(f"   ✗ {missing['name']}: {missing['description']}")
            
            if env_validation["missing_optional"]:
                click.echo("\n⚠️  Optional environment variables:")
                for missing in env_validation["missing_optional"]:
                    click.echo(f"   - {missing['name']}: {missing['description']}")
            
            if not env_validation["valid"]:
                click.echo("\nPlease configure required environment variables and try again.")
                sys.exit(1)
        
        # Check database status
        click.echo("\n🔍 Database Status Check:")
        status = ingestor.get_database_status()
        
        if status["error"]:
            click.echo(click.style(f"❌ Database error: {status['error']}", fg='red'))
            sys.exit(1)
        
        if status["connected"]:
            click.echo(click.style("✅ Database connection successful", fg='green'))
            if status["postgresql_version"]:
                # Extract just the version number for cleaner display
                version_parts = status["postgresql_version"].split()
                if len(version_parts) >= 2:
                    version = version_parts[1]
                    click.echo(f"   📊 PostgreSQL version: {version}")
        else:
            click.echo(click.style("❌ Database connection failed", fg='red'))
            sys.exit(1)
        
        # Show schema status
        if status["schema_initialized"]:
            click.echo(click.style("✅ Database schema already initialized", fg='green'))
            click.echo(f"   📁 Tables found: {len(status['tables_exist'])}")
            for table in status["tables_exist"]:
                click.echo(f"      ✓ {table}")
        else:
            click.echo(click.style("⚠️  Database schema not initialized", fg='yellow'))
            if status["tables_exist"]:
                click.echo(f"   📁 Existing tables: {len(status['tables_exist'])}")
                for table in status["tables_exist"]:
                    click.echo(f"      ✓ {table}")
            if status["missing_tables"]:
                click.echo(f"   📁 Missing tables: {len(status['missing_tables'])}")
                for table in status["missing_tables"]:
                    click.echo(f"      ✗ {table}")
        
        # If check-only mode, exit here
        if check_only:
            click.echo("\n✅ Database status check completed")
            return
        
        # Proceed with initialization if needed
        if status["schema_initialized"]:
            click.echo("\n✅ Database already initialized - no action needed")
            return
        
        # Confirm initialization
        if not click.confirm("\n🔧 Initialize database schema?"):
            click.echo("Initialization cancelled")
            return
        
        # Perform initialization
        click.echo("\n🔧 Initializing database schema...")
        with click.progressbar(length=100, label='Creating tables and indexes') as bar:
            results = ingestor.init_database()
            bar.update(100)
        
        # Display results
        if results["success"]:
            click.echo(click.style("\n🎉 Database initialization completed successfully!", fg='green'))
            click.echo(f"   📊 Duration: {results['duration_seconds']:.2f} seconds")
            
            if results["tables_created"]:
                click.echo(f"   📁 Tables created: {len(results['tables_created'])}")
                for table in results["tables_created"]:
                    click.echo(f"      ✓ {table}")
            
            if results["indexes_created"]:
                click.echo(f"   🔍 Indexes created: {len(results['indexes_created'])}")
                for index in results["indexes_created"]:
                    click.echo(f"      ✓ {index}")
            
            click.echo("\n✅ Ready for YouTube data ingestion!")
            
        else:
            click.echo(click.style("\n❌ Database initialization failed", fg='red'))
            if results["errors"]:
                for error in results["errors"]:
                    click.echo(f"   Error: {error}")
            sys.exit(1)
    
    except KeyboardInterrupt:
        click.echo("\n\n⚠️  Initialization cancelled by user")
        sys.exit(1)
    except Exception as e:
        click.echo(click.style(f"\n❌ Initialization failed: {e}", fg='red'))
        sys.exit(1)
    finally:
        # Always close connections
        try:
            ingestor.close()
        except:
            pass


@main.command()
def status() -> None:
    """Check system status and configuration."""
    click.echo("📊 Clustera YouTube Ingest - System Status")
    click.echo("=" * 50)
    
    try:
        ingestor = YouTubeIngestor()
        
        # Environment validation
        click.echo("\n📋 Environment Configuration:")
        env_validation = ingestor.validate_environment()
        
        if env_validation["configured_vars"]:
            click.echo("   Configured variables:")
            for var in env_validation["configured_vars"]:
                click.echo(f"      ✓ {var}")
        
        if env_validation["missing_required"]:
            click.echo("   Missing required variables:")
            for missing in env_validation["missing_required"]:
                click.echo(f"      ✗ {missing['name']}")
        
        if env_validation["missing_optional"]:
            click.echo("   Missing optional variables:")
            for missing in env_validation["missing_optional"]:
                click.echo(f"      - {missing['name']}")
        
        # Database status
        click.echo("\n🔍 Database Status:")
        db_status = ingestor.get_database_status()
        
        if db_status["error"]:
            click.echo(click.style(f"   ❌ Error: {db_status['error']}", fg='red'))
        else:
            if db_status["connected"]:
                click.echo(click.style("   ✅ Connected", fg='green'))
                if db_status["postgresql_version"]:
                    version_parts = db_status["postgresql_version"].split()
                    if len(version_parts) >= 2:
                        click.echo(f"   📊 Version: {version_parts[1]}")
            else:
                click.echo(click.style("   ❌ Not connected", fg='red'))
            
            if db_status["schema_initialized"]:
                click.echo(click.style("   ✅ Schema initialized", fg='green'))
            else:
                click.echo(click.style("   ⚠️  Schema not initialized", fg='yellow'))
            
            if db_status["tables_exist"]:
                click.echo(f"   📁 Tables: {len(db_status['tables_exist'])}/{len(db_status['tables_exist']) + len(db_status['missing_tables'])}")
        
        # Overall status
        click.echo("\n🎯 Overall Status:")
        if env_validation["valid"] and db_status["connected"] and db_status["schema_initialized"]:
            click.echo(click.style("   ✅ System ready for ingestion", fg='green'))
        else:
            click.echo(click.style("   ⚠️  System requires setup", fg='yellow'))
            click.echo("   Run 'clustera-youtube-ingest init' to initialize")
    
    except Exception as e:
        click.echo(click.style(f"❌ Status check failed: {e}", fg='red'))
        sys.exit(1)
    finally:
        try:
            ingestor.close()
        except:
            pass


# Stage 1: Source Management Commands

@main.command('add-source')
@click.option(
    '--url', 
    required=True,
    help='YouTube channel or playlist URL'
)
@click.option(
    '--name',
    help='Custom name for the source (optional)'
)
@click.option(
    '--sync-hours',
    type=int,
    default=24,
    help='Sync frequency in hours (1-168, default: 24)'
)
@click.option(
    '--resource-pool',
    help='Resource pool identifier for processing isolation'
)
@click.option(
    '--database-url', 
    envvar='CLUSTERA_DATABASE_URL',
    help='PostgreSQL connection string (or set CLUSTERA_DATABASE_URL env var)'
)
def add_source(url: str, name: Optional[str], sync_hours: int, resource_pool: Optional[str], database_url: Optional[str]) -> None:
    """Add a new YouTube source for monitoring."""
    click.echo("📺 Clustera YouTube Ingest - Add Source")
    click.echo("=" * 45)
    
    try:
        import asyncio
        
        async def add_source_async():
            ingestor = YouTubeIngestor(database_url)
            try:
                click.echo(f"\n🔍 Adding source: {url}")
                if name:
                    click.echo(f"   📝 Name: {name}")
                click.echo(f"   ⏰ Sync frequency: {sync_hours} hours")
                if resource_pool:
                    click.echo(f"   🏷️  Resource pool: {resource_pool}")
                
                result = await ingestor.add_source(url, name, sync_hours, resource_pool)
                
                if result["success"]:
                    click.echo(click.style("\n✅ Source added successfully!", fg='green'))
                    source = result["source"]
                    click.echo(f"   🆔 Source ID: {source['id']}")
                    click.echo(f"   📺 Type: {source['source_type']}")
                    click.echo(f"   📝 Name: {source['source_name']}")
                    click.echo(f"   🔗 URL: {source['source_url']}")
                    click.echo(f"   ⏰ Sync frequency: {source['sync_frequency_hours']} hours")
                    if source.get('resource_pool'):
                        click.echo(f"   🏷️  Resource pool: {source['resource_pool']}")
                else:
                    click.echo(click.style(f"\n❌ Failed to add source: {result['error']}", fg='red'))
                    if result.get("existing_source"):
                        existing = result["existing_source"]
                        click.echo(f"   Existing source ID: {existing['id']}")
                    sys.exit(1)
                    
            finally:
                ingestor.close()
        
        asyncio.run(add_source_async())
        
    except KeyboardInterrupt:
        click.echo("\n\n⚠️  Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        click.echo(click.style(f"\n❌ Failed to add source: {e}", fg='red'))
        sys.exit(1)


@main.command('list-sources')
@click.option(
    '--all',
    'show_all',
    is_flag=True,
    help='Show all sources including inactive ones'
)
@click.option(
    '--database-url', 
    envvar='CLUSTERA_DATABASE_URL',
    help='PostgreSQL connection string (or set CLUSTERA_DATABASE_URL env var)'
)
def list_sources(show_all: bool, database_url: Optional[str]) -> None:
    """List YouTube sources."""
    click.echo("📺 Clustera YouTube Ingest - List Sources")
    click.echo("=" * 47)
    
    try:
        import asyncio
        
        async def list_sources_async():
            ingestor = YouTubeIngestor(database_url)
            try:
                sources = await ingestor.list_sources(active_only=not show_all)
                
                if not sources:
                    click.echo("\n📭 No sources found")
                    click.echo("   Use 'clustera-youtube-ingest add-source --url <URL>' to add sources")
                    return
                
                click.echo(f"\n📋 Found {len(sources)} source(s):")
                click.echo()
                
                for source in sources:
                    status_icon = "✅" if source["is_active"] else "❌"
                    type_icon = "📺" if source["source_type"] == "channel" else "📋"
                    
                    click.echo(f"{status_icon} {type_icon} {source['source_name']}")
                    click.echo(f"   🆔 ID: {source['id']}")
                    click.echo(f"   🔗 URL: {source['source_url']}")
                    click.echo(f"   ⏰ Sync: every {source['sync_frequency_hours']} hours")
                    if source.get('resource_pool'):
                        click.echo(f"   🏷️  Resource pool: {source['resource_pool']}")
                    
                    if source["last_sync_at"]:
                        click.echo(f"   🕐 Last sync: {source['last_sync_at']}")
                    else:
                        click.echo(f"   🕐 Last sync: Never")
                    
                    click.echo(f"   📅 Created: {source['created_at']}")
                    click.echo()
                    
            finally:
                ingestor.close()
        
        asyncio.run(list_sources_async())
        
    except KeyboardInterrupt:
        click.echo("\n\n⚠️  Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        click.echo(click.style(f"\n❌ Failed to list sources: {e}", fg='red'))
        sys.exit(1)


@main.command('remove-source')
@click.option(
    '--id',
    'source_id',
    type=int,
    required=True,
    help='ID of the source to remove'
)
@click.option(
    '--database-url', 
    envvar='CLUSTERA_DATABASE_URL',
    help='PostgreSQL connection string (or set CLUSTERA_DATABASE_URL env var)'
)
def remove_source(source_id: int, database_url: Optional[str]) -> None:
    """Remove a YouTube source from monitoring."""
    click.echo("📺 Clustera YouTube Ingest - Remove Source")
    click.echo("=" * 48)
    
    try:
        import asyncio
        
        async def remove_source_async():
            ingestor = YouTubeIngestor(database_url)
            try:
                click.echo(f"\n🗑️  Removing source ID: {source_id}")
                
                # Confirm removal
                if not click.confirm("Are you sure you want to remove this source?"):
                    click.echo("Removal cancelled")
                    return
                
                result = await ingestor.remove_source(source_id)
                
                if result["success"]:
                    click.echo(click.style("\n✅ Source removed successfully!", fg='green'))
                    removed = result["removed_source"]
                    click.echo(f"   📝 Name: {removed['source_name']}")
                    click.echo(f"   🔗 URL: {removed['source_url']}")
                else:
                    click.echo(click.style(f"\n❌ Failed to remove source: {result['error']}", fg='red'))
                    sys.exit(1)
                    
            finally:
                ingestor.close()
        
        asyncio.run(remove_source_async())
        
    except KeyboardInterrupt:
        click.echo("\n\n⚠️  Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        click.echo(click.style(f"\n❌ Failed to remove source: {e}", fg='red'))
        sys.exit(1)


@main.command('sync')
@click.option(
    '--all',
    'sync_all',
    is_flag=True,
    help='Sync all sources due for synchronization'
)
@click.option(
    '--source-id',
    type=int,
    help='Sync specific source by ID'
)
@click.option(
    '--dry-run',
    is_flag=True,
    help='Show what would be synced without actually syncing'
)
@click.option(
    '--database-url', 
    envvar='CLUSTERA_DATABASE_URL',
    help='PostgreSQL connection string (or set CLUSTERA_DATABASE_URL env var)'
)
def sync(sync_all: bool, source_id: Optional[int], dry_run: bool, database_url: Optional[str]) -> None:
    """Sync YouTube sources."""
    if not sync_all and source_id is None:
        click.echo("❌ Must specify either --all or --source-id")
        sys.exit(1)
    
    if sync_all and source_id is not None:
        click.echo("❌ Cannot specify both --all and --source-id")
        sys.exit(1)
    
    click.echo("🔄 Clustera YouTube Ingest - Sync Sources")
    click.echo("=" * 47)
    
    try:
        import asyncio
        
        async def sync_async():
            ingestor = YouTubeIngestor(database_url)
            try:
                if sync_all:
                    click.echo(f"\n🔄 Syncing all eligible sources (dry_run={dry_run})")
                    result = await ingestor.sync_all_sources(dry_run)
                    
                    if dry_run:
                        if result.get("eligible_sources"):
                            click.echo(f"\n📋 Would sync {len(result['eligible_sources'])} source(s):")
                            for source in result["eligible_sources"]:
                                click.echo(f"   📺 {source['source_name']} (ID: {source['id']})")
                                click.echo(f"      Reason: {source.get('eligible_reason', 'Due for sync')}")
                        else:
                            click.echo("\n📭 No sources due for sync")
                    else:
                        click.echo(f"\n📊 Sync Results:")
                        click.echo(f"   ✅ Successful: {result['sources_successful']}")
                        click.echo(f"   ❌ Failed: {result['sources_failed']}")
                        click.echo(f"   ⏭️  Skipped: {result['sources_skipped']}")
                        click.echo(f"   ⏱️  Duration: {result.get('duration_seconds', 0):.2f} seconds")
                        
                        if result.get("errors"):
                            click.echo("\n❌ Errors:")
                            for error in result["errors"]:
                                click.echo(f"   Source {error['source_id']}: {error['error']}")
                
                else:
                    click.echo(f"\n🔄 Syncing source ID: {source_id}")
                    result = await ingestor.sync_source(source_id)
                    
                    if result["success"]:
                        click.echo(click.style("\n✅ Source synced successfully!", fg='green'))
                        click.echo(f"   📺 URL: {result.get('source_url', 'Unknown')}")
                        click.echo(f"   ⏱️  Duration: {result.get('duration_seconds', 0):.2f} seconds")
                    else:
                        click.echo(click.style(f"\n❌ Sync failed: {result.get('error', 'Unknown error')}", fg='red'))
                        if result.get("skipped"):
                            click.echo("   ⏭️  Source was skipped")
                        sys.exit(1)
                    
            finally:
                ingestor.close()
        
        asyncio.run(sync_async())
        
    except KeyboardInterrupt:
        click.echo("\n\n⚠️  Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        click.echo(click.style(f"\n❌ Sync failed: {e}", fg='red'))
        sys.exit(1)


@main.command('ingest')
@click.option(
    '--url',
    required=True,
    help='YouTube channel or playlist URL to ingest'
)
@click.option(
    '--resource-pool',
    required=True,
    help='Resource pool identifier for processing isolation (required)'
)
@click.option(
    '--database-url', 
    envvar='CLUSTERA_DATABASE_URL',
    help='PostgreSQL connection string (or set CLUSTERA_DATABASE_URL env var)'
)
def ingest(url: str, resource_pool: str, database_url: Optional[str]) -> None:
    """Ingest videos from a YouTube channel or playlist (Stage 2)."""
    click.echo("📺 Clustera YouTube Ingest - List Ingestion")
    click.echo("=" * 49)
    
    try:
        import asyncio
        
        async def ingest_async():
            ingestor = YouTubeIngestor(database_url)
            try:
                click.echo(f"\n🔄 Starting ingestion for: {url}")
                click.echo(f"🏷️  Resource pool: {resource_pool}")
                
                # Validate URL first
                from .url_utils import YouTubeURLParser
                if not YouTubeURLParser.validate_url(url):
                    click.echo(click.style(f"❌ Invalid YouTube URL: {url}", fg='red'))
                    sys.exit(1)
                
                source_type = YouTubeURLParser.get_source_type(url)
                click.echo(f"📋 Source type: {source_type.value}")
                
                # Run ingestion
                with click.progressbar(length=100, label='Ingesting videos') as bar:
                    new_video_ids = await ingestor.ingest_source(url, resource_pool=resource_pool)
                    bar.update(100)
                
                click.echo(click.style(f"\n✅ Ingestion completed successfully!", fg='green'))
                click.echo(f"   📊 New videos found: {len(new_video_ids)}")
                
                if new_video_ids:
                    click.echo(f"   🎬 Video IDs: {', '.join(new_video_ids[:5])}")
                    if len(new_video_ids) > 5:
                        click.echo(f"      ... and {len(new_video_ids) - 5} more")
                    
                    # Ask if user wants to ingest transcripts
                    if click.confirm("\nWould you like to ingest transcripts for new videos?"):
                        click.echo("\n📝 Starting transcript ingestion...")
                        
                        # Use batch processing for better performance
                        try:
                            result = await ingestor.process_transcript_queue(
                                new_video_ids, 
                                source_identifier=f"auto_from_ingest_{url}"
                            )
                            
                            stats = result['statistics']
                            click.echo(f"\n📊 Transcript Results:")
                            click.echo(f"   ✅ Successful: {stats['successful']}")
                            click.echo(f"   ❌ Failed: {stats['failed']}")
                            click.echo(f"   🚫 Unavailable: {stats['unavailable']}")
                            click.echo(f"   🔍 Quality rejected: {stats['quality_rejected']}")
                            click.echo(f"   📈 Success rate: {result['success_rate']:.1f}%")
                            click.echo(f"   ⏱️  Processing time: {result['processing_time']:.1f}s")
                            
                        except Exception as e:
                            click.echo(f"\n⚠️  Transcript processing failed: {e}")
                            # Fallback to individual processing
                            click.echo("   🔄 Falling back to individual processing...")
                            
                            successful_transcripts = 0
                            with click.progressbar(new_video_ids, label='Processing transcripts') as video_ids:
                                for video_id in video_ids:
                                    try:
                                        success = await ingestor.ingest_video_transcript(video_id)
                                        if success:
                                            successful_transcripts += 1
                                    except Exception as e:
                                        click.echo(f"\n⚠️  Failed to process transcript for {video_id}: {e}")
                            
                            click.echo(f"\n📊 Fallback Transcript Results:")
                            click.echo(f"   ✅ Successful: {successful_transcripts}")
                            click.echo(f"   ❌ Failed: {len(new_video_ids) - successful_transcripts}")
                else:
                    click.echo("   ℹ️  No new videos found (all videos already in database)")
                    
            finally:
                ingestor.close()
        
        asyncio.run(ingest_async())
        
    except KeyboardInterrupt:
        click.echo("\n\n⚠️  Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        click.echo(click.style(f"\n❌ Ingestion failed: {e}", fg='red'))
        sys.exit(1)


@main.command('transcripts')
@click.option(
    '--video-id',
    help='Specific video ID to process'
)
@click.option(
    '--missing-only',
    is_flag=True,
    default=True,
    help='Only process videos without transcripts (default: true)'
)
@click.option(
    '--limit',
    type=int,
    default=20,
    help='Maximum number of videos to process (default: 20)'
)
@click.option(
    '--source-id',
    type=int,
    help='Filter by specific source list ID'
)
@click.option(
    '--resource-pool',
    required=True,
    help='Resource pool identifier for processing isolation (required)'
)
@click.option(
    '--batch-mode',
    is_flag=True,
    help='Use optimized batch processing'
)
@click.option(
    '--show-stats',
    is_flag=True,
    help='Show detailed statistics after processing'
)
@click.option(
    '--database-url', 
    envvar='CLUSTERA_DATABASE_URL',
    help='PostgreSQL connection string (or set CLUSTERA_DATABASE_URL env var)'
)
def transcripts(video_id: Optional[str], missing_only: bool, limit: int, source_id: Optional[int], 
               resource_pool: str, batch_mode: bool, show_stats: bool, database_url: Optional[str]) -> None:
    """Ingest transcripts for videos (Stage 3)."""
    click.echo("📝 Clustera YouTube Ingest - Transcript Ingestion")
    click.echo("=" * 54)
    
    try:
        import asyncio
        
        async def transcripts_async():
            ingestor = YouTubeIngestor(database_url)
            try:
                if video_id:
                    # Process specific video
                    click.echo(f"\n📝 Processing transcript for video: {video_id}")
                    
                    success = await ingestor.ingest_video_transcript(video_id)
                    
                    if success:
                        click.echo(click.style("✅ Transcript ingested successfully!", fg='green'))
                    else:
                        click.echo(click.style("❌ Failed to ingest transcript", fg='red'))
                        sys.exit(1)
                
                else:
                    # Process multiple videos
                    click.echo(f"\n🔍 Finding videos for transcript processing...")
                    click.echo(f"   📊 Limit: {limit} videos")
                    click.echo(f"   🎯 Missing only: {missing_only}")
                    click.echo(f"   🏷️  Source filter: {source_id if source_id else 'All sources'}")
                    click.echo(f"   🏷️  Resource pool: {resource_pool}")
                    click.echo(f"   ⚡ Batch mode: {batch_mode}")
                    
                    # Get videos that need transcript processing
                    if missing_only:
                        video_ids = await ingestor.get_videos_needing_transcripts(limit, source_id)
                    else:
                        # Get all videos for the source
                        with ingestor.db_manager.get_session() as session:
                            from .models import DatasetYouTubeVideo
                            
                            query = session.query(DatasetYouTubeVideo.video_id)
                            if source_id:
                                query = query.filter(DatasetYouTubeVideo.source_list_id == source_id)
                            
                            video_ids = [row.video_id for row in query.limit(limit).all()]
                    
                    if not video_ids:
                        click.echo("📭 No videos found for transcript processing")
                        return
                    
                    click.echo(f"📋 Found {len(video_ids)} video(s) to process")
                    
                    if batch_mode:
                        # Use optimized batch processing
                        click.echo("\n⚡ Starting batch transcript processing...")
                        
                        source_identifier = f"source_{source_id}" if source_id else "manual_batch"
                        result = await ingestor.process_transcript_queue(video_ids, source_identifier)
                        
                        stats = result['statistics']
                        click.echo(f"\n📊 Batch Processing Results:")
                        click.echo(f"   ✅ Successful: {stats['successful']}")
                        click.echo(f"   ❌ Failed: {stats['failed']}")
                        click.echo(f"   🚫 Unavailable: {stats['unavailable']}")
                        click.echo(f"   🔍 Quality rejected: {stats['quality_rejected']}")
                        click.echo(f"   ⏭️  Already processed: {stats['already_processed']}")
                        click.echo(f"   📈 Success rate: {result['success_rate']:.1f}%")
                        click.echo(f"   ⏱️  Processing time: {result['processing_time']:.1f}s")
                        
                        if stats['errors']:
                            click.echo(f"\n⚠️  Errors encountered:")
                            for error in stats['errors'][:5]:  # Show first 5 errors
                                click.echo(f"   • {error['video_id']}: {error['error']}")
                            if len(stats['errors']) > 5:
                                click.echo(f"   ... and {len(stats['errors']) - 5} more errors")
                    
                    else:
                        # Individual processing with progress bar
                        successful = 0
                        failed = 0
                        unavailable = 0
                        
                        with click.progressbar(video_ids, label='Processing transcripts') as videos:
                            for vid_id in videos:
                                try:
                                    success = await ingestor.ingest_video_transcript(vid_id)
                                    if success:
                                        successful += 1
                                    else:
                                        failed += 1
                                except Exception as e:
                                    failed += 1
                                    click.echo(f"\n⚠️  Error processing {vid_id}: {e}")
                        
                        click.echo(f"\n📊 Individual Processing Results:")
                        click.echo(f"   ✅ Successful: {successful}")
                        click.echo(f"   ❌ Failed: {failed}")
                        click.echo(f"   📈 Success rate: {(successful / len(video_ids) * 100):.1f}%")
                
                # Show detailed statistics if requested
                if show_stats:
                    click.echo(f"\n📈 Detailed Transcript Statistics:")
                    stats = await ingestor.get_transcript_statistics(source_id)
                    
                    click.echo(f"   📹 Total videos: {stats.get('total_videos', 0)}")
                    click.echo(f"   ✅ With transcripts: {stats.get('videos_with_transcripts', 0)}")
                    click.echo(f"   🚫 Unavailable: {stats.get('videos_unavailable', 0)}")
                    click.echo(f"   ⏳ Unprocessed: {stats.get('videos_unprocessed', 0)}")
                    click.echo(f"   📊 Coverage: {stats.get('coverage_percentage', 0):.1f}%")
                    click.echo(f"   🎯 Availability rate: {stats.get('availability_rate', 0):.1f}%")
                    click.echo(f"   📏 Avg length: {stats.get('average_transcript_length', 0)} chars")
                    click.echo(f"   🆕 Today: {stats.get('recent_transcripts_today', 0)} transcripts")
                    
                    # Language distribution
                    lang_dist = stats.get('language_distribution', {})
                    if lang_dist:
                        click.echo(f"   🌐 Languages: {', '.join([f'{lang}({count})' for lang, count in lang_dist.items()])}")
                    
            finally:
                ingestor.close()
        
        asyncio.run(transcripts_async())
        
    except KeyboardInterrupt:
        click.echo("\n\n⚠️  Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        click.echo(click.style(f"\n❌ Transcript processing failed: {e}", fg='red'))
        sys.exit(1)


@main.command('pipeline')
@click.option(
    '--url',
    required=True,
    help='YouTube channel or playlist URL to process'
)
@click.option(
    '--name',
    help='Custom name for the source (optional)'
)
@click.option(
    '--limit',
    type=int,
    default=10,
    help='Maximum number of videos to process (default: 10)'
)
@click.option(
    '--sync-hours',
    type=int,
    default=24,
    help='Sync frequency in hours for the source (default: 24)'
)
@click.option(
    '--resource-pool',
    required=True,
    help='Resource pool identifier for processing isolation (required)'
)
@click.option(
    '--skip-transcripts',
    is_flag=True,
    help='Skip transcript processing (Stage 3)'
)
@click.option(
    '--show-stats',
    is_flag=True,
    help='Show detailed statistics after processing'
)
@click.option(
    '--database-url', 
    envvar='CLUSTERA_DATABASE_URL',
    help='PostgreSQL connection string (or set CLUSTERA_DATABASE_URL env var)'
)
def pipeline(url: str, name: Optional[str], limit: int, sync_hours: int, resource_pool: str,
            skip_transcripts: bool, show_stats: bool, database_url: Optional[str]) -> None:
    """Run the complete ingestion pipeline for a YouTube channel or playlist.
    
    This command performs the full pipeline:
    1. Add the source to monitoring (if not already added)
    2. Ingest video metadata (Stage 2)
    3. Process transcripts (Stage 3, unless --skip-transcripts)
    """
    click.echo("🚀 Clustera YouTube Ingest - Full Pipeline")
    click.echo("=" * 48)
    
    try:
        import asyncio
        
        async def pipeline_async():
            ingestor = YouTubeIngestor(database_url)
            source_id = None
            
            try:
                click.echo(f"\n🎯 Target: {url}")
                click.echo(f"📊 Video limit: {limit}")
                click.echo(f"🏷️  Resource pool: {resource_pool}")
                click.echo(f"📝 Include transcripts: {not skip_transcripts}")
                
                # Step 1: Add source (if not already exists)
                click.echo(f"\n📋 Step 1: Adding source to monitoring...")
                
                # Check if source already exists
                if not ingestor.db_manager:
                    ingestor.db_manager = DatabaseManager(database_url)
                    ingestor.db_manager.connect()
                
                existing_source = await ingestor.db_manager.get_youtube_source_by_url(url)
                
                if existing_source:
                    source_id = existing_source['id']
                    click.echo(f"   ✅ Source already exists (ID: {source_id})")
                    click.echo(f"   📺 Name: {existing_source['source_name']}")
                else:
                    # Add new source
                    result = await ingestor.add_source(url, name, sync_hours, resource_pool)
                    source_id = result['source_id']
                    click.echo(f"   ✅ Source added successfully (ID: {source_id})")
                    click.echo(f"   📺 Name: {result['name']}")
                    click.echo(f"   🔄 Sync frequency: {sync_hours} hours")
                    click.echo(f"   🏷️  Resource pool: {resource_pool}")
                
                # Step 2: Ingest video metadata
                click.echo(f"\n📹 Step 2: Ingesting video metadata...")
                click.echo(f"   🔍 Extracting videos from: {url}")
                
                new_video_ids = await ingestor.ingest_source(url, limit)
                
                # Get total videos processed from source stats
                source_stats = await ingestor.db_manager.get_source_stats(source_id)
                total_videos = source_stats.get('total_videos', 0)
                
                click.echo(f"   ✅ Successfully processed {limit} video(s)")
                click.echo(f"   📊 Total videos in source: {total_videos}")
                click.echo(f"   🆕 New videos found: {len(new_video_ids)}")
                
                # Use new videos for transcript processing, or get recent videos if none are new
                video_ids = new_video_ids
                if not video_ids and not skip_transcripts:
                    # Get recent videos for transcript processing if no new videos
                    video_ids = await ingestor.get_videos_needing_transcripts(limit, source_id)
                    if video_ids:
                        click.echo(f"   📝 Found {len(video_ids)} videos needing transcripts")
                
                # Step 3: Process transcripts (unless skipped)
                if not skip_transcripts:
                    click.echo(f"\n📝 Step 3: Processing transcripts...")
                    click.echo(f"   ⚡ Using batch processing for {len(video_ids)} video(s)")
                    
                    source_identifier = f"pipeline_source_{source_id}"
                    result = await ingestor.process_transcript_queue(video_ids, source_identifier)
                    
                    stats = result['statistics']
                    click.echo(f"\n📊 Transcript Processing Results:")
                    click.echo(f"   ✅ Successful: {stats['successful']}")
                    click.echo(f"   ❌ Failed: {stats['failed']}")
                    click.echo(f"   🚫 Unavailable: {stats['unavailable']}")
                    click.echo(f"   🔍 Quality rejected: {stats['quality_rejected']}")
                    click.echo(f"   ⏭️  Already processed: {stats['already_processed']}")
                    click.echo(f"   📈 Success rate: {result['success_rate']:.1f}%")
                    click.echo(f"   ⏱️  Processing time: {result['processing_time']:.1f}s")
                    
                    if stats['errors']:
                        click.echo(f"\n⚠️  Transcript errors:")
                        for error in stats['errors'][:3]:  # Show first 3 errors
                            click.echo(f"   • {error['video_id']}: {error['error']}")
                        if len(stats['errors']) > 3:
                            click.echo(f"   ... and {len(stats['errors']) - 3} more errors")
                else:
                    click.echo(f"\n⏭️  Step 3: Skipped transcript processing")
                
                # Show final statistics if requested
                if show_stats and source_id:
                    click.echo(f"\n📈 Final Source Statistics:")
                    source_stats = await ingestor.db_manager.get_source_stats(source_id)
                    transcript_stats = await ingestor.get_transcript_statistics(source_id)
                    
                    click.echo(f"   📹 Total videos: {source_stats.get('total_videos', 0)}")
                    click.echo(f"   📝 With transcripts: {transcript_stats.get('videos_with_transcripts', 0)}")
                    click.echo(f"   📊 Transcript coverage: {transcript_stats.get('coverage_percentage', 0):.1f}%")
                    click.echo(f"   📏 Avg transcript length: {transcript_stats.get('average_transcript_length', 0)} chars")
                    
                    # Language distribution
                    lang_dist = transcript_stats.get('language_distribution', {})
                    if lang_dist:
                        click.echo(f"   🌐 Languages: {', '.join([f'{lang}({count})' for lang, count in lang_dist.items()])}")
                
                click.echo(f"\n🎉 Pipeline completed successfully!")
                click.echo(f"   📺 Source ID: {source_id}")
                click.echo(f"   📹 Videos in source: {total_videos}")
                click.echo(f"   🆕 New videos found: {len(new_video_ids)}")
                if not skip_transcripts:
                    click.echo(f"   📝 Transcripts processed: {len(video_ids)}")
                
            finally:
                ingestor.close()
        
        asyncio.run(pipeline_async())
        
    except KeyboardInterrupt:
        click.echo("\n\n⚠️  Pipeline cancelled by user")
        sys.exit(1)
    except Exception as e:
        click.echo(click.style(f"\n❌ Pipeline failed: {e}", fg='red'))
        sys.exit(1)


@main.command('stats')
@click.option(
    '--source-id',
    type=int,
    help='Show stats for specific source'
)
@click.option(
    '--database-url', 
    envvar='CLUSTERA_DATABASE_URL',
    help='PostgreSQL connection string (or set CLUSTERA_DATABASE_URL env var)'
)
def stats(source_id: Optional[int], database_url: Optional[str]) -> None:
    """Show ingestion statistics."""
    click.echo("📊 Clustera YouTube Ingest - Statistics")
    click.echo("=" * 42)
    
    try:
        import asyncio
        
        async def stats_async():
            ingestor = YouTubeIngestor(database_url)
            try:
                click.echo(f"\n📈 Gathering statistics...")
                
                stats_data = await ingestor.get_ingestion_stats(source_id)
                
                if source_id:
                    click.echo(f"\n📺 Source ID {source_id} Statistics:")
                else:
                    click.echo(f"\n🌐 Overall Statistics:")
                
                click.echo(f"   📹 Total videos: {stats_data.get('total_videos', 0):,}")
                
                if not source_id:
                    click.echo(f"   📺 Total channels: {stats_data.get('total_channels', 0):,}")
                    click.echo(f"   📋 Total sources: {stats_data.get('total_sources', 0):,}")
                    click.echo(f"   ✅ Active sources: {stats_data.get('active_sources', 0):,}")
                
                click.echo(f"   📝 Videos with transcripts: {stats_data.get('videos_with_transcripts', 0):,}")
                click.echo(f"   📊 Transcript coverage: {stats_data.get('transcript_coverage', 0):.1f}%")
                
                if stats_data.get('last_ingestion'):
                    click.echo(f"   🕐 Last ingestion: {stats_data['last_ingestion']}")
                    click.echo(f"   📊 Last status: {stats_data.get('last_ingestion_status', 'Unknown')}")
                
            finally:
                ingestor.close()
        
        asyncio.run(stats_async())
        
    except KeyboardInterrupt:
        click.echo("\n\n⚠️  Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        click.echo(click.style(f"\n❌ Failed to get statistics: {e}", fg='red'))
        sys.exit(1)


# Migration management commands

@main.group()
def migrate() -> None:
    """Database migration management commands."""
    pass


@migrate.command('status')
@click.option(
    '--database-url', 
    envvar='CLUSTERA_DATABASE_URL',
    help='PostgreSQL connection string (or set CLUSTERA_DATABASE_URL env var)'
)
def migration_status(database_url: Optional[str]) -> None:
    """Show current migration status."""
    click.echo("🔄 Clustera YouTube Ingest - Migration Status")
    click.echo("=" * 49)
    
    try:
        from .migration_manager import MigrationManager
        
        migration_manager = MigrationManager(database_url)
        status = migration_manager.get_migration_status()
        
        if status.get("error"):
            click.echo(click.style(f"❌ Error: {status['error']}", fg='red'))
            sys.exit(1)
        
        click.echo(f"\n📊 Migration Status:")
        click.echo(f"   Current revision: {status.get('current_revision', 'None')}")
        click.echo(f"   Head revision: {status.get('head_revision', 'None')}")
        click.echo(f"   Schema exists: {'✅' if status.get('schema_exists') else '❌'}")
        click.echo(f"   Alembic initialized: {'✅' if status.get('alembic_version_table_exists') else '❌'}")
        
        if status.get("is_up_to_date"):
            click.echo(click.style("\n✅ Database is up to date", fg='green'))
        else:
            pending = status.get("pending_upgrades", [])
            click.echo(click.style(f"\n⚠️  {len(pending)} migration(s) pending", fg='yellow'))
            if pending:
                click.echo("   Pending migrations:")
                for migration in pending:
                    click.echo(f"      • {migration}")
                    
    except Exception as e:
        click.echo(click.style(f"❌ Migration status check failed: {e}", fg='red'))
        sys.exit(1)


@migrate.command('upgrade')
@click.option(
    '--database-url', 
    envvar='CLUSTERA_DATABASE_URL',
    help='PostgreSQL connection string (or set CLUSTERA_DATABASE_URL env var)'
)
def migration_upgrade(database_url: Optional[str]) -> None:
    """Upgrade database to the latest migration."""
    click.echo("⬆️  Clustera YouTube Ingest - Migration Upgrade")
    click.echo("=" * 51)
    
    try:
        from .migration_manager import MigrationManager
        
        migration_manager = MigrationManager(database_url)
        
        click.echo("\n🔍 Checking current status...")
        status = migration_manager.get_migration_status()
        
        if status.get("error"):
            click.echo(click.style(f"❌ Error: {status['error']}", fg='red'))
            sys.exit(1)
        
        if status.get("is_up_to_date"):
            click.echo(click.style("✅ Database is already up to date", fg='green'))
            return
        
        pending = status.get("pending_upgrades", [])
        click.echo(f"📋 {len(pending)} migration(s) will be applied")
        
        if not click.confirm("\nProceed with migration upgrade?"):
            click.echo("Migration cancelled")
            return
        
        click.echo("\n🔄 Running migrations...")
        with click.progressbar(length=100, label='Applying migrations') as bar:
            result = migration_manager.upgrade_to_head()
            bar.update(100)
        
        if result["success"]:
            click.echo(click.style("\n✅ Migration upgrade completed successfully!", fg='green'))
            click.echo(f"   Previous revision: {result.get('previous_revision', 'None')}")
            click.echo(f"   New revision: {result.get('new_revision')}")
            if result.get("migrations_applied"):
                click.echo(f"   Migrations applied: {len(result['migrations_applied'])}")
        else:
            click.echo(click.style(f"\n❌ Migration upgrade failed: {result['error']}", fg='red'))
            sys.exit(1)
                    
    except Exception as e:
        click.echo(click.style(f"❌ Migration upgrade failed: {e}", fg='red'))
        sys.exit(1)


@migrate.command('history')
@click.option(
    '--database-url', 
    envvar='CLUSTERA_DATABASE_URL',
    help='PostgreSQL connection string (or set CLUSTERA_DATABASE_URL env var)'
)
def migration_history(database_url: Optional[str]) -> None:
    """Show migration history."""
    click.echo("📜 Clustera YouTube Ingest - Migration History")
    click.echo("=" * 50)
    
    try:
        from .migration_manager import MigrationManager
        
        migration_manager = MigrationManager(database_url)
        history = migration_manager.get_migration_history()
        
        if not history:
            click.echo("\n📭 No migrations found")
            return
        
        click.echo(f"\n📋 Found {len(history)} migration(s):\n")
        
        for migration in history:
            click.echo(f"🔹 {migration['revision']}")
            click.echo(f"   Message: {migration['message']}")
            click.echo(f"   Down revision: {migration['down_revision']}")
            if migration.get('branch_labels'):
                click.echo(f"   Branch labels: {migration['branch_labels']}")
            click.echo()
                    
    except Exception as e:
        click.echo(click.style(f"❌ Failed to get migration history: {e}", fg='red'))
        sys.exit(1)


@migrate.command('create')
@click.option(
    '--message', '-m',
    required=True,
    help='Migration message/description'
)
@click.option(
    '--autogenerate/--no-autogenerate',
    default=True,
    help='Auto-generate migration from model changes (default: True)'
)
@click.option(
    '--database-url', 
    envvar='CLUSTERA_DATABASE_URL',
    help='PostgreSQL connection string (or set CLUSTERA_DATABASE_URL env var)'
)
def migration_create(message: str, autogenerate: bool, database_url: Optional[str]) -> None:
    """Create a new migration."""
    click.echo("✨ Clustera YouTube Ingest - Create Migration")
    click.echo("=" * 48)
    
    try:
        from .migration_manager import MigrationManager
        
        migration_manager = MigrationManager(database_url)
        
        click.echo(f"\n📝 Creating migration: {message}")
        click.echo(f"   Auto-generate: {autogenerate}")
        
        result = migration_manager.create_migration(message, autogenerate)
        
        if result["success"]:
            click.echo(click.style("\n✅ Migration created successfully!", fg='green'))
            click.echo(f"   Revision ID: {result['revision_id']}")
            click.echo(f"   Migration file: {result['migration_file']}")
        else:
            click.echo(click.style(f"\n❌ Migration creation failed: {result['error']}", fg='red'))
            sys.exit(1)
                    
    except Exception as e:
        click.echo(click.style(f"❌ Migration creation failed: {e}", fg='red'))
        sys.exit(1)


@migrate.command('stamp')
@click.option(
    '--revision',
    default="head",
    help='Revision to stamp (default: head)'
)
@click.option(
    '--database-url', 
    envvar='CLUSTERA_DATABASE_URL',
    help='PostgreSQL connection string (or set CLUSTERA_DATABASE_URL env var)'
)
def migration_stamp(revision: str, database_url: Optional[str]) -> None:
    """Stamp database with a specific revision (without running migrations)."""
    click.echo("🏷️  Clustera YouTube Ingest - Stamp Database")
    click.echo("=" * 49)
    
    click.echo(f"\n⚠️  Warning: This will mark the database as being at revision '{revision}'")
    click.echo("   without actually running the migrations. Use with caution!")
    
    if not click.confirm("\nProceed with stamping?"):
        click.echo("Stamping cancelled")
        return
    
    try:
        from .migration_manager import MigrationManager
        
        migration_manager = MigrationManager(database_url)
        
        click.echo(f"\n🏷️  Stamping database with revision: {revision}")
        
        result = migration_manager.stamp_database(revision)
        
        if result["success"]:
            click.echo(click.style("\n✅ Database stamped successfully!", fg='green'))
            click.echo(f"   Stamped revision: {result['stamped_revision']}")
        else:
            click.echo(click.style(f"\n❌ Database stamping failed: {result['error']}", fg='red'))
            sys.exit(1)
                    
    except Exception as e:
        click.echo(click.style(f"❌ Database stamping failed: {e}", fg='red'))
        sys.exit(1)


if __name__ == '__main__':
    main() 