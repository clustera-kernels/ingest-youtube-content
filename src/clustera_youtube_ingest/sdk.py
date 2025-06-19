"""
Core SDK for Clustera YouTube Ingest.

Provides high-level interface for YouTube data ingestion operations.
"""

import logging
import os
from typing import Dict, Any, Optional, List
from datetime import datetime

from .database import DatabaseManager

logger = logging.getLogger(__name__)


class YouTubeIngestor:
    """
    Main orchestrator for YouTube data ingestion pipeline.
    
    Provides high-level interface for all ingestion operations while
    maintaining separation between CLI and core business logic.
    """
    
    def __init__(self, database_url: Optional[str] = None):
        """
        Initialize YouTube Ingestor.
        
        Args:
            database_url: PostgreSQL connection string. If None, reads from environment.
        """
        self.database_url = database_url or os.getenv('CLUSTERA_DATABASE_URL')
        self.db_manager: Optional[DatabaseManager] = None
        self._setup_logging()
    
    def _setup_logging(self) -> None:
        """Configure logging for the SDK."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
            ]
        )
    
    def init_database(self) -> Dict[str, Any]:
        """
        Initialize database schema (Stage 0 of pipeline).
        
        This method:
        1. Establishes database connection
        2. Verifies PostgreSQL version compatibility
        3. Executes schema migrations
        4. Creates performance indexes
        5. Validates schema integrity
        
        Returns:
            Dict containing initialization results and statistics.
            
        Raises:
            ValueError: If database URL is not configured
            RuntimeError: If database connection fails
            FileNotFoundError: If migration files are missing
            SQLAlchemyError: If schema creation fails
        """
        logger.info("Starting database initialization (Stage 0)")
        
        if not self.database_url:
            raise ValueError(
                "Database URL not configured. Set CLUSTERA_DATABASE_URL environment variable."
            )
        
        try:
            # Initialize database manager
            self.db_manager = DatabaseManager(self.database_url)
            
            # Establish connection
            logger.info("Connecting to database...")
            self.db_manager.connect()
            
            # Verify connection and check PostgreSQL version
            if not self.db_manager.verify_connection():
                raise RuntimeError("Database connection verification failed")
            
            version = self.db_manager.check_postgresql_version()
            logger.info(f"PostgreSQL version verified: {version}")
            
            # Initialize schema
            logger.info("Initializing database schema...")
            results = self.db_manager.init_schema()
            
            if results["success"]:
                logger.info(
                    f"Database initialization completed successfully. "
                    f"Created {len(results['tables_created'])} tables and "
                    f"{len(results['indexes_created'])} indexes."
                )
            else:
                logger.error("Database initialization failed")
                
            return results
            
        except Exception as e:
            logger.error(f"Database initialization failed: {e}")
            raise
    
    def validate_environment(self) -> Dict[str, Any]:
        """
        Validate that all required environment variables are set.
        
        Returns:
            Dict with validation results and missing variables.
        """
        required_vars = {
            'CLUSTERA_DATABASE_URL': 'PostgreSQL connection string',
            'APIFY_TOKEN': 'Apify API token for data extraction'
        }
        
        optional_vars = {
            'YOUTUBE_API_KEY': 'YouTube Data API key (optional)',
        }
        
        results = {
            "valid": True,
            "missing_required": [],
            "missing_optional": [],
            "configured_vars": []
        }
        
        # Check required variables
        for var_name, description in required_vars.items():
            value = os.getenv(var_name)
            if not value:
                results["missing_required"].append({
                    "name": var_name,
                    "description": description
                })
                results["valid"] = False
            else:
                results["configured_vars"].append(var_name)
        
        # Check optional variables
        for var_name, description in optional_vars.items():
            value = os.getenv(var_name)
            if not value:
                results["missing_optional"].append({
                    "name": var_name,
                    "description": description
                })
            else:
                results["configured_vars"].append(var_name)
        
        return results
    
    def get_database_status(self) -> Dict[str, Any]:
        """
        Get current database connection and schema status.
        
        Returns:
            Dict with database status information.
        """
        status = {
            "connected": False,
            "schema_initialized": False,
            "tables_exist": [],
            "missing_tables": [],
            "postgresql_version": None,
            "error": None
        }
        
        try:
            if not self.db_manager:
                self.db_manager = DatabaseManager(self.database_url)
                self.db_manager.connect()
            
            # Check connection
            status["connected"] = self.db_manager.verify_connection()
            
            if status["connected"]:
                # Check PostgreSQL version
                status["postgresql_version"] = self.db_manager.check_postgresql_version()
                
                # Check required tables
                required_tables = [
                    "ctrl_youtube_lists",
                    "dataset_youtube_video",
                    "dataset_youtube_channel", 
                    "ctrl_ingestion_log"
                ]
                
                for table in required_tables:
                    if self.db_manager.table_exists(table):
                        status["tables_exist"].append(table)
                    else:
                        status["missing_tables"].append(table)
                
                status["schema_initialized"] = len(status["missing_tables"]) == 0
            
        except Exception as e:
            status["error"] = str(e)
            logger.error(f"Error checking database status: {e}")
        
        return status
    
    def close(self) -> None:
        """Close database connections and cleanup resources."""
        if self.db_manager:
            self.db_manager.close()
            self.db_manager = None
        logger.info("YouTube Ingestor closed")
    
    # Stage 1: Source Management and Sync Operations
    
    async def add_source(
        self, 
        url: str, 
        name: Optional[str] = None, 
        sync_hours: int = 24,
        resource_pool: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Add a new YouTube source for monitoring.
        
        Args:
            url: YouTube channel or playlist URL
            name: Optional custom name for the source
            sync_hours: Sync frequency in hours (1-168)
            resource_pool: Optional resource pool identifier
            
        Returns:
            Dict with operation result and source information
        """
        logger.info(f"Adding YouTube source: {url}")
        
        if not self.db_manager:
            self.db_manager = DatabaseManager(self.database_url)
            self.db_manager.connect()
        
        from .source_manager import SourceManager
        source_manager = SourceManager(self.db_manager)
        
        return await source_manager.add_source(url, name, sync_hours, resource_pool)
    
    async def remove_source(self, source_id: int) -> Dict[str, Any]:
        """
        Remove a YouTube source from monitoring.
        
        Args:
            source_id: ID of the source to remove
            
        Returns:
            Dict with operation result
        """
        logger.info(f"Removing YouTube source: {source_id}")
        
        if not self.db_manager:
            self.db_manager = DatabaseManager(self.database_url)
            self.db_manager.connect()
        
        from .source_manager import SourceManager
        source_manager = SourceManager(self.db_manager)
        
        return await source_manager.remove_source(source_id)
    
    async def list_sources(self, active_only: bool = True) -> List[Dict[str, Any]]:
        """
        List YouTube sources.
        
        Args:
            active_only: If True, only return active sources
            
        Returns:
            List of source dictionaries
        """
        logger.debug(f"Listing YouTube sources (active_only={active_only})")
        
        if not self.db_manager:
            self.db_manager = DatabaseManager(self.database_url)
            self.db_manager.connect()
        
        from .source_manager import SourceManager
        source_manager = SourceManager(self.db_manager)
        
        return await source_manager.list_sources(active_only)
    
    async def update_source(
        self, 
        source_id: int, 
        **kwargs
    ) -> Dict[str, Any]:
        """
        Update a YouTube source.
        
        Args:
            source_id: ID of the source to update
            **kwargs: Fields to update (name, sync_frequency_hours, is_active)
            
        Returns:
            Dict with operation result
        """
        logger.info(f"Updating YouTube source {source_id}: {kwargs}")
        
        if not self.db_manager:
            self.db_manager = DatabaseManager(self.database_url)
            self.db_manager.connect()
        
        from .source_manager import SourceManager
        source_manager = SourceManager(self.db_manager)
        
        return await source_manager.update_source(source_id, **kwargs)
    
    async def sync_all_sources(self, dry_run: bool = False) -> Dict[str, Any]:
        """
        Sync all sources that are due for synchronization.
        
        Args:
            dry_run: If True, only identify sources without syncing
            
        Returns:
            Dict with sync results and statistics
        """
        logger.info(f"Starting sync all sources (dry_run={dry_run})")
        
        if not self.db_manager:
            self.db_manager = DatabaseManager(self.database_url)
            self.db_manager.connect()
        
        from .sync_orchestrator import SyncOrchestrator
        
        # Get configuration from environment
        max_concurrent = int(os.getenv('MAX_CONCURRENT_SYNCS', '3'))
        timeout_seconds = int(os.getenv('SYNC_TIMEOUT_SECONDS', '300'))
        
        orchestrator = SyncOrchestrator(
            self.db_manager,
            max_concurrent_syncs=max_concurrent,
            sync_timeout_seconds=timeout_seconds
        )
        
        return await orchestrator.sync_all_sources(dry_run)
    
    async def sync_source(self, source_id: int) -> Dict[str, Any]:
        """
        Sync a specific source by ID.
        
        Args:
            source_id: ID of the source to sync
            
        Returns:
            Dict with sync result
        """
        logger.info(f"Starting sync for source {source_id}")
        
        if not self.db_manager:
            self.db_manager = DatabaseManager(self.database_url)
            self.db_manager.connect()
        
        from .sync_orchestrator import SyncOrchestrator
        
        # Get configuration from environment
        max_concurrent = int(os.getenv('MAX_CONCURRENT_SYNCS', '3'))
        timeout_seconds = int(os.getenv('SYNC_TIMEOUT_SECONDS', '300'))
        
        orchestrator = SyncOrchestrator(
            self.db_manager,
            max_concurrent_syncs=max_concurrent,
            sync_timeout_seconds=timeout_seconds
        )
        
        return await orchestrator.sync_source(source_id)
    
    async def get_sources_due_for_sync(self) -> List[Dict[str, Any]]:
        """
        Get sources that are due for synchronization.
        
        Returns:
            List of sources that need syncing
        """
        logger.debug("Getting sources due for sync")
        
        if not self.db_manager:
            self.db_manager = DatabaseManager(self.database_url)
            self.db_manager.connect()
        
        from .source_manager import SourceManager
        source_manager = SourceManager(self.db_manager)
        
        return await source_manager.get_sources_due_for_sync()
    
    async def ingest_source(self, source_url: str, limit: Optional[int] = None, resource_pool: Optional[str] = None) -> List[str]:
        """
        Ingest videos from a YouTube channel or playlist (Stage 2).
        
        Args:
            source_url: YouTube channel or playlist URL
            limit: Optional limit on number of videos to process
            resource_pool: Optional resource pool identifier
            
        Returns:
            List of new video IDs for transcript processing
        """
        logger.info(f"Starting list ingestion for: {source_url}")
        
        if not self.db_manager:
            self.db_manager = DatabaseManager(self.database_url)
            self.db_manager.connect()
        
        # Get or create source record
        from .source_manager import SourceManager
        source_manager = SourceManager(self.db_manager)
        
        # Check if source exists
        existing_source = await source_manager.get_source_by_url(source_url)
        if not existing_source:
            # Auto-add source if it doesn't exist
            from .url_utils import YouTubeURLParser
            source_type = YouTubeURLParser.get_source_type(source_url)
            if not source_type:
                raise ValueError(f"Invalid YouTube URL: {source_url}")
            
            result = await source_manager.add_source(
                source_url, 
                name=f"Auto-added {source_type.value}",
                sync_hours=24,
                resource_pool=resource_pool
            )
            source_list_id = result['source_id']
        else:
            source_list_id = existing_source['id']
        
        # Initialize components
        from .apify_client import ApifyClient
        from .list_ingestion import ListIngestionManager
        
        apify_client = ApifyClient()
        list_manager = ListIngestionManager(self.db_manager, apify_client)
        
        # Run ingestion
        result = await list_manager.ingest_source(source_url, source_list_id, limit, resource_pool)
        
        logger.info(f"Ingestion completed: {result['videos_processed']} videos processed")
        return result['new_videos']
    
    async def ingest_video_transcript(self, video_id: str) -> bool:
        """
        Ingest transcript for a specific video (Stage 3).
        
        Args:
            video_id: YouTube video ID
            
        Returns:
            True if successful
        """
        logger.info(f"Starting transcript ingestion for video: {video_id}")
        
        if not self.db_manager:
            self.db_manager = DatabaseManager(self.database_url)
            self.db_manager.connect()
        
        from .apify_client import ApifyClient
        from .transcript_ingestion import TranscriptIngestionManager
        
        apify_client = ApifyClient()
        transcript_manager = TranscriptIngestionManager(self.db_manager, apify_client)
        
        try:
            result = await transcript_manager.ingest_single_transcript(video_id)
            return result['status'] == 'success'
                
        except Exception as e:
            logger.error(f"Failed to ingest transcript for {video_id}: {str(e)}")
            return False
        finally:
            transcript_manager.close()
    
    async def process_transcript_queue(self, video_ids: List[str], source_identifier: Optional[str] = None) -> Dict[str, Any]:
        """
        Process a queue of videos for transcript ingestion (Stage 3).
        
        Args:
            video_ids: List of video IDs to process
            source_identifier: Optional source identifier for logging
            
        Returns:
            Dict with processing results and statistics
        """
        logger.info(f"Starting transcript queue processing for {len(video_ids)} videos")
        
        if not self.db_manager:
            self.db_manager = DatabaseManager(self.database_url)
            self.db_manager.connect()
        
        from .apify_client import ApifyClient
        from .transcript_ingestion import TranscriptIngestionManager
        
        apify_client = ApifyClient()
        transcript_manager = TranscriptIngestionManager(self.db_manager, apify_client)
        
        try:
            return await transcript_manager.process_transcript_queue(video_ids, source_identifier)
        finally:
            transcript_manager.close()
    
    async def get_videos_needing_transcripts(self, limit: Optional[int] = None, source_list_id: Optional[int] = None) -> List[str]:
        """
        Get video IDs that need transcript processing.
        
        Args:
            limit: Maximum number of video IDs to return
            source_list_id: Optional source list ID for filtering
            
        Returns:
            List of video IDs needing transcripts
        """
        if not self.db_manager:
            self.db_manager = DatabaseManager(self.database_url)
            self.db_manager.connect()
        
        return await self.db_manager.get_videos_needing_transcripts(limit, source_list_id)
    
    async def get_transcript_statistics(self, source_list_id: Optional[int] = None) -> Dict[str, Any]:
        """
        Get comprehensive transcript processing statistics.
        
        Args:
            source_list_id: Optional source list ID for filtering
            
        Returns:
            Dict with transcript statistics
        """
        if not self.db_manager:
            self.db_manager = DatabaseManager(self.database_url)
            self.db_manager.connect()
        
        return await self.db_manager.get_transcript_statistics(source_list_id)
    
    async def _update_video_transcript(self, video_id: str, transcript_data: Dict[str, Any]) -> None:
        """Update video record with transcript data."""
        try:
            with self.db_manager.get_session() as session:
                from .models import DatasetYouTubeVideo
                
                video = session.query(DatasetYouTubeVideo).filter(
                    DatasetYouTubeVideo.video_id == video_id
                ).first()
                
                if video:
                    # Process transcript segments
                    transcript_segments = transcript_data.get('transcript', [])
                    
                    if transcript_segments:
                        # Store raw transcript
                        video.transcript = transcript_segments
                        
                        # Generate concatenated text
                        transcript_text = ' '.join([
                            segment.get('text', '') for segment in transcript_segments
                        ])
                        video.transcript_text = transcript_text.strip()
                        
                        # Set language and timestamp
                        video.transcript_language = transcript_data.get('language', 'en')
                        video.transcript_ingested_at = datetime.now()
                        
                        session.commit()
                        logger.debug(f"Updated transcript for video: {video_id}")
                    else:
                        logger.warning(f"Empty transcript data for video: {video_id}")
                else:
                    logger.warning(f"Video not found in database: {video_id}")
                    
        except Exception as e:
            logger.error(f"Failed to update video transcript: {str(e)}")
            raise
    
    async def get_ingestion_stats(self, source_id: Optional[int] = None) -> Dict[str, Any]:
        """
        Get ingestion statistics.
        
        Args:
            source_id: Optional source ID for specific stats
            
        Returns:
            Statistics dict
        """
        if not self.db_manager:
            self.db_manager = DatabaseManager(self.database_url)
            self.db_manager.connect()
        
        if source_id:
            from .list_ingestion import ListIngestionManager
            from .apify_client import ApifyClient
            
            apify_client = ApifyClient()
            list_manager = ListIngestionManager(self.db_manager, apify_client)
            
            return await list_manager.get_ingestion_stats(source_id)
        else:
            # Get overall stats
            try:
                with self.db_manager.get_session() as session:
                    from sqlalchemy import func
                    from .models import DatasetYouTubeVideo, DatasetYouTubeChannel, CtrlYouTubeList
                    
                    # Count totals
                    total_videos = session.query(func.count(DatasetYouTubeVideo.id)).scalar()
                    total_channels = session.query(func.count(DatasetYouTubeChannel.id)).scalar()
                    total_sources = session.query(func.count(CtrlYouTubeList.id)).scalar()
                    active_sources = session.query(func.count(CtrlYouTubeList.id)).filter(
                        CtrlYouTubeList.is_active == True
                    ).scalar()
                    
                    # Count videos with transcripts
                    videos_with_transcripts = session.query(func.count(DatasetYouTubeVideo.id)).filter(
                        DatasetYouTubeVideo.transcript_text.isnot(None)
                    ).scalar()
                    
                    return {
                        'total_videos': total_videos or 0,
                        'total_channels': total_channels or 0,
                        'total_sources': total_sources or 0,
                        'active_sources': active_sources or 0,
                        'videos_with_transcripts': videos_with_transcripts or 0,
                        'transcript_coverage': (videos_with_transcripts / total_videos * 100) if total_videos > 0 else 0
                    }
                    
            except Exception as e:
                logger.error(f"Failed to get ingestion stats: {str(e)}")
                return {}


# Convenience functions for direct SDK usage
def init_database(database_url: Optional[str] = None) -> Dict[str, Any]:
    """
    Convenience function to initialize database schema.
    
    Args:
        database_url: PostgreSQL connection string. If None, reads from environment.
        
    Returns:
        Dict containing initialization results.
    """
    ingestor = YouTubeIngestor(database_url)
    try:
        return ingestor.init_database()
    finally:
        ingestor.close()


def validate_environment() -> Dict[str, Any]:
    """
    Convenience function to validate environment configuration.
    
    Returns:
        Dict with validation results.
    """
    ingestor = YouTubeIngestor()
    return ingestor.validate_environment()


def get_database_status(database_url: Optional[str] = None) -> Dict[str, Any]:
    """
    Convenience function to check database status.
    
    Args:
        database_url: PostgreSQL connection string. If None, reads from environment.
        
    Returns:
        Dict with database status information.
    """
    ingestor = YouTubeIngestor(database_url)
    try:
        return ingestor.get_database_status()
    finally:
        ingestor.close() 