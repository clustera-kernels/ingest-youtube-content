"""
Database management for Clustera YouTube Ingest.

Handles SQLAlchemy engine setup, connection management, and schema operations.
"""

import os
import logging
from pathlib import Path
from typing import Optional, Dict, Any, List, Set
from datetime import datetime, timedelta

from sqlalchemy import create_engine, text, inspect, String
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError, OperationalError

from .models import Base, CtrlIngestionLog, CtrlYouTubeList, DatasetYouTubeVideo, DatasetYouTubeChannel

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manages database connections and operations for YouTube ingestion."""
    
    def __init__(self, database_url: Optional[str] = None):
        """
        Initialize DatabaseManager with connection string.
        
        Args:
            database_url: PostgreSQL connection string. If None, reads from environment.
        """
        self.database_url = database_url or os.getenv('CLUSTERA_DATABASE_URL')
        if not self.database_url:
            raise ValueError(
                "Database URL not provided. Set CLUSTERA_DATABASE_URL environment variable "
                "or pass database_url parameter."
            )
        
        # Ensure proper driver specification for SQLAlchemy 2.0
        if self.database_url.startswith('postgresql://'):
            self.database_url = self.database_url.replace('postgresql://', 'postgresql+psycopg2://', 1)
            logger.debug(f"Updated database URL to use psycopg2 driver")
        elif self.database_url.startswith('postgres://'):
            self.database_url = self.database_url.replace('postgres://', 'postgresql+psycopg2://', 1)
            logger.debug(f"Updated database URL from postgres:// to use psycopg2 driver")
        
        self.engine: Optional[Engine] = None
        self.SessionLocal: Optional[sessionmaker] = None
        
    def connect(self) -> None:
        """Establish database connection and create session factory."""
        try:
            self.engine = create_engine(
                self.database_url,
                pool_size=5,
                max_overflow=10,
                pool_pre_ping=True,
                echo=False  # Set to True for SQL debugging
            )
            
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            self.SessionLocal = sessionmaker(
                autocommit=False,
                autoflush=False,
                bind=self.engine
            )
            
            logger.info("Database connection established successfully")
            
        except OperationalError as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error connecting to database: {e}")
            raise
    
    def get_session(self) -> Session:
        """Get a new database session."""
        if not self.SessionLocal:
            raise RuntimeError("Database not connected. Call connect() first.")
        return self.SessionLocal()
    
    def verify_connection(self) -> bool:
        """Verify database connection is working."""
        try:
            if not self.engine:
                return False
            
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT version()"))
                version = result.scalar()
                logger.info(f"Connected to PostgreSQL: {version}")
                return True
                
        except Exception as e:
            logger.error(f"Database connection verification failed: {e}")
            return False
    
    def check_postgresql_version(self) -> str:
        """Check PostgreSQL version and ensure compatibility."""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT version()"))
                version_string = result.scalar()
                
                # Extract version number
                if "PostgreSQL" in version_string:
                    version_parts = version_string.split()[1].split('.')
                    major_version = int(version_parts[0])
                    
                    if major_version < 12:
                        logger.warning(
                            f"PostgreSQL {major_version} detected. "
                            "Version 12+ recommended for optimal performance."
                        )
                    
                    return version_string
                else:
                    logger.warning("Could not determine PostgreSQL version")
                    return version_string
                    
        except Exception as e:
            logger.error(f"Failed to check PostgreSQL version: {e}")
            raise
    
    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database."""
        try:
            inspector = inspect(self.engine)
            return table_name in inspector.get_table_names()
        except Exception as e:
            logger.error(f"Error checking if table {table_name} exists: {e}")
            return False
    
    def init_schema(self) -> Dict[str, Any]:
        """
        Initialize database schema using Alembic migrations.
        
        This method is now a wrapper around Alembic migration management
        for compatibility with existing code.
        
        Returns:
            Dict with initialization results and statistics.
        """
        if not self.engine:
            raise RuntimeError("Database not connected. Call connect() first.")
        
        start_time = datetime.now()
        results = {
            "success": False,
            "tables_created": [],
            "indexes_created": [],
            "errors": [],
            "duration_seconds": 0,
            "migration_method": "alembic"
        }
        
        # Log start of schema initialization
        self._log_ingestion_event(
            stage_name="schema_init",
            status="started",
            source_identifier="database_schema"
        )
        
        try:
            from .migration_manager import MigrationManager
            
            migration_manager = MigrationManager(self.database_url)
            
            # Get current migration status
            status = migration_manager.get_migration_status()
            
            if status.get("error"):
                raise RuntimeError(f"Migration status check failed: {status['error']}")
            
            if status.get("is_up_to_date"):
                logger.info("Database schema is already up to date")
                results["success"] = True
                results["duration_seconds"] = (datetime.now() - start_time).total_seconds()
                
                # Still verify schema exists
                self._verify_schema()
                
                # Log completion
                self._log_ingestion_event(
                    stage_name="schema_init",
                    status="completed",
                    source_identifier="database_schema",
                    records_processed=0
                )
                
                return results
            
            # Check if this is a fresh database or needs migration
            if not status.get("schema_exists"):
                logger.info("Fresh database detected, running initial migration")
                # Run migration to head
                migration_result = migration_manager.upgrade_to_head()
            elif not status.get("alembic_version_table_exists"):
                logger.info("Existing schema detected without Alembic version table")
                # This is an existing database created with the old SQL method
                # Stamp it with the initial migration
                stamp_result = migration_manager.stamp_database("001_initial_schema")
                if not stamp_result["success"]:
                    raise RuntimeError(f"Failed to stamp existing database: {stamp_result['error']}")
                logger.info("Stamped existing database with initial migration")
                
                # Now upgrade to head if there are newer migrations
                migration_result = migration_manager.upgrade_to_head()
            else:
                logger.info("Running pending migrations")
                # Run pending migrations
                migration_result = migration_manager.upgrade_to_head()
            
            if not migration_result["success"]:
                raise RuntimeError(f"Migration failed: {migration_result['error']}")
            
            # Extract information for compatibility
            if migration_result.get("migrations_applied"):
                results["migrations_applied"] = migration_result["migrations_applied"]
                # For backward compatibility, assume tables were created
                results["tables_created"] = ["ctrl_youtube_lists", "dataset_youtube_video", 
                                           "dataset_youtube_channel", "ctrl_ingestion_log"]
                results["indexes_created"] = ["Various indexes created via migration"]
            
            # Verify schema creation
            self._verify_schema()
            
            results["success"] = True
            results["duration_seconds"] = (datetime.now() - start_time).total_seconds()
            results["previous_revision"] = migration_result.get("previous_revision")
            results["new_revision"] = migration_result.get("new_revision")
            
            logger.info(
                f"Schema initialization completed successfully using Alembic. "
                f"Upgraded from {results.get('previous_revision', 'None')} to "
                f"{results.get('new_revision')} in "
                f"{results['duration_seconds']:.2f} seconds."
            )
            
            # Log successful completion
            self._log_ingestion_event(
                stage_name="schema_init",
                status="completed",
                source_identifier="database_schema",
                records_processed=len(migration_result.get("migrations_applied", []))
            )
            
        except Exception as e:
            error_msg = f"Schema initialization failed: {e}"
            results["errors"].append(error_msg)
            results["duration_seconds"] = (datetime.now() - start_time).total_seconds()
            
            logger.error(error_msg)
            
            # Log failure
            self._log_ingestion_event(
                stage_name="schema_init",
                status="failed",
                source_identifier="database_schema",
                error_message=str(e)
            )
            
            raise
        
        return results
    
    def _extract_table_name(self, create_statement: str) -> str:
        """Extract table name from CREATE TABLE statement."""
        parts = create_statement.split()
        for i, part in enumerate(parts):
            if part.upper() == "TABLE":
                if i + 1 < len(parts):
                    table_name = parts[i + 1]
                    # Remove IF NOT EXISTS if present
                    if table_name.upper() == "IF":
                        table_name = parts[i + 4] if i + 4 < len(parts) else "unknown"
                    return table_name.strip('(')
        return "unknown"
    
    def _extract_index_name(self, create_statement: str) -> str:
        """Extract index name from CREATE INDEX statement."""
        parts = create_statement.split()
        for i, part in enumerate(parts):
            if part.upper() == "INDEX":
                if i + 1 < len(parts):
                    index_name = parts[i + 1]
                    # Remove IF NOT EXISTS if present
                    if index_name.upper() == "IF":
                        index_name = parts[i + 4] if i + 4 < len(parts) else "unknown"
                    return index_name
        return "unknown"
    
    def _verify_schema(self) -> None:
        """Verify that all required tables were created."""
        required_tables = [
            "ctrl_youtube_lists",
            "dataset_youtube_video", 
            "dataset_youtube_channel",
            "ctrl_ingestion_log"
        ]
        
        inspector = inspect(self.engine)
        existing_tables = inspector.get_table_names()
        
        missing_tables = [table for table in required_tables if table not in existing_tables]
        
        if missing_tables:
            raise RuntimeError(f"Schema verification failed. Missing tables: {missing_tables}")
        
        logger.info("Schema verification passed - all required tables exist")
    
    def _log_ingestion_event(
        self,
        stage_name: str,
        status: str,
        source_identifier: Optional[str] = None,
        error_message: Optional[str] = None,
        records_processed: int = 0,
        resource_pool: Optional[str] = None
    ) -> None:
        """Log an ingestion event to the control log table."""
        try:
            # Only log if the ingestion log table exists
            if not self.table_exists("ctrl_ingestion_log"):
                return
            
            with self.get_session() as session:
                log_entry = CtrlIngestionLog(
                    stage_name=stage_name,
                    source_identifier=source_identifier,
                    status=status,
                    error_message=error_message,
                    records_processed=records_processed,
                    completed_at=datetime.now() if status in ["completed", "failed"] else None,
                    resource_pool=resource_pool
                )
                session.add(log_entry)
                session.commit()
                
        except Exception as e:
            # Don't fail the main operation if logging fails
            logger.warning(f"Failed to log ingestion event: {e}")
    
    def close(self) -> None:
        """Close database connections."""
        if self.engine:
            self.engine.dispose()
            logger.info("Database connections closed")
    
    # Source Management Methods for Stage 1
    
    async def add_youtube_source(
        self, 
        url: str, 
        source_type: str, 
        name: str, 
        sync_hours: int,
        resource_pool: Optional[str] = None
    ) -> int:
        """
        Add a new YouTube source to the database.
        
        Args:
            url: YouTube source URL
            source_type: 'channel' or 'playlist'
            name: Display name for the source
            sync_hours: Sync frequency in hours
            resource_pool: Resource pool identifier
            
        Returns:
            ID of the created source
        """
        try:
            with self.get_session() as session:
                source = CtrlYouTubeList(
                    source_type=source_type,
                    source_url=url,
                    source_name=name,
                    sync_frequency_hours=sync_hours,
                    is_active=True,
                    resource_pool=resource_pool
                )
                session.add(source)
                session.commit()
                session.refresh(source)
                
                logger.info(f"Added YouTube source {source.id}: {url}")
                return source.id
                
        except Exception as e:
            logger.error(f"Failed to add YouTube source {url}: {e}")
            raise
    
    async def get_youtube_source_by_id(self, source_id: int) -> Optional[Dict[str, Any]]:
        """
        Get YouTube source by ID.
        
        Args:
            source_id: Source ID
            
        Returns:
            Source dictionary or None if not found
        """
        try:
            with self.get_session() as session:
                source = session.query(CtrlYouTubeList).filter(
                    CtrlYouTubeList.id == source_id
                ).first()
                
                if source:
                    return {
                        "id": source.id,
                        "source_type": source.source_type,
                        "source_url": source.source_url,
                        "source_name": source.source_name,
                        "is_active": source.is_active,
                        "created_at": source.created_at.isoformat() if source.created_at else None,
                        "updated_at": source.updated_at.isoformat() if source.updated_at else None,
                        "last_sync_at": source.last_sync_at.isoformat() if source.last_sync_at else None,
                        "sync_frequency_hours": source.sync_frequency_hours,
                        "resource_pool": source.resource_pool
                    }
                return None
                
        except Exception as e:
            logger.error(f"Failed to get YouTube source {source_id}: {e}")
            raise
    
    async def get_youtube_source_by_url(self, url: str) -> Optional[Dict[str, Any]]:
        """
        Get YouTube source by URL.
        
        Args:
            url: Source URL
            
        Returns:
            Source dictionary or None if not found
        """
        try:
            with self.get_session() as session:
                source = session.query(CtrlYouTubeList).filter(
                    CtrlYouTubeList.source_url == url
                ).first()
                
                if source:
                    return {
                        "id": source.id,
                        "source_type": source.source_type,
                        "source_url": source.source_url,
                        "source_name": source.source_name,
                        "is_active": source.is_active,
                        "created_at": source.created_at.isoformat() if source.created_at else None,
                        "updated_at": source.updated_at.isoformat() if source.updated_at else None,
                        "last_sync_at": source.last_sync_at.isoformat() if source.last_sync_at else None,
                        "sync_frequency_hours": source.sync_frequency_hours,
                        "resource_pool": source.resource_pool
                    }
                return None
                
        except Exception as e:
            logger.error(f"Failed to get YouTube source by URL {url}: {e}")
            raise
    
    async def get_active_sources(self) -> List[Dict[str, Any]]:
        """
        Get all active YouTube sources.
        
        Returns:
            List of active source dictionaries
        """
        try:
            with self.get_session() as session:
                sources = session.query(CtrlYouTubeList).filter(
                    CtrlYouTubeList.is_active == True
                ).order_by(CtrlYouTubeList.created_at.desc()).all()
                
                return [
                    {
                        "id": source.id,
                        "source_type": source.source_type,
                        "source_url": source.source_url,
                        "source_name": source.source_name,
                        "is_active": source.is_active,
                        "created_at": source.created_at.isoformat() if source.created_at else None,
                        "updated_at": source.updated_at.isoformat() if source.updated_at else None,
                        "last_sync_at": source.last_sync_at.isoformat() if source.last_sync_at else None,
                        "sync_frequency_hours": source.sync_frequency_hours,
                        "resource_pool": source.resource_pool
                    }
                    for source in sources
                ]
                
        except Exception as e:
            logger.error(f"Failed to get active sources: {e}")
            raise
    
    async def get_all_sources(self) -> List[Dict[str, Any]]:
        """
        Get all YouTube sources (active and inactive).
        
        Returns:
            List of all source dictionaries
        """
        try:
            with self.get_session() as session:
                sources = session.query(CtrlYouTubeList).order_by(
                    CtrlYouTubeList.created_at.desc()
                ).all()
                
                return [
                    {
                        "id": source.id,
                        "source_type": source.source_type,
                        "source_url": source.source_url,
                        "source_name": source.source_name,
                        "is_active": source.is_active,
                        "created_at": source.created_at.isoformat() if source.created_at else None,
                        "updated_at": source.updated_at.isoformat() if source.updated_at else None,
                        "last_sync_at": source.last_sync_at.isoformat() if source.last_sync_at else None,
                        "sync_frequency_hours": source.sync_frequency_hours,
                        "resource_pool": source.resource_pool
                    }
                    for source in sources
                ]
                
        except Exception as e:
            logger.error(f"Failed to get all sources: {e}")
            raise
    
    async def get_sources_due_for_sync(self) -> List[Dict[str, Any]]:
        """
        Get sources that are due for synchronization.
        
        Returns:
            List of sources due for sync
        """
        try:
            with self.get_session() as session:
                from sqlalchemy import or_, func
                
                now = datetime.now()
                
                # Sources are due for sync if:
                # 1. They've never been synced (last_sync_at is NULL)
                # 2. last_sync_at + sync_frequency_hours < now
                sources = session.query(CtrlYouTubeList).filter(
                    CtrlYouTubeList.is_active == True
                ).filter(
                    or_(
                        CtrlYouTubeList.last_sync_at.is_(None),
                        func.datetime(
                            CtrlYouTubeList.last_sync_at, 
                            '+' + func.cast(CtrlYouTubeList.sync_frequency_hours, String) + ' hours'
                        ) <= now
                    )
                ).order_by(CtrlYouTubeList.last_sync_at.asc().nullsfirst()).all()
                
                return [
                    {
                        "id": source.id,
                        "source_type": source.source_type,
                        "source_url": source.source_url,
                        "source_name": source.source_name,
                        "is_active": source.is_active,
                        "created_at": source.created_at.isoformat() if source.created_at else None,
                        "updated_at": source.updated_at.isoformat() if source.updated_at else None,
                        "last_sync_at": source.last_sync_at.isoformat() if source.last_sync_at else None,
                        "sync_frequency_hours": source.sync_frequency_hours,
                        "resource_pool": source.resource_pool
                    }
                    for source in sources
                ]
                
        except Exception as e:
            logger.error(f"Failed to get sources due for sync: {e}")
            raise
    
    async def update_youtube_source(self, source_id: int, **kwargs) -> bool:
        """
        Update a YouTube source.
        
        Args:
            source_id: Source ID
            **kwargs: Fields to update
            
        Returns:
            True if successful
        """
        try:
            with self.get_session() as session:
                source = session.query(CtrlYouTubeList).filter(
                    CtrlYouTubeList.id == source_id
                ).first()
                
                if not source:
                    return False
                
                # Update allowed fields
                allowed_fields = {
                    'source_name', 'sync_frequency_hours', 'is_active'
                }
                
                for field, value in kwargs.items():
                    if field in allowed_fields and hasattr(source, field):
                        setattr(source, field, value)
                
                source.updated_at = datetime.now()
                session.commit()
                
                logger.info(f"Updated YouTube source {source_id}")
                return True
                
        except Exception as e:
            logger.error(f"Failed to update YouTube source {source_id}: {e}")
            raise
    
    async def deactivate_youtube_source(self, source_id: int) -> bool:
        """
        Deactivate a YouTube source (soft delete).
        
        Args:
            source_id: Source ID
            
        Returns:
            True if successful
        """
        try:
            with self.get_session() as session:
                source = session.query(CtrlYouTubeList).filter(
                    CtrlYouTubeList.id == source_id
                ).first()
                
                if not source:
                    return False
                
                source.is_active = False
                source.updated_at = datetime.now()
                session.commit()
                
                logger.info(f"Deactivated YouTube source {source_id}")
                return True
                
        except Exception as e:
            logger.error(f"Failed to deactivate YouTube source {source_id}: {e}")
            raise
    
    async def update_source_sync_time(self, source_id: int) -> None:
        """
        Update the last sync time for a source.
        
        Args:
            source_id: Source ID
        """
        try:
            with self.get_session() as session:
                source = session.query(CtrlYouTubeList).filter(
                    CtrlYouTubeList.id == source_id
                ).first()
                
                if source:
                    source.last_sync_at = datetime.now()
                    source.updated_at = datetime.now()
                    session.commit()
                    
                    logger.debug(f"Updated sync time for source {source_id}")
                
        except Exception as e:
            logger.error(f"Failed to update sync time for source {source_id}: {e}")
            raise
    
    async def log_sync_operation(
        self,
        stage_name: str,
        source_type: Optional[str] = None,
        source_identifier: Optional[str] = None,
        status: str = "started",
        error_message: Optional[str] = None,
        records_processed: int = 0,
        apify_run_id: Optional[str] = None,
        apify_dataset_id: Optional[str] = None,
        resource_pool: Optional[str] = None
    ) -> None:
        """
        Log a sync operation to the ingestion log.
        
        Args:
            stage_name: Name of the pipeline stage
            source_type: Type of source being processed
            source_identifier: Source URL or identifier
            status: Operation status
            error_message: Error message if failed
            records_processed: Number of records processed
            apify_run_id: Apify run ID if applicable
            apify_dataset_id: Apify dataset ID if applicable
        """
        try:
            with self.get_session() as session:
                log_entry = CtrlIngestionLog(
                    stage_name=stage_name,
                    source_type=source_type,
                    source_identifier=source_identifier,
                    status=status,
                    error_message=error_message,
                    records_processed=records_processed,
                    apify_run_id=apify_run_id,
                    apify_dataset_id=apify_dataset_id,
                    completed_at=datetime.now() if status in ["completed", "failed"] else None,
                    resource_pool=resource_pool
                )
                session.add(log_entry)
                session.commit()
                
        except Exception as e:
            logger.error(f"Failed to log sync operation: {e}")
            # Don't raise - logging failures shouldn't stop operations
    
    async def log_ingestion_stage(
        self,
        stage_name: str,
        source_type: Optional[str] = None,
        source_identifier: Optional[str] = None,
        status: str = "started",
        error_message: Optional[str] = None,
        records_processed: int = 0,
        log_id: Optional[int] = None,
        apify_run_id: Optional[str] = None,
        apify_dataset_id: Optional[str] = None,
        resource_pool: Optional[str] = None
    ) -> int:
        """
        Log an ingestion stage operation.
        
        Args:
            stage_name: Name of the pipeline stage
            source_type: Type of source being processed
            source_identifier: Source URL or identifier
            status: Operation status
            error_message: Error message if failed
            records_processed: Number of records processed
            log_id: Existing log ID to update (for completion)
            apify_run_id: Apify run ID if applicable
            apify_dataset_id: Apify dataset ID if applicable
            
        Returns:
            Log entry ID
        """
        try:
            with self.get_session() as session:
                if log_id:
                    # Update existing log entry
                    log_entry = session.query(CtrlIngestionLog).filter(
                        CtrlIngestionLog.id == log_id
                    ).first()
                    
                    if log_entry:
                        log_entry.status = status
                        log_entry.error_message = error_message
                        log_entry.records_processed = records_processed
                        log_entry.apify_run_id = apify_run_id
                        log_entry.apify_dataset_id = apify_dataset_id
                        log_entry.resource_pool = resource_pool
                        if status in ["completed", "failed"]:
                            log_entry.completed_at = datetime.now()
                        session.commit()
                        return log_entry.id
                else:
                    # Create new log entry
                    log_entry = CtrlIngestionLog(
                        stage_name=stage_name,
                        source_type=source_type,
                        source_identifier=source_identifier,
                        status=status,
                        error_message=error_message,
                        records_processed=records_processed,
                        apify_run_id=apify_run_id,
                        apify_dataset_id=apify_dataset_id,
                        completed_at=datetime.now() if status in ["completed", "failed"] else None,
                        resource_pool=resource_pool
                    )
                    session.add(log_entry)
                    session.commit()
                    return log_entry.id
                
        except Exception as e:
            logger.error(f"Failed to log ingestion stage: {e}")
            return 0
    
    async def upsert_channel(self, channel_data: Dict[str, Any]) -> str:
        """
        Insert or update channel data.
        
        Args:
            channel_data: Processed channel data
            
        Returns:
            Channel ID
        """
        try:
            with self.get_session() as session:
                channel_id = channel_data['channel_id']
                
                # Check if channel exists
                existing_channel = session.query(DatasetYouTubeChannel).filter(
                    DatasetYouTubeChannel.channel_id == channel_id
                ).first()
                
                if existing_channel:
                    # Update existing channel
                    for key, value in channel_data.items():
                        if hasattr(existing_channel, key) and key != 'id':
                            setattr(existing_channel, key, value)
                    existing_channel.updated_at = datetime.now()
                else:
                    # Insert new channel
                    new_channel = DatasetYouTubeChannel(**channel_data)
                    session.add(new_channel)
                
                session.commit()
                logger.debug(f"Upserted channel: {channel_id}")
                return channel_id
                
        except Exception as e:
            logger.error(f"Failed to upsert channel {channel_data.get('channel_id')}: {e}")
            raise
    
    async def upsert_video(self, video_data: Dict[str, Any]) -> str:
        """
        Insert or update video data.
        
        Args:
            video_data: Processed video data
            
        Returns:
            Video ID
        """
        try:
            with self.get_session() as session:
                video_id = video_data['video_id']
                
                # Check if video exists
                existing_video = session.query(DatasetYouTubeVideo).filter(
                    DatasetYouTubeVideo.video_id == video_id
                ).first()
                
                if existing_video:
                    # Update existing video (preserve transcript data)
                    for key, value in video_data.items():
                        if hasattr(existing_video, key) and key not in ['id', 'transcript', 'transcript_text', 'transcript_language', 'transcript_ingested_at']:
                            setattr(existing_video, key, value)
                    existing_video.metadata_updated_at = datetime.now()
                else:
                    # Insert new video
                    new_video = DatasetYouTubeVideo(**video_data)
                    session.add(new_video)
                
                session.commit()
                logger.debug(f"Upserted video: {video_id}")
                return video_id
                
        except Exception as e:
            logger.error(f"Failed to upsert video {video_data.get('video_id')}: {e}")
            raise
    
    async def get_existing_video_ids(self, video_ids: List[str]) -> Set[str]:
        """
        Get existing video IDs from the database.
        
        Args:
            video_ids: List of video IDs to check
            
        Returns:
            Set of existing video IDs
        """
        try:
            with self.get_session() as session:
                existing_videos = session.query(DatasetYouTubeVideo.video_id).filter(
                    DatasetYouTubeVideo.video_id.in_(video_ids)
                ).all()
                
                return {video.video_id for video in existing_videos}
                
        except Exception as e:
            logger.error(f"Failed to get existing video IDs: {e}")
            return set()
    
    async def get_source_stats(self, source_list_id: int) -> Dict[str, Any]:
        """
        Get statistics for a source.
        
        Args:
            source_list_id: Source list ID
            
        Returns:
            Statistics dict
        """
        try:
            with self.get_session() as session:
                from sqlalchemy import func
                
                # Get video count
                video_count = session.query(func.count(DatasetYouTubeVideo.id)).filter(
                    DatasetYouTubeVideo.source_list_id == source_list_id
                ).scalar()
                
                # Get videos with transcripts
                transcript_count = session.query(func.count(DatasetYouTubeVideo.id)).filter(
                    DatasetYouTubeVideo.source_list_id == source_list_id,
                    DatasetYouTubeVideo.transcript_text.isnot(None)
                ).scalar()
                
                # Get latest ingestion
                latest_ingestion = session.query(CtrlIngestionLog).filter(
                    CtrlIngestionLog.source_identifier.contains(str(source_list_id))
                ).order_by(CtrlIngestionLog.started_at.desc()).first()
                
                return {
                    'total_videos': video_count or 0,
                    'videos_with_transcripts': transcript_count or 0,
                    'transcript_coverage': (transcript_count / video_count * 100) if video_count > 0 else 0,
                    'last_ingestion': latest_ingestion.started_at.isoformat() if latest_ingestion else None,
                    'last_ingestion_status': latest_ingestion.status if latest_ingestion else None
                }
                
        except Exception as e:
            logger.error(f"Failed to get source stats for {source_list_id}: {e}")
            return {}
    
    async def get_videos_needing_transcripts(self, limit: Optional[int] = None, source_list_id: Optional[int] = None) -> List[str]:
        """
        Get video IDs that need transcript processing.
        
        Args:
            limit: Maximum number of video IDs to return
            source_list_id: Optional source list ID for filtering
            
        Returns:
            List of video IDs needing transcripts
        """
        try:
            with self.get_session() as session:
                query = session.query(DatasetYouTubeVideo.video_id).filter(
                    DatasetYouTubeVideo.transcript_text.is_(None)
                )
                
                if source_list_id:
                    query = query.filter(DatasetYouTubeVideo.source_list_id == source_list_id)
                
                if limit:
                    query = query.limit(limit)
                
                return [row.video_id for row in query.all()]
                
        except Exception as e:
            logger.error(f"Failed to get videos needing transcripts: {e}")
            return []
    
    async def get_videos_by_batch(self, video_ids: List[str]) -> List[Dict[str, Any]]:
        """
        Get video records by batch of video IDs.
        
        Args:
            video_ids: List of video IDs to retrieve
            
        Returns:
            List of video data dictionaries
        """
        try:
            with self.get_session() as session:
                videos = session.query(DatasetYouTubeVideo).filter(
                    DatasetYouTubeVideo.video_id.in_(video_ids)
                ).all()
                
                return [
                    {
                        'video_id': video.video_id,
                        'title': video.title,
                        'channel_name': video.channel_name,
                        'duration': video.duration,
                        'has_transcript': video.transcript_text is not None
                    }
                    for video in videos
                ]
                
        except Exception as e:
            logger.error(f"Failed to get videos by batch: {e}")
            return []
    
    async def update_transcript_batch(self, transcript_updates: List[Dict[str, Any]]) -> int:
        """
        Update transcripts for multiple videos in a batch.
        
        Args:
            transcript_updates: List of dicts with video_id and transcript data
            
        Returns:
            Number of videos updated
        """
        updated_count = 0
        
        try:
            with self.get_session() as session:
                for update in transcript_updates:
                    video_id = update.get('video_id')
                    transcript_data = update.get('transcript_data', {})
                    
                    video = session.query(DatasetYouTubeVideo).filter(
                        DatasetYouTubeVideo.video_id == video_id
                    ).first()
                    
                    if video:
                        video.transcript = transcript_data.get('segments', [])
                        video.transcript_text = transcript_data.get('text', '')
                        video.transcript_language = transcript_data.get('language', 'en')
                        video.transcript_ingested_at = datetime.now()
                        updated_count += 1
                
                session.commit()
                logger.info(f"Updated transcripts for {updated_count} videos")
                
        except Exception as e:
            logger.error(f"Failed to update transcript batch: {e}")
            
        return updated_count
    
    async def get_transcript_statistics(self, source_list_id: Optional[int] = None) -> Dict[str, Any]:
        """
        Get comprehensive transcript processing statistics.
        
        Args:
            source_list_id: Optional source list ID for filtering
            
        Returns:
            Dict with transcript statistics
        """
        try:
            with self.get_session() as session:
                from sqlalchemy import func, and_
                
                # Base query
                base_query = session.query(DatasetYouTubeVideo)
                if source_list_id:
                    base_query = base_query.filter(DatasetYouTubeVideo.source_list_id == source_list_id)
                
                # Count totals
                total_videos = base_query.count()
                
                # Videos with transcripts (not null and not empty)
                videos_with_transcripts = base_query.filter(
                    and_(
                        DatasetYouTubeVideo.transcript_text.isnot(None),
                        DatasetYouTubeVideo.transcript_text != ''
                    )
                ).count()
                
                # Videos marked as unavailable (empty string)
                videos_unavailable = base_query.filter(
                    DatasetYouTubeVideo.transcript_text == ''
                ).count()
                
                # Videos never processed (null)
                videos_unprocessed = base_query.filter(
                    DatasetYouTubeVideo.transcript_text.is_(None)
                ).count()
                
                # Get language distribution
                language_stats = session.query(
                    DatasetYouTubeVideo.transcript_language,
                    func.count(DatasetYouTubeVideo.id)
                ).filter(
                    DatasetYouTubeVideo.transcript_language.isnot(None)
                ).group_by(DatasetYouTubeVideo.transcript_language).all()
                
                # Get average transcript length
                avg_length = session.query(
                    func.avg(func.length(DatasetYouTubeVideo.transcript_text))
                ).filter(
                    and_(
                        DatasetYouTubeVideo.transcript_text.isnot(None),
                        DatasetYouTubeVideo.transcript_text != ''
                    )
                ).scalar() or 0
                
                # Recent processing activity
                today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                recent_transcripts = base_query.filter(
                    DatasetYouTubeVideo.transcript_ingested_at >= today
                ).count()
                
                return {
                    'total_videos': total_videos,
                    'videos_with_transcripts': videos_with_transcripts,
                    'videos_unavailable': videos_unavailable,
                    'videos_unprocessed': videos_unprocessed,
                    'coverage_percentage': round(
                        (videos_with_transcripts / total_videos * 100) if total_videos > 0 else 0, 2
                    ),
                    'availability_rate': round(
                        (videos_with_transcripts / (total_videos - videos_unprocessed) * 100) 
                        if (total_videos - videos_unprocessed) > 0 else 0, 2
                    ),
                    'average_transcript_length': int(avg_length),
                    'recent_transcripts_today': recent_transcripts,
                    'language_distribution': {lang: count for lang, count in language_stats}
                }
                
        except Exception as e:
            logger.error(f"Failed to get transcript statistics: {e}")
            return {}
    
    async def mark_transcript_unavailable(self, video_id: str) -> bool:
        """
        Mark a video as having no available transcript.
        
        Args:
            video_id: YouTube video ID
            
        Returns:
            True if successfully marked
        """
        try:
            with self.get_session() as session:
                video = session.query(DatasetYouTubeVideo).filter(
                    DatasetYouTubeVideo.video_id == video_id
                ).first()
                
                if video:
                    video.transcript_text = ""  # Empty string indicates "checked but unavailable"
                    video.transcript_ingested_at = datetime.now()
                    session.commit()
                    return True
                    
        except Exception as e:
            logger.error(f"Failed to mark transcript unavailable for {video_id}: {e}")
            
        return False
    
    async def cleanup_failed_ingestion_logs(self, max_age_hours: int = 24) -> int:
        """
        Clean up failed ingestion logs older than specified age.
        
        Args:
            max_age_hours: Maximum age in hours
            
        Returns:
            Number of cleaned up records
        """
        try:
            with self.get_session() as session:
                from sqlalchemy import func
                
                cutoff_time = datetime.now() - timedelta(hours=max_age_hours)
                
                deleted_count = session.query(CtrlIngestionLog).filter(
                    CtrlIngestionLog.status == 'failed',
                    CtrlIngestionLog.started_at < cutoff_time
                ).delete()
                
                session.commit()
                return deleted_count
                
        except Exception as e:
            logger.error(f"Failed to cleanup failed ingestion logs: {e}")
            return 0 