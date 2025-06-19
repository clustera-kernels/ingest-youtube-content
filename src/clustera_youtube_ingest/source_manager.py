"""
Source management for YouTube channels and playlists.

Provides CRUD operations for managing YouTube sources in the database.
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta

from .database import DatabaseManager
from .url_utils import parse_youtube_url, validate_youtube_url, normalize_youtube_url
from .models import CtrlYouTubeList

logger = logging.getLogger(__name__)


class SourceManager:
    """
    Manages YouTube sources (channels and playlists) in the database.
    
    Provides CRUD operations with URL validation and metadata tracking.
    """
    
    def __init__(self, db_manager: DatabaseManager):
        """
        Initialize SourceManager.
        
        Args:
            db_manager: Database manager instance
        """
        self.db_manager = db_manager
    
    async def add_source(
        self, 
        url: str, 
        name: Optional[str] = None, 
        sync_hours: int = 24,
        resource_pool: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Add a new YouTube source to the database.
        
        Args:
            url: YouTube channel or playlist URL
            name: Optional custom name for the source
            sync_hours: Sync frequency in hours (1-168)
            resource_pool: Optional resource pool identifier
            
        Returns:
            Dict with operation result and source information
            
        Raises:
            ValueError: If URL is invalid or sync_hours out of range
            RuntimeError: If database operation fails
        """
        logger.info(f"Adding new source: {url}")
        
        # Validate URL
        if not validate_youtube_url(url):
            raise ValueError(f"Invalid YouTube URL: {url}")
        
        # Parse URL to get metadata
        parsed = parse_youtube_url(url)
        if not parsed:
            raise ValueError(f"Could not parse YouTube URL: {url}")
        
        # Validate sync frequency
        if not (1 <= sync_hours <= 168):
            raise ValueError("Sync frequency must be between 1 and 168 hours")
        
        # Normalize URL
        normalized_url = normalize_youtube_url(url)
        if not normalized_url:
            raise ValueError(f"Could not normalize URL: {url}")
        
        try:
            # Check if source already exists
            existing = await self._get_source_by_url(normalized_url)
            if existing:
                return {
                    "success": False,
                    "error": "Source already exists",
                    "existing_source": existing
                }
            
            # Use provided name or extract from URL
            source_name = name or self._extract_name_from_url(parsed)
            
            # Insert into database
            source_id = await self.db_manager.add_youtube_source(
                url=normalized_url,
                source_type=parsed["source_type"],
                name=source_name,
                sync_hours=sync_hours,
                resource_pool=resource_pool
            )
            
            # Get the created source
            source = await self._get_source_by_id(source_id)
            
            logger.info(f"Successfully added source {source_id}: {normalized_url}")
            
            return {
                "success": True,
                "source_id": source_id,
                "source": source,
                "parsed_url": parsed
            }
            
        except Exception as e:
            logger.error(f"Failed to add source {url}: {e}")
            raise RuntimeError(f"Database operation failed: {e}")
    
    async def remove_source(self, source_id: int) -> Dict[str, Any]:
        """
        Remove a YouTube source from the database.
        
        Args:
            source_id: ID of the source to remove
            
        Returns:
            Dict with operation result
        """
        logger.info(f"Removing source: {source_id}")
        
        try:
            # Get source before deletion for logging
            source = await self._get_source_by_id(source_id)
            if not source:
                return {
                    "success": False,
                    "error": f"Source {source_id} not found"
                }
            
            # Soft delete by setting is_active to False
            success = await self.db_manager.deactivate_youtube_source(source_id)
            
            if success:
                logger.info(f"Successfully removed source {source_id}: {source['source_url']}")
                return {
                    "success": True,
                    "removed_source": source
                }
            else:
                return {
                    "success": False,
                    "error": f"Failed to remove source {source_id}"
                }
                
        except Exception as e:
            logger.error(f"Failed to remove source {source_id}: {e}")
            raise RuntimeError(f"Database operation failed: {e}")
    
    async def list_sources(self, active_only: bool = True) -> List[Dict[str, Any]]:
        """
        List YouTube sources from the database.
        
        Args:
            active_only: If True, only return active sources
            
        Returns:
            List of source dictionaries
        """
        logger.debug(f"Listing sources (active_only={active_only})")
        
        try:
            if active_only:
                sources = await self.db_manager.get_active_sources()
            else:
                sources = await self.db_manager.get_all_sources()
            
            logger.debug(f"Found {len(sources)} sources")
            return sources
            
        except Exception as e:
            logger.error(f"Failed to list sources: {e}")
            raise RuntimeError(f"Database operation failed: {e}")
    
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
        logger.info(f"Updating source {source_id}: {kwargs}")
        
        try:
            # Validate sync_frequency_hours if provided
            if 'sync_frequency_hours' in kwargs:
                sync_hours = kwargs['sync_frequency_hours']
                if not (1 <= sync_hours <= 168):
                    raise ValueError("Sync frequency must be between 1 and 168 hours")
            
            # Update in database
            success = await self.db_manager.update_youtube_source(source_id, **kwargs)
            
            if success:
                # Get updated source
                source = await self._get_source_by_id(source_id)
                logger.info(f"Successfully updated source {source_id}")
                return {
                    "success": True,
                    "source": source
                }
            else:
                return {
                    "success": False,
                    "error": f"Source {source_id} not found or update failed"
                }
                
        except Exception as e:
            logger.error(f"Failed to update source {source_id}: {e}")
            raise RuntimeError(f"Database operation failed: {e}")
    
    async def get_source_by_id(self, source_id: int) -> Optional[Dict[str, Any]]:
        """
        Get a source by its ID.
        
        Args:
            source_id: Source ID
            
        Returns:
            Source dictionary or None if not found
        """
        return await self._get_source_by_id(source_id)
    
    async def get_source_by_url(self, url: str) -> Optional[Dict[str, Any]]:
        """
        Get a source by its URL.
        
        Args:
            url: Source URL (will be normalized)
            
        Returns:
            Source dictionary or None if not found
        """
        normalized_url = normalize_youtube_url(url)
        if not normalized_url:
            return None
        
        return await self._get_source_by_url(normalized_url)
    
    async def get_sources_due_for_sync(self) -> List[Dict[str, Any]]:
        """
        Get sources that are due for synchronization.
        
        Returns:
            List of sources that need syncing
        """
        logger.debug("Getting sources due for sync")
        
        try:
            sources = await self.db_manager.get_sources_due_for_sync()
            logger.debug(f"Found {len(sources)} sources due for sync")
            return sources
            
        except Exception as e:
            logger.error(f"Failed to get sources due for sync: {e}")
            raise RuntimeError(f"Database operation failed: {e}")
    
    async def update_source_sync_time(self, source_id: int) -> bool:
        """
        Update the last sync time for a source.
        
        Args:
            source_id: Source ID
            
        Returns:
            True if successful
        """
        try:
            await self.db_manager.update_source_sync_time(source_id)
            logger.debug(f"Updated sync time for source {source_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to update sync time for source {source_id}: {e}")
            return False
    
    def _extract_name_from_url(self, parsed_url: Dict[str, str]) -> str:
        """
        Extract a readable name from parsed URL data.
        
        Args:
            parsed_url: Parsed URL dictionary
            
        Returns:
            Extracted name
        """
        identifier = parsed_url["identifier"]
        source_type = parsed_url["source_type"]
        
        # For @username format, use the username
        if identifier.startswith('@') or '@' in parsed_url["original_url"]:
            return identifier.lstrip('@')
        
        # For channel IDs, use a generic name
        if identifier.startswith('UC'):
            return f"Channel {identifier[:8]}..."
        
        # For playlists, use generic name
        if source_type == "playlist":
            return f"Playlist {identifier[:8]}..."
        
        # Default to identifier
        return identifier
    
    async def _get_source_by_id(self, source_id: int) -> Optional[Dict[str, Any]]:
        """Get source by ID from database."""
        try:
            return await self.db_manager.get_youtube_source_by_id(source_id)
        except Exception as e:
            logger.error(f"Failed to get source {source_id}: {e}")
            return None
    
    async def _get_source_by_url(self, url: str) -> Optional[Dict[str, Any]]:
        """Get source by URL from database."""
        try:
            return await self.db_manager.get_youtube_source_by_url(url)
        except Exception as e:
            logger.error(f"Failed to get source by URL {url}: {e}")
            return None 