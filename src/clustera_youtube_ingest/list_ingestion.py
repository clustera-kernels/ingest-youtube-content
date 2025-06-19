"""
List ingestion manager for Stage 2 of the YouTube ingestion pipeline.

Orchestrates the extraction and processing of video metadata from YouTube channels/playlists.
"""

import asyncio
import logging
import os
from typing import Dict, List, Optional, Any, Set
from datetime import datetime

from .apify_client import ApifyClient
from .database import DatabaseManager
from .processors import VideoProcessor, ChannelProcessor, DateParser
from .url_utils import YouTubeURLParser


logger = logging.getLogger(__name__)


class ListIngestionManager:
    """Manages the ingestion of video lists from YouTube sources."""
    
    def __init__(self, db_manager: DatabaseManager, apify_client: ApifyClient):
        """
        Initialize list ingestion manager.
        
        Args:
            db_manager: Database manager instance
            apify_client: Apify client instance
        """
        self.db_manager = db_manager
        self.apify_client = apify_client
        self.batch_size = int(os.getenv('BATCH_SIZE_VIDEOS', '50'))
    
    async def ingest_source(self, source_url: str, source_list_id: int, limit: Optional[int] = None, resource_pool: Optional[str] = None) -> Dict[str, Any]:
        """
        Ingest videos from a YouTube channel or playlist.
        
        Args:
            source_url: YouTube channel or playlist URL
            source_list_id: ID from ctrl_youtube_lists table
            limit: Optional limit on number of videos to process
            resource_pool: Optional resource pool identifier
            
        Returns:
            Dict with ingestion results and statistics
        """
        start_time = datetime.now()
        
        # Log ingestion start
        log_id = await self.db_manager.log_ingestion_stage(
            stage_name="list_ingestion",
            source_type=YouTubeURLParser.get_source_type(source_url).value if YouTubeURLParser.get_source_type(source_url) else "unknown",
            source_identifier=source_url,
            status="started",
            resource_pool=resource_pool
        )
        
        try:
            logger.info(f"Starting list ingestion for source: {source_url}")
            
            # Run Apify scraper
            scraper_result = await self.apify_client.run_youtube_scraper(source_url, limit)
            
            if scraper_result['status'] != 'success':
                raise Exception(f"Apify scraper failed: {scraper_result}")
            
            # Get results from dataset
            raw_results = await self.apify_client.get_run_results(scraper_result['dataset_id'])
            
            if not raw_results:
                logger.warning(f"No results returned for source: {source_url}")
                await self.db_manager.log_ingestion_stage(
                    stage_name="list_ingestion",
                    source_type=YouTubeURLParser.get_source_type(source_url).value,
                    source_identifier=source_url,
                    status="completed",
                    records_processed=0,
                    log_id=log_id,
                    apify_run_id=scraper_result['run_id'],
                    apify_dataset_id=scraper_result['dataset_id']
                )
                return {
                    'status': 'success',
                    'videos_processed': 0,
                    'new_videos': [],
                    'channel_updated': False,
                    'processing_time': (datetime.now() - start_time).total_seconds()
                }
            
            # Process results
            result = await self.process_apify_results(raw_results, source_list_id, limit, resource_pool)
            
            # Update ingestion log
            await self.db_manager.log_ingestion_stage(
                stage_name="list_ingestion",
                source_type=YouTubeURLParser.get_source_type(source_url).value,
                source_identifier=source_url,
                status="completed",
                records_processed=result['videos_processed'],
                log_id=log_id,
                apify_run_id=scraper_result['run_id'],
                apify_dataset_id=scraper_result['dataset_id']
            )
            
            result['processing_time'] = (datetime.now() - start_time).total_seconds()
            logger.info(f"Completed list ingestion for {source_url}: {result['videos_processed']} videos processed")
            
            return result
            
        except Exception as e:
            logger.error(f"List ingestion failed for {source_url}: {str(e)}")
            
            # Log failure
            await self.db_manager.log_ingestion_stage(
                stage_name="list_ingestion",
                source_type=YouTubeURLParser.get_source_type(source_url).value if YouTubeURLParser.get_source_type(source_url) else "unknown",
                source_identifier=source_url,
                status="failed",
                error_message=str(e),
                log_id=log_id
            )
            
            raise
    
    async def process_apify_results(self, results: List[Dict[str, Any]], source_list_id: int, limit: Optional[int] = None, resource_pool: Optional[str] = None) -> Dict[str, Any]:
        """
        Process raw Apify results and store in database.
        
        Args:
            results: Raw results from Apify scraper
            source_list_id: Source list ID for tracking
            limit: Optional limit on number of videos to process
            resource_pool: Optional resource pool identifier
            
        Returns:
            Processing statistics
        """
        videos_processed = 0
        new_videos = []
        channel_updated = False
        
        # Extract channel data from first video (all videos have same channel info)
        channel_data = None
        video_data_list = results
        
        # Get channel data from the first video record
        if video_data_list:
            first_video = video_data_list[0]
            if first_video.get('channelId'):
                channel_data = first_video  # Channel info is embedded in video data
        
        # Process channel data if available
        if channel_data:
            try:
                channel_id = await self.upsert_channel_data(channel_data, resource_pool)
                if channel_id:
                    channel_updated = True
                    logger.info(f"Updated channel data for: {channel_id}")
            except Exception as e:
                logger.error(f"Failed to process channel data: {str(e)}")
        
        # Deduplicate videos
        unique_videos = self._deduplicate_videos(video_data_list)
        
        # Apply limit if specified
        if limit and limit < len(unique_videos):
            unique_videos = unique_videos[:limit]
            logger.info(f"Limited to {limit} videos from {len(self._deduplicate_videos(video_data_list))} unique videos")
        
        logger.info(f"Processing {len(unique_videos)} unique videos from {len(video_data_list)} total")
        
        # Get existing video IDs to avoid duplicates
        video_ids = [VideoProcessor.extract_video_id(v.get('url', '')) for v in unique_videos]
        video_ids = [vid for vid in video_ids if vid]  # Remove None values
        
        existing_video_ids = await self.db_manager.get_existing_video_ids(video_ids)
        
        # Process videos in batches
        for i in range(0, len(unique_videos), self.batch_size):
            batch = unique_videos[i:i + self.batch_size]
            
            for video_data in batch:
                try:
                    video_id = await self.upsert_video_data(video_data, source_list_id, resource_pool)
                    if video_id:
                        videos_processed += 1
                        
                        # Track new videos for transcript processing
                        if video_id not in existing_video_ids:
                            new_videos.append(video_id)
                            
                except Exception as e:
                    logger.error(f"Failed to process video: {str(e)}")
                    continue
        
        return {
            'status': 'success',
            'videos_processed': videos_processed,
            'new_videos': new_videos,
            'channel_updated': channel_updated,
            'total_raw_items': len(results),
            'unique_videos': len(unique_videos)
        }
    
    async def upsert_channel_data(self, channel_data: Dict[str, Any], resource_pool: Optional[str] = None) -> Optional[str]:
        """
        Insert or update channel data in database.
        
        Args:
            channel_data: Raw channel data from scraper
            resource_pool: Optional resource pool identifier
            
        Returns:
            Channel ID if successful, None otherwise
        """
        try:
            # Process channel data
            processed_data = ChannelProcessor.parse_channel_data(channel_data)
            
            if not processed_data.get('channel_id'):
                logger.warning("No channel ID found in channel data")
                return None
            
            # Add resource pool
            processed_data['resource_pool'] = resource_pool
            
            # Upsert to database
            await self.db_manager.upsert_channel(processed_data)
            
            return processed_data['channel_id']
            
        except Exception as e:
            logger.error(f"Error upserting channel data: {str(e)}")
            return None
    
    async def upsert_video_data(self, video_data: Dict[str, Any], source_list_id: int, resource_pool: Optional[str] = None) -> Optional[str]:
        """
        Insert or update video data in database.
        
        Args:
            video_data: Raw video data from scraper
            source_list_id: Source list ID for tracking
            resource_pool: Optional resource pool identifier
            
        Returns:
            Video ID if successful, None otherwise
        """
        try:
            # Process video data
            processed_data = VideoProcessor.parse_video_data(video_data)
            
            if not processed_data.get('video_id'):
                logger.warning(f"No video ID found in video data: {video_data.get('url', 'unknown')}")
                return None
            
            # Parse published date
            published_str = processed_data.get('published_at', '')
            if published_str:
                original_str, parsed_date = DateParser.extract_published_date(published_str)
                processed_data['published_date'] = parsed_date
            
            # Add source tracking
            processed_data['source_list_id'] = source_list_id
            processed_data['from_yt_url'] = video_data.get('sourceUrl', '')
            processed_data['resource_pool'] = resource_pool
            
            # Upsert to database
            await self.db_manager.upsert_video(processed_data)
            
            return processed_data['video_id']
            
        except Exception as e:
            logger.error(f"Error upserting video data: {str(e)}")
            return None
    
    def _deduplicate_videos(self, videos: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Remove duplicate videos based on video ID.
        
        Args:
            videos: List of video data dicts
            
        Returns:
            Deduplicated list of videos
        """
        seen_ids: Set[str] = set()
        unique_videos = []
        
        for video in videos:
            video_id = VideoProcessor.extract_video_id(video.get('url', ''))
            
            if video_id and video_id not in seen_ids:
                seen_ids.add(video_id)
                unique_videos.append(video)
            elif not video_id:
                logger.warning(f"Could not extract video ID from: {video.get('url', 'unknown')}")
        
        return unique_videos
    
    async def get_ingestion_stats(self, source_list_id: int) -> Dict[str, Any]:
        """
        Get ingestion statistics for a source.
        
        Args:
            source_list_id: Source list ID
            
        Returns:
            Statistics dict
        """
        try:
            stats = await self.db_manager.get_source_stats(source_list_id)
            return stats
        except Exception as e:
            logger.error(f"Error getting ingestion stats: {str(e)}")
            return {}
    
    async def cleanup_failed_runs(self, max_age_hours: int = 24) -> int:
        """
        Clean up failed ingestion runs older than specified age.
        
        Args:
            max_age_hours: Maximum age in hours for failed runs
            
        Returns:
            Number of cleaned up runs
        """
        try:
            cleaned_count = await self.db_manager.cleanup_failed_ingestion_logs(max_age_hours)
            logger.info(f"Cleaned up {cleaned_count} failed ingestion runs")
            return cleaned_count
        except Exception as e:
            logger.error(f"Error cleaning up failed runs: {str(e)}")
            return 0 