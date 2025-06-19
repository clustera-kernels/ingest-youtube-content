"""
Transcript ingestion manager for Stage 3 of the YouTube ingestion pipeline.

Orchestrates the extraction and processing of video transcripts with quality validation.
"""

import asyncio
import logging
import os
from typing import Dict, List, Optional, Any, Set
from datetime import datetime

from .apify_client import ApifyClient
from .database import DatabaseManager
from .processors import TranscriptProcessor
from .kafka_publisher import KafkaPublisher


logger = logging.getLogger(__name__)


class TranscriptIngestionManager:
    """Manages the ingestion of video transcripts with batch processing and quality validation."""
    
    def __init__(self, db_manager: DatabaseManager, apify_client: ApifyClient):
        """
        Initialize transcript ingestion manager.
        
        Args:
            db_manager: Database manager instance
            apify_client: Apify client instance
        """
        self.db_manager = db_manager
        self.apify_client = apify_client
        self.transcript_processor = TranscriptProcessor()
        
        # Initialize Kafka publisher for final stage publishing
        try:
            self.kafka_publisher = KafkaPublisher()
            self.enable_kafka = True
            logger.info("Kafka publisher initialized for transcript ingestion")
        except Exception as e:
            logger.warning(f"Failed to initialize Kafka publisher: {e}")
            self.kafka_publisher = None
            self.enable_kafka = False
        
        # Configuration from environment
        self.batch_size = int(os.getenv('TRANSCRIPT_BATCH_SIZE', '20'))
        self.concurrency = int(os.getenv('TRANSCRIPT_CONCURRENCY', '3'))
        self.min_length = int(os.getenv('TRANSCRIPT_MIN_LENGTH', '50'))
        self.retry_attempts = int(os.getenv('TRANSCRIPT_RETRY_ATTEMPTS', '3'))
        self.quality_threshold = float(os.getenv('TRANSCRIPT_QUALITY_THRESHOLD', '0.7'))
        self.enable_validation = os.getenv('ENABLE_TRANSCRIPT_VALIDATION', 'true').lower() == 'true'
        self.language_filter = os.getenv('TRANSCRIPT_LANGUAGE_FILTER', 'en').split(',')
    
    async def process_transcript_queue(self, video_ids: List[str], source_identifier: Optional[str] = None) -> Dict[str, Any]:
        """
        Process a queue of videos for transcript ingestion.
        
        Args:
            video_ids: List of video IDs to process
            source_identifier: Optional source identifier for logging
            
        Returns:
            Dict with processing results and statistics
        """
        start_time = datetime.now()
        
        # Log processing start
        log_id = await self.db_manager.log_ingestion_stage(
            stage_name="transcript_ingestion",
            source_type="batch",
            source_identifier=source_identifier or f"batch_{len(video_ids)}_videos",
            status="started"
        )
        
        try:
            logger.info(f"Starting transcript processing for {len(video_ids)} videos")
            
            # Initialize statistics
            stats = {
                'total_videos': len(video_ids),
                'successful': 0,
                'failed': 0,
                'unavailable': 0,
                'quality_rejected': 0,
                'already_processed': 0,
                'kafka_published': 0,
                'kafka_failed': 0,
                'errors': []
            }
            
            # Filter out videos that already have transcripts
            videos_needing_transcripts = await self._filter_videos_needing_transcripts(video_ids)
            stats['already_processed'] = len(video_ids) - len(videos_needing_transcripts)
            
            if not videos_needing_transcripts:
                logger.info("All videos already have transcripts")
                await self._complete_ingestion_log(log_id, stats)
                return self._build_result(stats, start_time)
            
            logger.info(f"Processing {len(videos_needing_transcripts)} videos needing transcripts")
            
            # Process videos in batches with concurrency control
            semaphore = asyncio.Semaphore(self.concurrency)
            
            for i in range(0, len(videos_needing_transcripts), self.batch_size):
                batch = videos_needing_transcripts[i:i + self.batch_size]
                
                # Process batch concurrently
                tasks = [
                    self._process_single_video_with_semaphore(semaphore, video_id, stats)
                    for video_id in batch
                ]
                
                await asyncio.gather(*tasks, return_exceptions=True)
                
                # Log progress
                processed = min(i + self.batch_size, len(videos_needing_transcripts))
                logger.info(f"Processed {processed}/{len(videos_needing_transcripts)} videos")
            
            # Complete logging
            await self._complete_ingestion_log(log_id, stats)
            
            result = self._build_result(stats, start_time)
            logger.info(f"Transcript processing completed: {stats['successful']} successful, {stats['failed']} failed")
            
            return result
            
        except Exception as e:
            logger.error(f"Transcript processing failed: {str(e)}")
            
            # Log failure
            await self.db_manager.log_ingestion_stage(
                stage_name="transcript_ingestion",
                source_type="batch",
                source_identifier=source_identifier or f"batch_{len(video_ids)}_videos",
                status="failed",
                error_message=str(e),
                log_id=log_id
            )
            
            raise
    
    async def ingest_single_transcript(self, video_id: str) -> Dict[str, Any]:
        """
        Ingest transcript for a single video.
        
        Args:
            video_id: YouTube video ID
            
        Returns:
            Dict with ingestion result
        """
        logger.info(f"Processing transcript for video: {video_id}")
        
        try:
            # Check if transcript already exists
            if await self._has_transcript(video_id):
                return {
                    'video_id': video_id,
                    'status': 'already_processed',
                    'message': 'Transcript already exists'
                }
            
            # Run transcript scraper
            video_url = f"https://www.youtube.com/watch?v={video_id}"
            scraper_result = await self.apify_client.run_transcript_scraper(video_url)
            
            if scraper_result['status'] != 'success' or not scraper_result['transcript_data']:
                await self._mark_transcript_unavailable(video_id)
                return {
                    'video_id': video_id,
                    'status': 'unavailable',
                    'message': 'No transcript available'
                }
            
            # Process and validate transcript
            transcript_data = scraper_result['transcript_data']
            processed_data = await self._process_transcript_data(transcript_data)
            
            if self.enable_validation and not await self._validate_transcript_quality(processed_data):
                return {
                    'video_id': video_id,
                    'status': 'quality_rejected',
                    'message': 'Transcript quality below threshold'
                }
            
            # Store transcript
            await self._store_transcript(video_id, processed_data)
            
            return {
                'video_id': video_id,
                'status': 'success',
                'language': processed_data.get('language', 'unknown'),
                'segment_count': len(processed_data.get('segments', [])),
                'text_length': len(processed_data.get('text', ''))
            }
            
        except Exception as e:
            logger.error(f"Failed to process transcript for {video_id}: {str(e)}")
            return {
                'video_id': video_id,
                'status': 'error',
                'message': str(e)
            }
    
    async def get_processing_statistics(self, source_list_id: Optional[int] = None) -> Dict[str, Any]:
        """
        Get comprehensive transcript processing statistics.
        
        Args:
            source_list_id: Optional source list ID for filtering
            
        Returns:
            Dict with statistics
        """
        try:
            with self.db_manager.get_session() as session:
                from sqlalchemy import func, and_
                from .models import DatasetYouTubeVideo, CtrlIngestionLog
                
                # Base query
                base_query = session.query(DatasetYouTubeVideo)
                if source_list_id:
                    base_query = base_query.filter(DatasetYouTubeVideo.source_list_id == source_list_id)
                
                # Count totals
                total_videos = base_query.count()
                videos_with_transcripts = base_query.filter(
                    DatasetYouTubeVideo.transcript_text.isnot(None)
                ).count()
                
                # Get transcript processing logs
                log_query = session.query(CtrlIngestionLog).filter(
                    CtrlIngestionLog.stage_name == 'transcript_ingestion'
                )
                
                recent_runs = log_query.filter(
                    CtrlIngestionLog.started_at >= datetime.now().replace(hour=0, minute=0, second=0)
                ).count()
                
                failed_runs = log_query.filter(
                    CtrlIngestionLog.status == 'failed'
                ).count()
                
                # Calculate coverage and quality metrics
                coverage_percentage = (videos_with_transcripts / total_videos * 100) if total_videos > 0 else 0
                
                # Get average transcript length
                avg_length = session.query(func.avg(func.length(DatasetYouTubeVideo.transcript_text))).filter(
                    DatasetYouTubeVideo.transcript_text.isnot(None)
                ).scalar() or 0
                
                return {
                    'total_videos': total_videos,
                    'videos_with_transcripts': videos_with_transcripts,
                    'coverage_percentage': round(coverage_percentage, 2),
                    'average_transcript_length': int(avg_length),
                    'recent_processing_runs': recent_runs,
                    'failed_runs': failed_runs,
                    'processing_success_rate': round(
                        ((recent_runs - failed_runs) / recent_runs * 100) if recent_runs > 0 else 0, 2
                    )
                }
                
        except Exception as e:
            logger.error(f"Failed to get transcript statistics: {str(e)}")
            return {}
    
    async def _filter_videos_needing_transcripts(self, video_ids: List[str]) -> List[str]:
        """Filter out videos that already have transcripts."""
        try:
            with self.db_manager.get_session() as session:
                from .models import DatasetYouTubeVideo
                
                existing_transcripts = session.query(DatasetYouTubeVideo.video_id).filter(
                    DatasetYouTubeVideo.video_id.in_(video_ids),
                    DatasetYouTubeVideo.transcript_text.isnot(None)
                ).all()
                
                existing_ids = {row.video_id for row in existing_transcripts}
                return [vid for vid in video_ids if vid not in existing_ids]
                
        except Exception as e:
            logger.error(f"Failed to filter videos: {str(e)}")
            return video_ids  # Return all if filtering fails
    
    async def _process_single_video_with_semaphore(self, semaphore: asyncio.Semaphore, video_id: str, stats: Dict) -> None:
        """Process a single video with concurrency control."""
        async with semaphore:
            try:
                result = await self.ingest_single_transcript(video_id)
                
                # Update statistics
                if result['status'] == 'success':
                    stats['successful'] += 1
                elif result['status'] == 'unavailable':
                    stats['unavailable'] += 1
                elif result['status'] == 'quality_rejected':
                    stats['quality_rejected'] += 1
                elif result['status'] == 'already_processed':
                    stats['already_processed'] += 1
                else:
                    stats['failed'] += 1
                    stats['errors'].append({
                        'video_id': video_id,
                        'error': result.get('message', 'Unknown error')
                    })
                    
            except Exception as e:
                stats['failed'] += 1
                stats['errors'].append({
                    'video_id': video_id,
                    'error': str(e)
                })
                logger.error(f"Error processing video {video_id}: {str(e)}")
    
    async def _has_transcript(self, video_id: str) -> bool:
        """Check if video already has a transcript."""
        try:
            with self.db_manager.get_session() as session:
                from .models import DatasetYouTubeVideo
                
                video = session.query(DatasetYouTubeVideo).filter(
                    DatasetYouTubeVideo.video_id == video_id,
                    DatasetYouTubeVideo.transcript_text.isnot(None)
                ).first()
                
                return video is not None
                
        except Exception as e:
            logger.error(f"Failed to check transcript existence for {video_id}: {str(e)}")
            return False
    
    async def _process_transcript_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process raw transcript data from Apify."""
        return self.transcript_processor.process_transcript_data(raw_data)
    
    async def _validate_transcript_quality(self, processed_data: Dict[str, Any]) -> bool:
        """Validate transcript quality against configured thresholds."""
        return self.transcript_processor.validate_quality(
            processed_data, 
            min_length=self.min_length,
            quality_threshold=self.quality_threshold,
            allowed_languages=self.language_filter
        )
    
    async def _store_transcript(self, video_id: str, processed_data: Dict[str, Any]) -> None:
        """Store processed transcript in database and publish to Kafka."""
        try:
            with self.db_manager.get_session() as session:
                from .models import DatasetYouTubeVideo
                
                video = session.query(DatasetYouTubeVideo).filter(
                    DatasetYouTubeVideo.video_id == video_id
                ).first()
                
                if video:
                    video.transcript = processed_data.get('segments', [])
                    video.transcript_text = processed_data.get('text', '')
                    video.transcript_language = processed_data.get('language', 'en')
                    video.transcript_ingested_at = datetime.now()
                    
                    session.commit()
                    logger.debug(f"Stored transcript for video: {video_id}")
                    
                    # After successful database storage, publish complete record to Kafka
                    kafka_success = await self._publish_complete_record(video_id, session)
                    if kafka_success:
                        logger.debug(f"Successfully published {video_id} to Kafka")
                    else:
                        logger.debug(f"Kafka publishing failed for {video_id} (pipeline continues)")
                    
                else:
                    logger.warning(f"Video not found in database: {video_id}")
                    
        except Exception as e:
            logger.error(f"Failed to store transcript for {video_id}: {str(e)}")
            raise
    
    async def _mark_transcript_unavailable(self, video_id: str) -> None:
        """Mark video as having no available transcript."""
        try:
            with self.db_manager.get_session() as session:
                from .models import DatasetYouTubeVideo
                
                video = session.query(DatasetYouTubeVideo).filter(
                    DatasetYouTubeVideo.video_id == video_id
                ).first()
                
                if video:
                    # Use empty string to indicate "checked but unavailable"
                    video.transcript_text = ""
                    video.transcript_ingested_at = datetime.now()
                    session.commit()
                    
        except Exception as e:
            logger.error(f"Failed to mark transcript unavailable for {video_id}: {str(e)}")
    
    async def _publish_complete_record(self, video_id: str, session) -> None:
        """Publish complete video record to Kafka raw topic after successful ingestion."""
        if not self.enable_kafka or not self.kafka_publisher:
            return
        
        try:
            from .models import DatasetYouTubeVideo
            
            # Fetch the complete updated video record
            video = session.query(DatasetYouTubeVideo).filter(
                DatasetYouTubeVideo.video_id == video_id
            ).first()
            
            if not video:
                logger.warning(f"Video {video_id} not found for Kafka publishing")
                return
            
            # Convert video record to JSON-serializable dict
            record_data = self._video_to_kafka_record(video)
            
            # Publish to raw records topic
            success = self.kafka_publisher.publish_raw_record(record_data, key=video_id)
            
            if success:
                logger.debug(f"Published video record to Kafka: {video_id}")
                return True
            else:
                logger.warning(f"Failed to publish video record to Kafka: {video_id}")
                return False
                
        except Exception as e:
            # Don't raise - Kafka failures shouldn't break ingestion pipeline
            logger.error(f"Error publishing video {video_id} to Kafka: {str(e)}")
            return False
    
    def _video_to_kafka_record(self, video) -> Dict[str, Any]:
        """Convert SQLAlchemy video model to Kafka-ready JSON record."""
        try:
            return {
                'record_type': 'youtube_video_complete',
                'video_id': video.video_id,
                'video_url': video.video_url,
                'title': video.title,
                'description': video.description,
                'channel_id': video.channel_id,
                'channel_name': video.channel_name,
                'channel_url': video.channel_url,
                'playlist_id': video.playlist_id,
                'playlist_name': video.playlist_name,
                'duration': video.duration,
                'duration_seconds': video.duration_seconds,
                'view_count': video.view_count,
                'like_count': video.like_count,
                'comment_count': video.comment_count,
                'published_at': video.published_at,
                'published_date': video.published_date.isoformat() if video.published_date else None,
                'transcript_segments': video.transcript,
                'transcript_text': video.transcript_text,
                'transcript_language': video.transcript_language,
                'thumbnail_url': video.thumbnail_url,
                'tags': video.tags,
                'category': video.category,
                'is_live_content': video.is_live_content,
                'is_monetized': video.is_monetized,
                'comments_turned_off': video.comments_turned_off,
                'location': video.location,
                'description_links': video.description_links,
                'subtitles': video.subtitles,
                'from_yt_url': video.from_yt_url,
                'source_list_id': video.source_list_id,
                'ingested_at': video.ingested_at.isoformat() if video.ingested_at else None,
                'transcript_ingested_at': video.transcript_ingested_at.isoformat() if video.transcript_ingested_at else None,
                'metadata_updated_at': video.metadata_updated_at.isoformat() if video.metadata_updated_at else None,
                'pipeline_completed_at': datetime.now().isoformat(),
                'has_transcript': bool(video.transcript_text),
                'processing_stage': 'complete'
            }
        except Exception as e:
            logger.error(f"Error serializing video record for Kafka: {str(e)}")
            # Return minimal record on serialization error
            return {
                'record_type': 'youtube_video_complete',
                'video_id': video.video_id,
                'title': getattr(video, 'title', None),
                'channel_name': getattr(video, 'channel_name', None),
                'has_transcript': bool(getattr(video, 'transcript_text', None)),
                'pipeline_completed_at': datetime.now().isoformat(),
                'processing_stage': 'complete',
                'serialization_error': str(e)
            }
    
    async def _complete_ingestion_log(self, log_id: int, stats: Dict[str, Any]) -> None:
        """Complete the ingestion log with final statistics."""
        await self.db_manager.log_ingestion_stage(
            stage_name="transcript_ingestion",
            status="completed",
            records_processed=stats['successful'],
            log_id=log_id
        )
    
    def _build_result(self, stats: Dict[str, Any], start_time: datetime) -> Dict[str, Any]:
        """Build final result dictionary."""
        processing_time = (datetime.now() - start_time).total_seconds()
        
        return {
            'status': 'success',
            'processing_time': processing_time,
            'statistics': stats,
            'success_rate': round(
                (stats['successful'] / stats['total_videos'] * 100) if stats['total_videos'] > 0 else 0, 2
            )
        }
    
    def close(self) -> None:
        """Clean up resources including Kafka publisher."""
        if self.kafka_publisher:
            try:
                self.kafka_publisher.close()
                logger.debug("Kafka publisher closed")
            except Exception as e:
                logger.warning(f"Error closing Kafka publisher: {e}")
            finally:
                self.kafka_publisher = None
                self.enable_kafka = False 