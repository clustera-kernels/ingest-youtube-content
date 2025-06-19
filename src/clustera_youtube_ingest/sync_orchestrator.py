"""
Sync orchestration for YouTube sources.

Coordinates synchronization of YouTube sources based on frequency and eligibility.
"""

import logging
import asyncio
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from .database import DatabaseManager
from .source_manager import SourceManager

logger = logging.getLogger(__name__)


class SyncOrchestrator:
    """
    Orchestrates synchronization of YouTube sources.
    
    Manages sync scheduling, concurrency, and error handling for source processing.
    """
    
    def __init__(
        self, 
        db_manager: DatabaseManager,
        max_concurrent_syncs: int = 3,
        sync_timeout_seconds: int = 300
    ):
        """
        Initialize SyncOrchestrator.
        
        Args:
            db_manager: Database manager instance
            max_concurrent_syncs: Maximum concurrent sync operations
            sync_timeout_seconds: Timeout for individual sync operations
        """
        self.db_manager = db_manager
        self.source_manager = SourceManager(db_manager)
        self.max_concurrent_syncs = max_concurrent_syncs
        self.sync_timeout_seconds = sync_timeout_seconds
    
    async def sync_all_sources(self, dry_run: bool = False) -> Dict[str, Any]:
        """
        Sync all sources that are due for synchronization.
        
        Args:
            dry_run: If True, only identify sources without syncing
            
        Returns:
            Dict with sync results and statistics
        """
        logger.info(f"Starting sync all sources (dry_run={dry_run})")
        
        start_time = datetime.now()
        results = {
            "started_at": start_time.isoformat(),
            "dry_run": dry_run,
            "sources_processed": 0,
            "sources_successful": 0,
            "sources_failed": 0,
            "sources_skipped": 0,
            "errors": [],
            "source_results": []
        }
        
        try:
            # Get sources due for sync
            eligible_sources = await self.get_eligible_sources()
            
            if not eligible_sources:
                logger.info("No sources due for sync")
                results["message"] = "No sources due for sync"
                return results
            
            logger.info(f"Found {len(eligible_sources)} sources due for sync")
            
            if dry_run:
                results["eligible_sources"] = eligible_sources
                results["message"] = f"Would sync {len(eligible_sources)} sources"
                return results
            
            # Process sources with concurrency control
            source_results = await self._process_sources_concurrent(eligible_sources)
            
            # Aggregate results
            for source_result in source_results:
                results["source_results"].append(source_result)
                results["sources_processed"] += 1
                
                if source_result["success"]:
                    results["sources_successful"] += 1
                elif source_result.get("skipped"):
                    results["sources_skipped"] += 1
                else:
                    results["sources_failed"] += 1
                    if source_result.get("error"):
                        results["errors"].append({
                            "source_id": source_result["source_id"],
                            "error": source_result["error"]
                        })
            
            # Calculate duration
            end_time = datetime.now()
            results["completed_at"] = end_time.isoformat()
            results["duration_seconds"] = (end_time - start_time).total_seconds()
            
            logger.info(
                f"Sync completed: {results['sources_successful']} successful, "
                f"{results['sources_failed']} failed, {results['sources_skipped']} skipped"
            )
            
            return results
            
        except Exception as e:
            logger.error(f"Sync all sources failed: {e}")
            results["error"] = str(e)
            results["completed_at"] = datetime.now().isoformat()
            return results
    
    async def sync_source(self, source_id: int) -> Dict[str, Any]:
        """
        Sync a specific source by ID.
        
        Args:
            source_id: ID of the source to sync
            
        Returns:
            Dict with sync result
        """
        logger.info(f"Starting sync for source {source_id}")
        
        start_time = datetime.now()
        result = {
            "source_id": source_id,
            "started_at": start_time.isoformat(),
            "success": False
        }
        
        try:
            # Get source details
            source = await self.source_manager.get_source_by_id(source_id)
            if not source:
                result["error"] = f"Source {source_id} not found"
                return result
            
            if not source.get("is_active", True):
                result["error"] = f"Source {source_id} is not active"
                result["skipped"] = True
                return result
            
            result["source_url"] = source["source_url"]
            result["source_type"] = source["source_type"]
            
            # Log sync start
            await self._log_sync_operation(
                source_id=source_id,
                status="started",
                source_type=source["source_type"],
                source_identifier=source["source_url"]
            )
            
            # TODO: This is where Stage 2 (List Ingestion) will be called
            # For now, we'll simulate the sync operation
            await self._simulate_sync_operation(source)
            
            # Update sync time
            await self.source_manager.update_source_sync_time(source_id)
            
            # Log sync completion
            await self._log_sync_operation(
                source_id=source_id,
                status="completed",
                source_type=source["source_type"],
                source_identifier=source["source_url"],
                records_processed=0  # Will be updated in Stage 2
            )
            
            end_time = datetime.now()
            result["completed_at"] = end_time.isoformat()
            result["duration_seconds"] = (end_time - start_time).total_seconds()
            result["success"] = True
            
            logger.info(f"Successfully synced source {source_id}")
            return result
            
        except Exception as e:
            logger.error(f"Failed to sync source {source_id}: {e}")
            
            # Log sync failure
            await self._log_sync_operation(
                source_id=source_id,
                status="failed",
                source_type=result.get("source_type"),
                source_identifier=result.get("source_url"),
                error_message=str(e)
            )
            
            result["error"] = str(e)
            result["completed_at"] = datetime.now().isoformat()
            return result
    
    async def get_eligible_sources(self) -> List[Dict[str, Any]]:
        """
        Get sources that are eligible for synchronization.
        
        Returns:
            List of sources due for sync
        """
        logger.debug("Getting eligible sources for sync")
        
        try:
            sources = await self.source_manager.get_sources_due_for_sync()
            
            # Add eligibility metadata
            for source in sources:
                source["eligible_reason"] = self._get_eligibility_reason(source)
            
            return sources
            
        except Exception as e:
            logger.error(f"Failed to get eligible sources: {e}")
            return []
    
    async def _process_sources_concurrent(
        self, 
        sources: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Process multiple sources concurrently with controlled parallelism.
        
        Args:
            sources: List of sources to process
            
        Returns:
            List of sync results
        """
        logger.info(f"Processing {len(sources)} sources with max concurrency {self.max_concurrent_syncs}")
        
        # Create semaphore to limit concurrency
        semaphore = asyncio.Semaphore(self.max_concurrent_syncs)
        
        async def sync_with_semaphore(source: Dict[str, Any]) -> Dict[str, Any]:
            async with semaphore:
                try:
                    # Add timeout to individual sync operations
                    return await asyncio.wait_for(
                        self.sync_source(source["id"]),
                        timeout=self.sync_timeout_seconds
                    )
                except asyncio.TimeoutError:
                    logger.error(f"Sync timeout for source {source['id']}")
                    return {
                        "source_id": source["id"],
                        "success": False,
                        "error": f"Sync timeout after {self.sync_timeout_seconds} seconds"
                    }
                except Exception as e:
                    logger.error(f"Unexpected error syncing source {source['id']}: {e}")
                    return {
                        "source_id": source["id"],
                        "success": False,
                        "error": str(e)
                    }
        
        # Create tasks for all sources
        tasks = [sync_with_semaphore(source) for source in sources]
        
        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Handle any exceptions that weren't caught
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Task exception for source {sources[i]['id']}: {result}")
                processed_results.append({
                    "source_id": sources[i]["id"],
                    "success": False,
                    "error": str(result)
                })
            else:
                processed_results.append(result)
        
        return processed_results
    
    async def _simulate_sync_operation(self, source: Dict[str, Any]) -> None:
        """
        Simulate sync operation for Stage 1 implementation.
        
        This will be replaced with actual Stage 2 (List Ingestion) logic.
        
        Args:
            source: Source dictionary
        """
        logger.debug(f"Simulating sync for {source['source_type']}: {source['source_url']}")
        
        # Simulate processing time
        await asyncio.sleep(0.1)
        
        # TODO: Replace with actual Stage 2 implementation
        # This would call the Apify actor and process results
        pass
    
    def _get_eligibility_reason(self, source: Dict[str, Any]) -> str:
        """
        Get human-readable reason why source is eligible for sync.
        
        Args:
            source: Source dictionary
            
        Returns:
            Eligibility reason string
        """
        last_sync = source.get("last_sync_at")
        sync_frequency = source.get("sync_frequency_hours", 24)
        
        if not last_sync:
            return "Never synced before"
        
        if isinstance(last_sync, str):
            try:
                last_sync = datetime.fromisoformat(last_sync.replace('Z', '+00:00'))
            except:
                return "Invalid last sync time"
        
        next_sync_due = last_sync + timedelta(hours=sync_frequency)
        hours_overdue = (datetime.now() - next_sync_due).total_seconds() / 3600
        
        if hours_overdue > 0:
            return f"Overdue by {hours_overdue:.1f} hours"
        else:
            return "Due for sync"
    
    async def _log_sync_operation(
        self,
        source_id: int,
        status: str,
        source_type: Optional[str] = None,
        source_identifier: Optional[str] = None,
        error_message: Optional[str] = None,
        records_processed: int = 0,
        apify_run_id: Optional[str] = None,
        apify_dataset_id: Optional[str] = None
    ) -> None:
        """
        Log sync operation to the database.
        
        Args:
            source_id: Source ID being synced
            status: Operation status ('started', 'completed', 'failed')
            source_type: Type of source ('channel' or 'playlist')
            source_identifier: Source URL or identifier
            error_message: Error message if failed
            records_processed: Number of records processed
            apify_run_id: Apify run ID if applicable
            apify_dataset_id: Apify dataset ID if applicable
        """
        try:
            await self.db_manager.log_sync_operation(
                stage_name="stage_1_sync",
                source_type=source_type,
                source_identifier=source_identifier,
                status=status,
                error_message=error_message,
                records_processed=records_processed,
                apify_run_id=apify_run_id,
                apify_dataset_id=apify_dataset_id
            )
        except Exception as e:
            logger.error(f"Failed to log sync operation for source {source_id}: {e}")
            # Don't raise - logging failures shouldn't stop sync operations 