"""
Apify API client for YouTube data extraction.

Handles interactions with Apify actors for YouTube scraping operations.
"""

import asyncio
import logging
import os
from typing import Dict, List, Optional, Any
from datetime import datetime

import aiohttp
from apify_client import ApifyClient as SyncApifyClient


logger = logging.getLogger(__name__)


class ApifyClient:
    """Async wrapper for Apify API interactions with YouTube scrapers."""
    
    def __init__(self, api_token: Optional[str] = None):
        """
        Initialize Apify client.
        
        Args:
            api_token: Apify API token, defaults to APIFY_TOKEN env var
        """
        self.api_token = api_token or os.getenv('APIFY_TOKEN')
        if not self.api_token:
            raise ValueError("APIFY_TOKEN environment variable or api_token parameter required")
        
        self.base_url = "https://api.apify.com/v2"
        self.youtube_scraper_id = os.getenv('APIFY_YOUTUBE_SCRAPER_ID', 'streamers~youtube-scraper')
        self.transcript_scraper_id = os.getenv('APIFY_TRANSCRIPT_SCRAPER_ID', 'pintostudio~youtube-transcript-scraper')
        
        # Configuration
        self.max_results = int(os.getenv('APIFY_MAX_RESULTS_PER_SOURCE', '100'))
        self.results_per_page = int(os.getenv('APIFY_RESULTS_PER_PAGE', '50'))
        self.request_timeout = int(os.getenv('APIFY_REQUEST_TIMEOUT', '300'))
        self.retry_attempts = int(os.getenv('RETRY_ATTEMPTS', '3'))
        self.use_proxy = os.getenv('ENABLE_PROXY', 'true').lower() == 'true'
        self.proxy_type = os.getenv('PROXY_TYPE', 'RESIDENTIAL')
    
    async def run_youtube_scraper(self, source_url: str, max_results: Optional[int] = None) -> Dict[str, Any]:
        """
        Run YouTube scraper actor for channel or playlist.
        
        Args:
            source_url: YouTube channel or playlist URL
            max_results: Maximum number of videos to extract
            
        Returns:
            Dict containing run_id, dataset_id, and status
            
        Raises:
            Exception: If actor run fails after retries
        """
        config = self._build_scraper_config(source_url, max_results or self.max_results)
        
        logger.info(f"Starting YouTube scraper for URL: {source_url}")
        
        for attempt in range(self.retry_attempts):
            try:
                async with aiohttp.ClientSession() as session:
                    # Start actor run
                    run_response = await self._start_actor_run(
                        session, self.youtube_scraper_id, config
                    )
                    
                    run_id = run_response['data']['id']
                    logger.info(f"Started actor run: {run_id}")
                    
                    # Wait for completion
                    final_status = await self._wait_for_completion(session, run_id)
                    
                    if final_status['status'] == 'SUCCEEDED':
                        return {
                            'run_id': run_id,
                            'dataset_id': final_status['defaultDatasetId'],
                            'status': 'success',
                            'stats': final_status.get('stats', {})
                        }
                    else:
                        error_msg = f"Actor run failed with status: {final_status['status']}"
                        logger.error(error_msg)
                        if attempt == self.retry_attempts - 1:
                            raise Exception(error_msg)
                        
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed: {str(e)}")
                if attempt == self.retry_attempts - 1:
                    raise
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
        
        raise Exception("All retry attempts failed")
    
    async def get_run_results(self, dataset_id: str) -> List[Dict[str, Any]]:
        """
        Retrieve results from completed actor run.
        
        Args:
            dataset_id: Apify dataset ID
            
        Returns:
            List of scraped data items
        """
        logger.info(f"Retrieving results from dataset: {dataset_id}")
        
        async with aiohttp.ClientSession() as session:
            url = f"{self.base_url}/datasets/{dataset_id}/items"
            params = {
                'token': self.api_token,
                'format': 'json',
                'limit': 10000  # Get all results
            }
            
            async with session.get(url, params=params, timeout=self.request_timeout) as response:
                if response.status == 200:
                    results = await response.json()
                    logger.info(f"Retrieved {len(results)} items from dataset")
                    return results
                else:
                    error_text = await response.text()
                    raise Exception(f"Failed to retrieve dataset: {response.status} - {error_text}")
    
    def _build_scraper_config(self, url: str, max_results: int) -> Dict[str, Any]:
        """
        Build configuration for YouTube scraper actor.
        
        Args:
            url: YouTube URL to scrape
            max_results: Maximum number of results
            
        Returns:
            Actor configuration dict
        """
        config = {
            "startUrls": [{"url": url}],
            "maxResults": max_results,
            "resultsPerPage": self.results_per_page,
            "handleRequestTimeoutSecs": self.request_timeout,
        }
        
        if self.use_proxy:
            config["proxyConfiguration"] = {
                "useApifyProxy": True,
                "apifyProxyGroups": [self.proxy_type]
            }
        
        return config
    
    async def _start_actor_run(self, session: aiohttp.ClientSession, actor_id: str, config: Dict) -> Dict:
        """Start an actor run via API."""
        url = f"{self.base_url}/acts/{actor_id}/runs"
        params = {'token': self.api_token}
        
        async with session.post(
            url, 
            params=params, 
            json=config,
            timeout=self.request_timeout
        ) as response:
            if response.status == 201:
                return await response.json()
            else:
                error_text = await response.text()
                raise Exception(f"Failed to start actor: {response.status} - {error_text}")
    
    async def _wait_for_completion(self, session: aiohttp.ClientSession, run_id: str) -> Dict:
        """Wait for actor run to complete."""
        url = f"{self.base_url}/actor-runs/{run_id}"
        params = {'token': self.api_token}
        
        while True:
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    run_data = await response.json()
                    status = run_data['data']['status']
                    
                    if status in ['SUCCEEDED', 'FAILED', 'ABORTED', 'TIMED-OUT']:
                        return run_data['data']
                    
                    # Still running, wait before checking again
                    await asyncio.sleep(10)
                else:
                    error_text = await response.text()
                    raise Exception(f"Failed to check run status: {response.status} - {error_text}")
    
    async def run_transcript_scraper(self, video_url: str) -> Dict[str, Any]:
        """
        Run transcript scraper for a single video.
        
        Args:
            video_url: YouTube video URL
            
        Returns:
            Dict containing transcript data
        """
        config = {"videoUrl": video_url}
        
        logger.info(f"Starting transcript scraper for video: {video_url}")
        
        for attempt in range(self.retry_attempts):
            try:
                async with aiohttp.ClientSession() as session:
                    run_response = await self._start_actor_run(
                        session, self.transcript_scraper_id, config
                    )
                    
                    run_id = run_response['data']['id']
                    final_status = await self._wait_for_completion(session, run_id)
                    
                    if final_status['status'] == 'SUCCEEDED':
                        results = await self.get_run_results(final_status['defaultDatasetId'])
                        return {
                            'run_id': run_id,
                            'status': 'success',
                            'transcript_data': results[0] if results else None
                        }
                    else:
                        error_msg = f"Transcript scraper failed: {final_status['status']}"
                        logger.error(error_msg)
                        if attempt == self.retry_attempts - 1:
                            raise Exception(error_msg)
                        
            except Exception as e:
                logger.warning(f"Transcript attempt {attempt + 1} failed: {str(e)}")
                if attempt == self.retry_attempts - 1:
                    raise
                await asyncio.sleep(2 ** attempt)
        
        raise Exception("All transcript retry attempts failed") 