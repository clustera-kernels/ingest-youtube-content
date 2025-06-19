"""
YouTube URL validation and parsing utilities.

Provides functions to validate, parse, and extract information from YouTube URLs.
"""

import re
import urllib.parse
from typing import Optional, Dict, Tuple
from enum import Enum


class SourceType(Enum):
    """YouTube source types."""
    CHANNEL = "channel"
    PLAYLIST = "playlist"


class YouTubeURLParser:
    """Parser for YouTube URLs with validation and metadata extraction."""
    
    # Channel URL patterns
    CHANNEL_PATTERNS = [
        r'(?:https?://)?(?:www\.)?youtube\.com/channel/([a-zA-Z0-9_-]+)',
        r'(?:https?://)?(?:www\.)?youtube\.com/c/([a-zA-Z0-9_-]+)',
        r'(?:https?://)?(?:www\.)?youtube\.com/user/([a-zA-Z0-9_-]+)',
        r'(?:https?://)?(?:www\.)?youtube\.com/@([a-zA-Z0-9_.-]+)',
    ]
    
    # Playlist URL patterns
    PLAYLIST_PATTERNS = [
        r'(?:https?://)?(?:www\.)?youtube\.com/playlist\?list=([a-zA-Z0-9_-]+)',
        r'(?:https?://)?(?:www\.)?youtube\.com/watch\?.*list=([a-zA-Z0-9_-]+)',
    ]
    
    @classmethod
    def parse_url(cls, url: str) -> Optional[Dict[str, str]]:
        """
        Parse a YouTube URL and extract metadata.
        
        Args:
            url: YouTube URL to parse
            
        Returns:
            Dict with parsed information or None if invalid
            
        Example:
            {
                "source_type": "channel",
                "identifier": "UCxxxxxx",
                "normalized_url": "https://www.youtube.com/channel/UCxxxxxx",
                "original_url": "youtube.com/@username"
            }
        """
        if not url or not isinstance(url, str):
            return None
        
        url = url.strip()
        
        # Try channel patterns first
        for pattern in cls.CHANNEL_PATTERNS:
            match = re.match(pattern, url, re.IGNORECASE)
            if match:
                identifier = match.group(1)
                return {
                    "source_type": SourceType.CHANNEL.value,
                    "identifier": identifier,
                    "normalized_url": cls._normalize_channel_url(url, identifier),
                    "original_url": url
                }
        
        # Try playlist patterns
        for pattern in cls.PLAYLIST_PATTERNS:
            match = re.search(pattern, url, re.IGNORECASE)
            if match:
                identifier = match.group(1)
                return {
                    "source_type": SourceType.PLAYLIST.value,
                    "identifier": identifier,
                    "normalized_url": f"https://www.youtube.com/playlist?list={identifier}",
                    "original_url": url
                }
        
        return None
    
    @classmethod
    def _normalize_channel_url(cls, original_url: str, identifier: str) -> str:
        """
        Normalize channel URL to standard format.
        
        Args:
            original_url: Original URL provided
            identifier: Extracted channel identifier
            
        Returns:
            Normalized YouTube channel URL
        """
        # If it's already a channel ID (starts with UC), use channel format
        if identifier.startswith('UC'):
            return f"https://www.youtube.com/channel/{identifier}"
        
        # For @username format, preserve it
        if '@' in original_url:
            return f"https://www.youtube.com/@{identifier}"
        
        # For /c/ format, preserve it
        if '/c/' in original_url:
            return f"https://www.youtube.com/c/{identifier}"
        
        # For /user/ format, preserve it
        if '/user/' in original_url:
            return f"https://www.youtube.com/user/{identifier}"
        
        # Default to @username format for clean URLs
        return f"https://www.youtube.com/@{identifier}"
    
    @classmethod
    def validate_url(cls, url: str) -> bool:
        """
        Validate if URL is a supported YouTube channel or playlist.
        
        Args:
            url: URL to validate
            
        Returns:
            True if valid YouTube URL, False otherwise
        """
        return cls.parse_url(url) is not None
    
    @classmethod
    def get_source_type(cls, url: str) -> Optional[SourceType]:
        """
        Get the source type (channel or playlist) from URL.
        
        Args:
            url: YouTube URL
            
        Returns:
            SourceType enum value or None if invalid
        """
        parsed = cls.parse_url(url)
        if parsed:
            return SourceType(parsed["source_type"])
        return None
    
    @classmethod
    def extract_identifier(cls, url: str) -> Optional[str]:
        """
        Extract the unique identifier from YouTube URL.
        
        Args:
            url: YouTube URL
            
        Returns:
            Channel ID, username, or playlist ID
        """
        parsed = cls.parse_url(url)
        if parsed:
            return parsed["identifier"]
        return None
    
    @classmethod
    def normalize_url(cls, url: str) -> Optional[str]:
        """
        Normalize YouTube URL to standard format.
        
        Args:
            url: YouTube URL to normalize
            
        Returns:
            Normalized URL or None if invalid
        """
        parsed = cls.parse_url(url)
        if parsed:
            return parsed["normalized_url"]
        return None


def validate_youtube_url(url: str) -> bool:
    """
    Simple validation function for YouTube URLs.
    
    Args:
        url: URL to validate
        
    Returns:
        True if valid YouTube URL
    """
    return YouTubeURLParser.validate_url(url)


def parse_youtube_url(url: str) -> Optional[Dict[str, str]]:
    """
    Parse YouTube URL and return metadata.
    
    Args:
        url: YouTube URL to parse
        
    Returns:
        Parsed URL metadata or None if invalid
    """
    return YouTubeURLParser.parse_url(url)


def get_youtube_source_type(url: str) -> Optional[str]:
    """
    Get source type from YouTube URL.
    
    Args:
        url: YouTube URL
        
    Returns:
        'channel' or 'playlist' or None if invalid
    """
    source_type = YouTubeURLParser.get_source_type(url)
    return source_type.value if source_type else None


def normalize_youtube_url(url: str) -> Optional[str]:
    """
    Normalize YouTube URL to standard format.
    
    Args:
        url: YouTube URL to normalize
        
    Returns:
        Normalized URL or None if invalid
    """
    return YouTubeURLParser.normalize_url(url) 