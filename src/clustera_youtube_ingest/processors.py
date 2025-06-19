"""
Data processors for YouTube metadata parsing and validation.

Handles parsing of video metadata, channel information, and date conversion.
"""

import re
import logging
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, date, timedelta
from urllib.parse import urlparse, parse_qs


logger = logging.getLogger(__name__)


class VideoProcessor:
    """Processes and validates YouTube video metadata."""
    
    @staticmethod
    def parse_video_data(raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse raw video data from Apify scraper.
        
        Args:
            raw_data: Raw video data from YouTube scraper
            
        Returns:
            Processed video data dict
        """
        try:
            # Extract video ID from URL
            video_id = VideoProcessor.extract_video_id(raw_data.get('url', ''))
            if not video_id:
                logger.warning(f"Could not extract video ID from URL: {raw_data.get('url')}")
                return {}
            
            # Parse duration
            duration_str, duration_seconds = VideoProcessor.parse_duration(
                raw_data.get('duration', '')
            )
            
            # Parse view count
            view_count = VideoProcessor.parse_view_count(raw_data.get('viewCount', '0'))
            
            # Parse like count (Apify uses 'likes' not 'likeCount')
            like_count = VideoProcessor.parse_count(raw_data.get('likes', '0'))
            
            # Parse comment count (Apify uses 'commentsCount' not 'commentCount')
            comment_count = VideoProcessor.parse_count(raw_data.get('commentsCount', '0'))
            
            # Get description (Apify uses 'text' not 'description')
            description = raw_data.get('text', '').strip()
            
            # Extract tags from description
            tags = VideoProcessor.extract_tags(description)
            
            # Extract description links (Apify provides 'descriptionLinks' directly)
            description_links = raw_data.get('descriptionLinks', [])
            if not description_links:
                description_links = VideoProcessor.extract_links(description)
            
            processed_data = {
                'video_id': video_id,
                'video_url': f"https://www.youtube.com/watch?v={video_id}",
                'title': raw_data.get('title', '').strip(),
                'description': description,
                'channel_id': raw_data.get('channelId', ''),
                'channel_name': raw_data.get('channelName', '').strip(),
                'channel_url': raw_data.get('channelUrl', ''),
                'playlist_id': raw_data.get('playlistId'),
                'playlist_name': raw_data.get('playlistName'),
                'duration': duration_str,
                'duration_seconds': duration_seconds,
                'view_count': view_count,
                'like_count': like_count,
                'comment_count': comment_count,
                'published_at': raw_data.get('date', '').strip(),  # Apify uses 'date' not 'publishedAt'
                'thumbnail_url': raw_data.get('thumbnailUrl', ''),
                'tags': tags,
                'category': raw_data.get('category', ''),  # May not be provided by Apify
                'is_live_content': raw_data.get('isLiveContent', False),
                'is_monetized': raw_data.get('isMonetized'),
                'comments_turned_off': raw_data.get('commentsTurnedOff', False),
                'location': raw_data.get('location', ''),
                'description_links': description_links,
                'subtitles': raw_data.get('subtitles'),
            }
            
            # Remove None values
            return {k: v for k, v in processed_data.items() if v is not None}
            
        except Exception as e:
            logger.error(f"Error processing video data: {str(e)}")
            return {}
    
    @staticmethod
    def extract_video_id(url: str) -> Optional[str]:
        """
        Extract video ID from YouTube URL.
        
        Args:
            url: YouTube video URL
            
        Returns:
            11-character video ID or None if invalid
        """
        if not url:
            return None
        
        # Handle various YouTube URL formats
        patterns = [
            r'(?:youtube\.com/watch\?v=|youtu\.be/|youtube\.com/embed/)([a-zA-Z0-9_-]{11})',
            r'youtube\.com/v/([a-zA-Z0-9_-]{11})',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                return match.group(1)
        
        # If URL is already just the video ID
        if re.match(r'^[a-zA-Z0-9_-]{11}$', url):
            return url
        
        return None
    
    @staticmethod
    def parse_duration(duration_str: str) -> Tuple[str, int]:
        """
        Parse video duration string to standardized format.
        
        Args:
            duration_str: Duration string (e.g., "10:30", "1:05:30")
            
        Returns:
            Tuple of (formatted_duration, total_seconds)
        """
        if not duration_str:
            return "", 0
        
        try:
            # Remove any non-digit/colon characters
            clean_duration = re.sub(r'[^\d:]', '', duration_str)
            
            if not clean_duration:
                return "", 0
            
            parts = clean_duration.split(':')
            total_seconds = 0
            
            if len(parts) == 2:  # MM:SS
                minutes, seconds = map(int, parts)
                total_seconds = minutes * 60 + seconds
                formatted = f"{minutes:02d}:{seconds:02d}"
            elif len(parts) == 3:  # HH:MM:SS
                hours, minutes, seconds = map(int, parts)
                total_seconds = hours * 3600 + minutes * 60 + seconds
                formatted = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
            else:
                return duration_str, 0
            
            return formatted, total_seconds
            
        except (ValueError, IndexError) as e:
            logger.warning(f"Could not parse duration '{duration_str}': {str(e)}")
            return duration_str, 0
    
    @staticmethod
    def parse_view_count(view_str: str) -> int:
        """Parse view count string to integer."""
        return VideoProcessor.parse_count(view_str)
    
    @staticmethod
    def parse_count(count_str) -> int:
        """
        Parse count string (views, likes, comments) to integer.
        
        Args:
            count_str: Count string or number (e.g., "1.2M", "1,234", "5K", 12345)
            
        Returns:
            Integer count value
        """
        if count_str is None:
            return 0
        
        # Convert to string first
        count_str = str(count_str).strip()
        
        if not count_str or count_str.lower() in ['none', 'n/a', '']:
            return 0
        
        try:
            # Remove commas and spaces
            clean_str = re.sub(r'[,\s]', '', count_str)
            
            # Handle K, M, B suffixes
            multipliers = {'K': 1000, 'M': 1000000, 'B': 1000000000}
            
            for suffix, multiplier in multipliers.items():
                if clean_str.upper().endswith(suffix):
                    number_part = clean_str[:-1]
                    return int(float(number_part) * multiplier)
            
            # Try direct conversion
            return int(float(clean_str))
            
        except (ValueError, TypeError) as e:
            logger.warning(f"Could not parse count '{count_str}': {str(e)}")
            return 0
    
    @staticmethod
    def extract_tags(description: str) -> List[str]:
        """
        Extract hashtags from video description.
        
        Args:
            description: Video description text
            
        Returns:
            List of hashtags without # symbol
        """
        if not description:
            return []
        
        # Find hashtags
        hashtags = re.findall(r'#(\w+)', description)
        return list(set(hashtags))  # Remove duplicates
    
    @staticmethod
    def extract_links(text: str) -> List[Dict[str, str]]:
        """
        Extract links from text.
        
        Args:
            text: Text containing potential links
            
        Returns:
            List of dicts with 'url' and 'text' keys
        """
        if not text:
            return []
        
        links = []
        
        # Find URLs
        url_pattern = r'https?://[^\s<>"{}|\\^`\[\]]+'
        urls = re.findall(url_pattern, text)
        
        for url in urls:
            # Try to extract surrounding text as context
            url_index = text.find(url)
            start = max(0, url_index - 50)
            end = min(len(text), url_index + len(url) + 50)
            context = text[start:end].strip()
            
            links.append({
                'url': url,
                'text': context
            })
        
        return links


class ChannelProcessor:
    """Processes and validates YouTube channel metadata."""
    
    @staticmethod
    def parse_channel_data(raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse raw channel data from Apify scraper.
        
        Args:
            raw_data: Raw channel data from YouTube scraper
            
        Returns:
            Processed channel data dict
        """
        try:
            # Parse subscriber count (from video data, field is 'numberOfSubscribers')
            subscriber_count = ChannelProcessor.normalize_subscriber_count(
                raw_data.get('numberOfSubscribers', '0')
            )
            
            # Parse total views (from video data, field is 'channelTotalViews')
            total_views_str = raw_data.get('channelTotalViews', '0')
            total_views_numeric = VideoProcessor.parse_count(total_views_str)
            
            # Extract description links (from video data, field is 'channelDescriptionLinks')
            description_links = raw_data.get('channelDescriptionLinks', [])
            if not description_links:
                description_links = VideoProcessor.extract_links(
                    raw_data.get('channelDescription', '')
                )
            
            processed_data = {
                'channel_id': raw_data.get('channelId', ''),
                'channel_name': raw_data.get('channelName', '').strip(),
                'channel_url': raw_data.get('channelUrl', ''),
                'channel_description': raw_data.get('channelDescription', '').strip(),
                'channel_description_links': description_links,
                'channel_joined_date': raw_data.get('channelJoinedDate', ''),
                'channel_location': raw_data.get('channelLocation', ''),
                'channel_total_videos': raw_data.get('channelTotalVideos'),
                'channel_total_views': str(total_views_str),
                'channel_total_views_numeric': total_views_numeric,
                'number_of_subscribers': subscriber_count,
                'is_monetized': raw_data.get('isMonetized'),
            }
            
            # Remove None values
            return {k: v for k, v in processed_data.items() if v is not None}
            
        except Exception as e:
            logger.error(f"Error processing channel data: {str(e)}")
            return {}
    
    @staticmethod
    def normalize_subscriber_count(sub_str) -> int:
        """
        Normalize subscriber count string to integer.
        
        Args:
            sub_str: Subscriber count string or number
            
        Returns:
            Integer subscriber count
        """
        return VideoProcessor.parse_count(sub_str)
    
    @staticmethod
    def extract_channel_links(description: str) -> List[Dict[str, str]]:
        """Extract social media and other links from channel description."""
        return VideoProcessor.extract_links(description)


class DateParser:
    """Handles parsing of relative and absolute dates."""
    
    @staticmethod
    def parse_relative_date(relative_str) -> Optional[date]:
        """
        Parse relative date string to approximate date.
        
        Args:
            relative_str: Relative date string (e.g., "2 years ago", "3 months ago")
            
        Returns:
            Approximate date or None if unparseable
        """
        if not relative_str:
            return None
        
        try:
            # Convert to string and normalize
            clean_str = str(relative_str).lower().strip()
            
            # Extract number and unit
            match = re.search(r'(\d+)\s*(year|month|week|day|hour|minute)s?\s*ago', clean_str)
            if not match:
                return None
            
            amount = int(match.group(1))
            unit = match.group(2)
            
            # Calculate approximate date
            now = datetime.now().date()
            
            if unit == 'year':
                return date(now.year - amount, now.month, now.day)
            elif unit == 'month':
                # Approximate month calculation
                total_months = now.year * 12 + now.month - amount
                year = total_months // 12
                month = total_months % 12
                if month == 0:
                    month = 12
                    year -= 1
                return date(year, month, min(now.day, 28))  # Safe day
            elif unit == 'week':
                return now - timedelta(weeks=amount)
            elif unit == 'day':
                return now - timedelta(days=amount)
            elif unit == 'hour':
                return now - timedelta(hours=amount)
            elif unit == 'minute':
                return now - timedelta(minutes=amount)
            
        except (ValueError, TypeError) as e:
            logger.warning(f"Could not parse relative date '{relative_str}': {str(e)}")
        
        return None
    
    @staticmethod
    def extract_published_date(published_str) -> Tuple[str, Optional[date]]:
        """
        Extract and parse published date from string.
        
        Args:
            published_str: Published date string
            
        Returns:
            Tuple of (original_string, parsed_date)
        """
        if not published_str:
            return "", None
        
        # Convert to string
        published_str = str(published_str)
        
        # Try relative date parsing first
        parsed_date = DateParser.parse_relative_date(published_str)
        
        if parsed_date:
            return published_str, parsed_date
        
        # Try absolute date parsing
        date_patterns = [
            r'(\d{4}-\d{2}-\d{2})',  # YYYY-MM-DD
            r'(\d{2}/\d{2}/\d{4})',  # MM/DD/YYYY
            r'(\d{2}-\d{2}-\d{4})',  # MM-DD-YYYY
        ]
        
        for pattern in date_patterns:
            match = re.search(pattern, published_str)
            if match:
                try:
                    date_str = match.group(1)
                    if '-' in date_str and len(date_str.split('-')[0]) == 4:
                        # YYYY-MM-DD
                        parsed_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                    elif '/' in date_str:
                        # MM/DD/YYYY
                        parsed_date = datetime.strptime(date_str, '%m/%d/%Y').date()
                    elif '-' in date_str:
                        # MM-DD-YYYY
                        parsed_date = datetime.strptime(date_str, '%m-%d-%Y').date()
                    
                    return published_str, parsed_date
                except ValueError:
                    continue
        
        return published_str, None


class TranscriptProcessor:
    """Processes and validates YouTube video transcripts."""
    
    @staticmethod
    def process_transcript_data(raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process raw transcript data from Apify scraper.
        
        Args:
            raw_data: Raw transcript data from transcript scraper
            
        Returns:
            Processed transcript data dict
        """
        try:
            # Extract transcript segments
            transcript_segments = raw_data.get('transcript', [])
            if not transcript_segments:
                # Try alternative field names
                transcript_segments = raw_data.get('transcriptSegments', [])
                if not transcript_segments:
                    transcript_segments = raw_data.get('captions', [])
                    if not transcript_segments:
                        transcript_segments = raw_data.get('data', [])
            
            # Process segments
            processed_segments = []
            full_text_parts = []
            
            for segment in transcript_segments:
                if isinstance(segment, dict):
                    # Normalize segment structure
                    processed_segment = {
                        'start': TranscriptProcessor._parse_timestamp(segment.get('start', segment.get('startTime', 0))),
                        'dur': TranscriptProcessor._parse_duration(segment.get('dur', segment.get('duration', 0))),
                        'text': TranscriptProcessor._clean_text(segment.get('text', ''))
                    }
                    
                    if processed_segment['text']:  # Only include segments with text
                        processed_segments.append(processed_segment)
                        full_text_parts.append(processed_segment['text'])
                elif isinstance(segment, str):
                    # Handle plain text segments
                    cleaned_text = TranscriptProcessor._clean_text(segment)
                    if cleaned_text:
                        processed_segments.append({
                            'start': 0,
                            'dur': 0,
                            'text': cleaned_text
                        })
                        full_text_parts.append(cleaned_text)
            
            # Generate full text
            full_text = ' '.join(full_text_parts).strip()
            
            # Detect language
            language = TranscriptProcessor._detect_language(raw_data, full_text)
            
            # Calculate quality metrics
            quality_score = TranscriptProcessor._calculate_quality_score(processed_segments, full_text)
            
            return {
                'segments': processed_segments,
                'text': full_text,
                'language': language,
                'segment_count': len(processed_segments),
                'total_duration': sum(seg.get('dur', 0) for seg in processed_segments),
                'quality_score': quality_score,
                'word_count': len(full_text.split()) if full_text else 0
            }
            
        except Exception as e:
            logger.error(f"Error processing transcript data: {str(e)}")
            return {
                'segments': [],
                'text': '',
                'language': 'unknown',
                'segment_count': 0,
                'total_duration': 0,
                'quality_score': 0.0,
                'word_count': 0
            }
    
    @staticmethod
    def validate_quality(processed_data: Dict[str, Any], min_length: int = 50, 
                        quality_threshold: float = 0.7, allowed_languages: List[str] = None) -> bool:
        """
        Validate transcript quality against thresholds.
        
        Args:
            processed_data: Processed transcript data
            min_length: Minimum text length required
            quality_threshold: Minimum quality score required
            allowed_languages: List of allowed language codes
            
        Returns:
            True if transcript meets quality requirements
        """
        try:
            # Check minimum length
            text_length = len(processed_data.get('text', ''))
            if text_length < min_length:
                logger.debug(f"Transcript too short: {text_length} < {min_length}")
                return False
            
            # Check quality score
            quality_score = processed_data.get('quality_score', 0.0)
            if quality_score < quality_threshold:
                logger.debug(f"Quality score too low: {quality_score} < {quality_threshold}")
                return False
            
            # Check language if filter is specified
            if allowed_languages:
                language = processed_data.get('language', 'unknown')
                if language not in allowed_languages and 'unknown' not in allowed_languages:
                    logger.debug(f"Language not allowed: {language} not in {allowed_languages}")
                    return False
            
            # Check segment count (should have some structure)
            segment_count = processed_data.get('segment_count', 0)
            if segment_count == 0:
                logger.debug("No transcript segments found")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error validating transcript quality: {str(e)}")
            return False
    
    @staticmethod
    def _parse_timestamp(timestamp) -> float:
        """Parse timestamp to float seconds."""
        try:
            if isinstance(timestamp, (int, float)):
                return float(timestamp)
            elif isinstance(timestamp, str):
                # Handle various timestamp formats
                if ':' in timestamp:
                    # Format: "MM:SS" or "HH:MM:SS"
                    parts = timestamp.split(':')
                    if len(parts) == 2:
                        return float(parts[0]) * 60 + float(parts[1])
                    elif len(parts) == 3:
                        return float(parts[0]) * 3600 + float(parts[1]) * 60 + float(parts[2])
                else:
                    return float(timestamp)
            return 0.0
        except (ValueError, TypeError):
            return 0.0
    
    @staticmethod
    def _parse_duration(duration) -> float:
        """Parse duration to float seconds."""
        return TranscriptProcessor._parse_timestamp(duration)
    
    @staticmethod
    def _clean_text(text: str) -> str:
        """Clean and normalize transcript text."""
        if not text:
            return ''
        
        # Remove extra whitespace
        cleaned = re.sub(r'\s+', ' ', text.strip())
        
        # Remove common transcript artifacts
        cleaned = re.sub(r'\[.*?\]', '', cleaned)  # Remove [Music], [Applause], etc.
        cleaned = re.sub(r'\(.*?\)', '', cleaned)  # Remove (inaudible), etc.
        
        # Remove speaker labels if present
        cleaned = re.sub(r'^[A-Z\s]+:', '', cleaned)
        
        # Final cleanup
        cleaned = cleaned.strip()
        
        return cleaned
    
    @staticmethod
    def _detect_language(raw_data: Dict[str, Any], text: str) -> str:
        """Detect transcript language."""
        # First check if language is provided in raw data
        language = raw_data.get('language', raw_data.get('lang', ''))
        if language:
            return language.lower()[:2]  # Normalize to 2-letter code
        
        # Simple language detection based on common words
        if not text:
            return 'unknown'
        
        text_lower = text.lower()
        
        # English indicators
        english_words = ['the', 'and', 'is', 'in', 'to', 'of', 'a', 'that', 'it', 'with']
        english_count = sum(1 for word in english_words if word in text_lower)
        
        # Spanish indicators
        spanish_words = ['el', 'la', 'de', 'que', 'y', 'en', 'un', 'es', 'se', 'no']
        spanish_count = sum(1 for word in spanish_words if word in text_lower)
        
        # French indicators
        french_words = ['le', 'de', 'et', 'à', 'un', 'il', 'être', 'et', 'en', 'avoir']
        french_count = sum(1 for word in french_words if word in text_lower)
        
        # Determine language based on highest count
        counts = {'en': english_count, 'es': spanish_count, 'fr': french_count}
        detected_lang = max(counts, key=counts.get)
        
        # Only return detected language if confidence is reasonable
        if counts[detected_lang] >= 2:
            return detected_lang
        
        return 'unknown'
    
    @staticmethod
    def _calculate_quality_score(segments: List[Dict], full_text: str) -> float:
        """Calculate transcript quality score (0.0 to 1.0)."""
        try:
            score = 0.0
            
            # Factor 1: Segment structure (0.3 weight)
            if segments:
                # Check for proper timing
                timed_segments = sum(1 for seg in segments if seg.get('start', 0) > 0)
                timing_ratio = timed_segments / len(segments) if segments else 0
                score += timing_ratio * 0.3
            
            # Factor 2: Text completeness (0.4 weight)
            if full_text:
                word_count = len(full_text.split())
                # Reasonable transcript should have decent word count
                completeness = min(word_count / 100, 1.0)  # Normalize to 100 words
                score += completeness * 0.4
            
            # Factor 3: Text quality (0.3 weight)
            if full_text:
                # Check for proper sentences
                sentence_count = len([s for s in full_text.split('.') if s.strip()])
                avg_sentence_length = len(full_text.split()) / max(sentence_count, 1)
                
                # Good sentences are 5-25 words
                sentence_quality = 1.0 if 5 <= avg_sentence_length <= 25 else 0.5
                score += sentence_quality * 0.3
            
            return min(score, 1.0)
            
        except Exception as e:
            logger.error(f"Error calculating quality score: {str(e)}")
            return 0.0 