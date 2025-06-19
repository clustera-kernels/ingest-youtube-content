"""
SQLAlchemy ORM models for Clustera YouTube Ingest database schema.
"""

from datetime import datetime
from typing import Optional, List, Dict, Any

from sqlalchemy import (
    Column, Integer, String, Text, Boolean, DateTime, Date, BigInteger,
    ForeignKey, Index, ARRAY
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

Base = declarative_base()


class CtrlYouTubeList(Base):
    """Control table for managing YouTube sources (channels/playlists)."""
    
    __tablename__ = "ctrl_youtube_lists"
    
    id = Column(Integer, primary_key=True)
    source_type = Column(String(50), nullable=False)  # 'channel' or 'playlist'
    source_url = Column(Text, nullable=False, unique=True)
    source_name = Column(Text)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=func.current_timestamp())
    updated_at = Column(DateTime, default=func.current_timestamp(), onupdate=func.current_timestamp())
    last_sync_at = Column(DateTime)
    sync_frequency_hours = Column(Integer, default=24)
    resource_pool = Column(String(100), nullable=True)  # Nullable for backward compatibility
    
    # Relationship to videos
    videos = relationship("DatasetYouTubeVideo", back_populates="source_list")
    
    __table_args__ = (
        Index('idx_youtube_lists_source_url', 'source_url'),
        Index('idx_youtube_lists_is_active', 'is_active'),
        Index('idx_youtube_lists_resource_pool', 'resource_pool'),
    )


class DatasetYouTubeVideo(Base):
    """Dataset table for YouTube video metadata."""
    
    __tablename__ = "dataset_youtube_video"
    
    id = Column(Integer, primary_key=True)
    video_id = Column(String(20), nullable=False, unique=True)
    video_url = Column(Text, nullable=False)
    title = Column(Text)
    description = Column(Text)
    channel_id = Column(String(50))
    channel_name = Column(Text)
    channel_url = Column(Text)
    playlist_id = Column(String(50))
    playlist_name = Column(Text)
    duration = Column(Text)  # Format: "HH:MM:SS" or "MM:SS"
    duration_seconds = Column(Integer)
    view_count = Column(BigInteger)
    like_count = Column(BigInteger)
    comment_count = Column(BigInteger)
    published_at = Column(Text)  # Can be relative like "2 years ago"
    published_date = Column(Date)  # Parsed date when available
    transcript = Column(JSONB)  # Array of {start, dur, text} objects
    transcript_text = Column(Text)  # Full concatenated transcript
    transcript_language = Column(String(10))
    thumbnail_url = Column(Text)
    tags = Column(ARRAY(Text))  # Array of tags
    category = Column(String(100))
    is_live_content = Column(Boolean, default=False)
    is_monetized = Column(Boolean)
    comments_turned_off = Column(Boolean)
    location = Column(Text)
    description_links = Column(JSONB)  # Array of {url, text} objects
    subtitles = Column(JSONB)  # Additional subtitle formats
    from_yt_url = Column(Text)  # Source URL this video was found from
    ingested_at = Column(DateTime, default=func.current_timestamp())
    transcript_ingested_at = Column(DateTime)
    metadata_updated_at = Column(DateTime, default=func.current_timestamp())
    source_list_id = Column(Integer, ForeignKey('ctrl_youtube_lists.id'))
    resource_pool = Column(String(100), nullable=True)  # Nullable for backward compatibility
    
    # Relationships
    source_list = relationship("CtrlYouTubeList", back_populates="videos")
    
    __table_args__ = (
        Index('idx_youtube_video_video_id', 'video_id'),
        Index('idx_youtube_video_channel_id', 'channel_id'),
        Index('idx_youtube_video_source_list_id', 'source_list_id'),
        Index('idx_youtube_video_ingested_at', 'ingested_at'),
        Index('idx_youtube_video_resource_pool', 'resource_pool'),
    )


class DatasetYouTubeChannel(Base):
    """Dataset table for YouTube channel metadata."""
    
    __tablename__ = "dataset_youtube_channel"
    
    id = Column(Integer, primary_key=True)
    channel_id = Column(String(50), nullable=False, unique=True)
    channel_name = Column(Text)
    channel_url = Column(Text)
    channel_description = Column(Text)
    channel_description_links = Column(JSONB)  # Array of {url, text} objects
    channel_joined_date = Column(Text)
    channel_location = Column(Text)
    channel_total_videos = Column(Integer)
    channel_total_views = Column(Text)  # Can be formatted like "1,710,167,563"
    channel_total_views_numeric = Column(BigInteger)
    number_of_subscribers = Column(BigInteger)
    is_monetized = Column(Boolean)
    ingested_at = Column(DateTime, default=func.current_timestamp())
    updated_at = Column(DateTime, default=func.current_timestamp(), onupdate=func.current_timestamp())
    resource_pool = Column(String(100), nullable=True)  # Nullable for backward compatibility
    
    __table_args__ = (
        Index('idx_youtube_channel_channel_id', 'channel_id'),
        Index('idx_youtube_channel_resource_pool', 'resource_pool'),
    )


class CtrlIngestionLog(Base):
    """Control table for ingestion logging."""
    
    __tablename__ = "ctrl_ingestion_log"
    
    id = Column(Integer, primary_key=True)
    stage_name = Column(String(50), nullable=False)
    source_type = Column(String(50))
    source_identifier = Column(Text)
    status = Column(String(20), nullable=False)  # 'started', 'completed', 'failed'
    error_message = Column(Text)
    records_processed = Column(Integer, default=0)
    started_at = Column(DateTime, default=func.current_timestamp())
    completed_at = Column(DateTime)
    apify_run_id = Column(String(100))
    apify_dataset_id = Column(String(100))
    resource_pool = Column(String(100), nullable=True)  # Nullable for backward compatibility
    
    __table_args__ = (
        Index('idx_ingestion_log_stage_status', 'stage_name', 'status'),
        Index('idx_ingestion_log_started_at', 'started_at'),
        Index('idx_ingestion_log_resource_pool', 'resource_pool'),
    ) 