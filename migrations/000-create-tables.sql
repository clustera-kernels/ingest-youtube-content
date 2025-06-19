-- Clustera YouTube Ingest - Database Schema Migration
-- Stage 0: Create all required tables and indexes

-- Control table for managing YouTube sources
CREATE TABLE IF NOT EXISTS ctrl_youtube_lists (
    id SERIAL PRIMARY KEY,
    source_type VARCHAR(50) NOT NULL, -- 'channel' or 'playlist'
    source_url TEXT NOT NULL UNIQUE,
    source_name TEXT,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_sync_at TIMESTAMP,
    sync_frequency_hours INTEGER DEFAULT 24
);

-- Dataset table for YouTube video metadata
CREATE TABLE IF NOT EXISTS dataset_youtube_video (
    id SERIAL PRIMARY KEY,
    video_id VARCHAR(20) NOT NULL UNIQUE,
    video_url TEXT NOT NULL,
    title TEXT,
    description TEXT,
    channel_id VARCHAR(50),
    channel_name TEXT,
    channel_url TEXT,
    playlist_id VARCHAR(50),
    playlist_name TEXT,
    duration TEXT, -- Format: "HH:MM:SS" or "MM:SS"
    duration_seconds INTEGER,
    view_count BIGINT,
    like_count BIGINT,
    comment_count BIGINT,
    published_at TEXT, -- Can be relative like "2 years ago" or date
    published_date DATE, -- Parsed date when available
    transcript JSONB, -- Array of {start, dur, text} objects
    transcript_text TEXT, -- Full concatenated transcript
    transcript_language VARCHAR(10),
    thumbnail_url TEXT,
    tags TEXT[], -- Array of tags
    category VARCHAR(100),
    is_live_content BOOLEAN DEFAULT false,
    is_monetized BOOLEAN,
    comments_turned_off BOOLEAN,
    location TEXT,
    description_links JSONB, -- Array of {url, text} objects
    subtitles JSONB, -- Additional subtitle formats if available
    from_yt_url TEXT, -- Source URL this video was found from
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    transcript_ingested_at TIMESTAMP,
    metadata_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_list_id INTEGER REFERENCES ctrl_youtube_lists(id)
);

-- Dataset table for YouTube channel metadata
CREATE TABLE IF NOT EXISTS dataset_youtube_channel (
    id SERIAL PRIMARY KEY,
    channel_id VARCHAR(50) NOT NULL UNIQUE,
    channel_name TEXT,
    channel_url TEXT,
    channel_description TEXT,
    channel_description_links JSONB, -- Array of {url, text} objects
    channel_joined_date TEXT,
    channel_location TEXT,
    channel_total_videos INTEGER,
    channel_total_views TEXT, -- Can be formatted like "1,710,167,563"
    channel_total_views_numeric BIGINT,
    number_of_subscribers BIGINT,
    is_monetized BOOLEAN,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Control table for ingestion logging
CREATE TABLE IF NOT EXISTS ctrl_ingestion_log (
    id SERIAL PRIMARY KEY,
    stage_name VARCHAR(50) NOT NULL,
    source_type VARCHAR(50),
    source_identifier TEXT,
    status VARCHAR(20) NOT NULL, -- 'started', 'completed', 'failed'
    error_message TEXT,
    records_processed INTEGER DEFAULT 0,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    apify_run_id VARCHAR(100),
    apify_dataset_id VARCHAR(100)
);

-- Performance indexes
CREATE INDEX IF NOT EXISTS idx_youtube_video_video_id ON dataset_youtube_video(video_id);
CREATE INDEX IF NOT EXISTS idx_youtube_video_channel_id ON dataset_youtube_video(channel_id);
CREATE INDEX IF NOT EXISTS idx_youtube_video_source_list_id ON dataset_youtube_video(source_list_id);
CREATE INDEX IF NOT EXISTS idx_youtube_video_ingested_at ON dataset_youtube_video(ingested_at);
CREATE INDEX IF NOT EXISTS idx_youtube_channel_channel_id ON dataset_youtube_channel(channel_id);
CREATE INDEX IF NOT EXISTS idx_youtube_lists_source_url ON ctrl_youtube_lists(source_url);
CREATE INDEX IF NOT EXISTS idx_youtube_lists_is_active ON ctrl_youtube_lists(is_active);
CREATE INDEX IF NOT EXISTS idx_ingestion_log_stage_status ON ctrl_ingestion_log(stage_name, status);
CREATE INDEX IF NOT EXISTS idx_ingestion_log_started_at ON ctrl_ingestion_log(started_at);

-- Full-text search index for transcript content
CREATE INDEX IF NOT EXISTS idx_youtube_video_transcript_text ON dataset_youtube_video USING gin(to_tsvector('english', transcript_text)); 