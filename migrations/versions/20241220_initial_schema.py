"""Initial schema creation

Revision ID: 001_initial_schema
Revises: 
Create Date: 2024-12-20 12:00:00.000000

This migration creates the initial database schema for Clustera YouTube Ingest.
It replaces the original 000-create-tables.sql migration.
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB, ARRAY

# revision identifiers, used by Alembic.
revision = '001_initial_schema'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create initial database schema."""
    
    # Create ctrl_youtube_lists table
    op.create_table(
        'ctrl_youtube_lists',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('source_type', sa.String(50), nullable=False),
        sa.Column('source_url', sa.Text(), nullable=False),
        sa.Column('source_name', sa.Text(), nullable=True),
        sa.Column('is_active', sa.Boolean(), nullable=True, default=True),
        sa.Column('created_at', sa.DateTime(), nullable=True, server_default=sa.func.current_timestamp()),
        sa.Column('updated_at', sa.DateTime(), nullable=True, server_default=sa.func.current_timestamp()),
        sa.Column('last_sync_at', sa.DateTime(), nullable=True),
        sa.Column('sync_frequency_hours', sa.Integer(), nullable=True, default=24),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('source_url')
    )
    
    # Create dataset_youtube_video table
    op.create_table(
        'dataset_youtube_video',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('video_id', sa.String(20), nullable=False),
        sa.Column('video_url', sa.Text(), nullable=False),
        sa.Column('title', sa.Text(), nullable=True),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('channel_id', sa.String(50), nullable=True),
        sa.Column('channel_name', sa.Text(), nullable=True),
        sa.Column('channel_url', sa.Text(), nullable=True),
        sa.Column('playlist_id', sa.String(50), nullable=True),
        sa.Column('playlist_name', sa.Text(), nullable=True),
        sa.Column('duration', sa.Text(), nullable=True),
        sa.Column('duration_seconds', sa.Integer(), nullable=True),
        sa.Column('view_count', sa.BigInteger(), nullable=True),
        sa.Column('like_count', sa.BigInteger(), nullable=True),
        sa.Column('comment_count', sa.BigInteger(), nullable=True),
        sa.Column('published_at', sa.Text(), nullable=True),
        sa.Column('published_date', sa.Date(), nullable=True),
        sa.Column('transcript', JSONB(), nullable=True),
        sa.Column('transcript_text', sa.Text(), nullable=True),
        sa.Column('transcript_language', sa.String(10), nullable=True),
        sa.Column('thumbnail_url', sa.Text(), nullable=True),
        sa.Column('tags', ARRAY(sa.Text()), nullable=True),
        sa.Column('category', sa.String(100), nullable=True),
        sa.Column('is_live_content', sa.Boolean(), nullable=True, default=False),
        sa.Column('is_monetized', sa.Boolean(), nullable=True),
        sa.Column('comments_turned_off', sa.Boolean(), nullable=True),
        sa.Column('location', sa.Text(), nullable=True),
        sa.Column('description_links', JSONB(), nullable=True),
        sa.Column('subtitles', JSONB(), nullable=True),
        sa.Column('from_yt_url', sa.Text(), nullable=True),
        sa.Column('ingested_at', sa.DateTime(), nullable=True, server_default=sa.func.current_timestamp()),
        sa.Column('transcript_ingested_at', sa.DateTime(), nullable=True),
        sa.Column('metadata_updated_at', sa.DateTime(), nullable=True, server_default=sa.func.current_timestamp()),
        sa.Column('source_list_id', sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(['source_list_id'], ['ctrl_youtube_lists.id']),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('video_id')
    )
    
    # Create dataset_youtube_channel table
    op.create_table(
        'dataset_youtube_channel',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('channel_id', sa.String(50), nullable=False),
        sa.Column('channel_name', sa.Text(), nullable=True),
        sa.Column('channel_url', sa.Text(), nullable=True),
        sa.Column('channel_description', sa.Text(), nullable=True),
        sa.Column('channel_description_links', JSONB(), nullable=True),
        sa.Column('channel_joined_date', sa.Text(), nullable=True),
        sa.Column('channel_location', sa.Text(), nullable=True),
        sa.Column('channel_total_videos', sa.Integer(), nullable=True),
        sa.Column('channel_total_views', sa.Text(), nullable=True),
        sa.Column('channel_total_views_numeric', sa.BigInteger(), nullable=True),
        sa.Column('number_of_subscribers', sa.BigInteger(), nullable=True),
        sa.Column('is_monetized', sa.Boolean(), nullable=True),
        sa.Column('ingested_at', sa.DateTime(), nullable=True, server_default=sa.func.current_timestamp()),
        sa.Column('updated_at', sa.DateTime(), nullable=True, server_default=sa.func.current_timestamp()),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('channel_id')
    )
    
    # Create ctrl_ingestion_log table
    op.create_table(
        'ctrl_ingestion_log',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('stage_name', sa.String(50), nullable=False),
        sa.Column('source_type', sa.String(50), nullable=True),
        sa.Column('source_identifier', sa.Text(), nullable=True),
        sa.Column('status', sa.String(20), nullable=False),
        sa.Column('error_message', sa.Text(), nullable=True),
        sa.Column('records_processed', sa.Integer(), nullable=True, default=0),
        sa.Column('started_at', sa.DateTime(), nullable=True, server_default=sa.func.current_timestamp()),
        sa.Column('completed_at', sa.DateTime(), nullable=True),
        sa.Column('apify_run_id', sa.String(100), nullable=True),
        sa.Column('apify_dataset_id', sa.String(100), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )
    
    # Create indexes
    op.create_index('idx_youtube_video_video_id', 'dataset_youtube_video', ['video_id'])
    op.create_index('idx_youtube_video_channel_id', 'dataset_youtube_video', ['channel_id'])
    op.create_index('idx_youtube_video_source_list_id', 'dataset_youtube_video', ['source_list_id'])
    op.create_index('idx_youtube_video_ingested_at', 'dataset_youtube_video', ['ingested_at'])
    op.create_index('idx_youtube_channel_channel_id', 'dataset_youtube_channel', ['channel_id'])
    op.create_index('idx_youtube_lists_source_url', 'ctrl_youtube_lists', ['source_url'])
    op.create_index('idx_youtube_lists_is_active', 'ctrl_youtube_lists', ['is_active'])
    op.create_index('idx_ingestion_log_stage_status', 'ctrl_ingestion_log', ['stage_name', 'status'])
    op.create_index('idx_ingestion_log_started_at', 'ctrl_ingestion_log', ['started_at'])
    
    # Create GIN index for full-text search on transcript_text
    op.execute("CREATE INDEX IF NOT EXISTS idx_youtube_video_transcript_text ON dataset_youtube_video USING gin(to_tsvector('english', transcript_text))")


def downgrade() -> None:
    """Drop all tables and indexes."""
    # Drop GIN index first
    op.execute("DROP INDEX IF EXISTS idx_youtube_video_transcript_text")
    
    # Drop regular indexes
    op.drop_index('idx_ingestion_log_started_at', 'ctrl_ingestion_log')
    op.drop_index('idx_ingestion_log_stage_status', 'ctrl_ingestion_log')
    op.drop_index('idx_youtube_lists_is_active', 'ctrl_youtube_lists')
    op.drop_index('idx_youtube_lists_source_url', 'ctrl_youtube_lists')
    op.drop_index('idx_youtube_channel_channel_id', 'dataset_youtube_channel')
    op.drop_index('idx_youtube_video_ingested_at', 'dataset_youtube_video')
    op.drop_index('idx_youtube_video_source_list_id', 'dataset_youtube_video')
    op.drop_index('idx_youtube_video_channel_id', 'dataset_youtube_video')
    op.drop_index('idx_youtube_video_video_id', 'dataset_youtube_video')
    
    # Drop tables in reverse dependency order
    op.drop_table('ctrl_ingestion_log')
    op.drop_table('dataset_youtube_channel')
    op.drop_table('dataset_youtube_video')
    op.drop_table('ctrl_youtube_lists') 