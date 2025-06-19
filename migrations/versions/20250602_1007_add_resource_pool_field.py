"""Add resource_pool field to all tables

Revision ID: 002_add_resource_pool
Revises: 001_initial_schema
Create Date: 2025-06-02 10:07:37.956446

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '002_add_resource_pool'
down_revision = '001_initial_schema'
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add resource_pool field to all tables."""
    
    # Add resource_pool column to ctrl_youtube_lists (nullable for backward compatibility)
    op.add_column('ctrl_youtube_lists', sa.Column('resource_pool', sa.String(length=100), nullable=True))
    op.create_index('idx_youtube_lists_resource_pool', 'ctrl_youtube_lists', ['resource_pool'])
    
    # Add resource_pool column to dataset_youtube_video (nullable initially, will be made required in application logic)
    op.add_column('dataset_youtube_video', sa.Column('resource_pool', sa.String(length=100), nullable=True))
    op.create_index('idx_youtube_video_resource_pool', 'dataset_youtube_video', ['resource_pool'])
    
    # Add resource_pool column to dataset_youtube_channel (nullable initially, will be made required in application logic)
    op.add_column('dataset_youtube_channel', sa.Column('resource_pool', sa.String(length=100), nullable=True))
    op.create_index('idx_youtube_channel_resource_pool', 'dataset_youtube_channel', ['resource_pool'])
    
    # Add resource_pool column to ctrl_ingestion_log (nullable for backward compatibility)
    op.add_column('ctrl_ingestion_log', sa.Column('resource_pool', sa.String(length=100), nullable=True))
    op.create_index('idx_ingestion_log_resource_pool', 'ctrl_ingestion_log', ['resource_pool'])


def downgrade() -> None:
    """Remove resource_pool field from all tables."""
    
    # Drop indexes and columns in reverse order
    op.drop_index('idx_ingestion_log_resource_pool', table_name='ctrl_ingestion_log')
    op.drop_column('ctrl_ingestion_log', 'resource_pool')
    
    op.drop_index('idx_youtube_channel_resource_pool', table_name='dataset_youtube_channel')
    op.drop_column('dataset_youtube_channel', 'resource_pool')
    
    op.drop_index('idx_youtube_video_resource_pool', table_name='dataset_youtube_video')
    op.drop_column('dataset_youtube_video', 'resource_pool')
    
    op.drop_index('idx_youtube_lists_resource_pool', table_name='ctrl_youtube_lists')
    op.drop_column('ctrl_youtube_lists', 'resource_pool') 