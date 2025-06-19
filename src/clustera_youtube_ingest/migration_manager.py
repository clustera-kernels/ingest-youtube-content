"""
Migration management for Clustera YouTube Ingest.

Provides integration between Alembic migrations and the existing DatabaseManager.
"""

import logging
import os
from pathlib import Path
from typing import Dict, Any, List, Optional

from alembic import command
from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import inspect

from .database import DatabaseManager

logger = logging.getLogger(__name__)


class MigrationManager:
    """
    Manages database migrations using Alembic.
    
    Provides a bridge between the existing DatabaseManager and Alembic
    for version-controlled schema management.
    """
    
    def __init__(self, database_url: Optional[str] = None):
        """
        Initialize MigrationManager.
        
        Args:
            database_url: PostgreSQL connection string. If None, reads from environment.
        """
        self.database_url = database_url or os.getenv('CLUSTERA_DATABASE_URL')
        if not self.database_url:
            raise ValueError(
                "Database URL not provided. Set CLUSTERA_DATABASE_URL environment variable "
                "or pass database_url parameter."
            )
        
        # Get project root and alembic.ini path
        self.project_root = Path(__file__).parent.parent.parent
        self.alembic_ini_path = self.project_root / "alembic.ini"
        
        if not self.alembic_ini_path.exists():
            raise FileNotFoundError(f"Alembic configuration not found: {self.alembic_ini_path}")
        
        # Initialize Alembic configuration
        self.alembic_cfg = Config(str(self.alembic_ini_path))
        self.alembic_cfg.set_main_option("sqlalchemy.url", self.database_url)
        
        # Initialize database manager for utility functions
        self.db_manager = DatabaseManager(self.database_url)
    
    def get_migration_status(self) -> Dict[str, Any]:
        """
        Get current migration status.
        
        Returns:
            Dict with migration status information
        """
        status = {
            "current_revision": None,
            "head_revision": None,
            "pending_upgrades": [],
            "is_up_to_date": False,
            "schema_exists": False,
            "alembic_version_table_exists": False,
            "error": None
        }
        
        try:
            self.db_manager.connect()
            
            # Check if any tables exist
            inspector = inspect(self.db_manager.engine)
            existing_tables = inspector.get_table_names()
            status["schema_exists"] = len(existing_tables) > 0
            status["alembic_version_table_exists"] = "alembic_version" in existing_tables
            
            # Get script directory
            script_dir = ScriptDirectory.from_config(self.alembic_cfg)
            status["head_revision"] = script_dir.get_current_head()
            
            # Get current revision from database
            if status["alembic_version_table_exists"]:
                with self.db_manager.engine.connect() as conn:
                    migration_ctx = MigrationContext.configure(conn)
                    status["current_revision"] = migration_ctx.get_current_revision()
            
            # Determine if migrations are needed
            if status["current_revision"] != status["head_revision"]:
                # Get pending migrations
                if status["current_revision"]:
                    status["pending_upgrades"] = self._get_pending_migrations(
                        status["current_revision"], 
                        status["head_revision"]
                    )
                else:
                    # No current revision means we need to run all migrations
                    status["pending_upgrades"] = self._get_all_migrations()
            else:
                status["is_up_to_date"] = True
            
        except Exception as e:
            status["error"] = str(e)
            logger.error(f"Error getting migration status: {e}")
        finally:
            self.db_manager.close()
        
        return status
    
    def upgrade_to_head(self) -> Dict[str, Any]:
        """
        Upgrade database to the latest migration.
        
        Returns:
            Dict with upgrade results
        """
        logger.info("Starting migration upgrade to head")
        
        result = {
            "success": False,
            "migrations_applied": [],
            "error": None,
            "previous_revision": None,
            "new_revision": None
        }
        
        try:
            # Get current status
            status = self.get_migration_status()
            result["previous_revision"] = status.get("current_revision")
            
            if status.get("error"):
                result["error"] = f"Status check failed: {status['error']}"
                return result
            
            if status.get("is_up_to_date"):
                result["success"] = True
                result["new_revision"] = status.get("current_revision")
                logger.info("Database is already up to date")
                return result
            
            # Apply migrations
            logger.info(f"Applying {len(status.get('pending_upgrades', []))} pending migrations")
            
            # Use Alembic command to upgrade
            command.upgrade(self.alembic_cfg, "head")
            
            # Get new status
            new_status = self.get_migration_status()
            result["new_revision"] = new_status.get("current_revision")
            result["migrations_applied"] = status.get("pending_upgrades", [])
            result["success"] = True
            
            logger.info(f"Successfully upgraded to revision: {result['new_revision']}")
            
        except Exception as e:
            result["error"] = str(e)
            logger.error(f"Migration upgrade failed: {e}")
        
        return result
    
    def downgrade_to_revision(self, revision: str) -> Dict[str, Any]:
        """
        Downgrade database to a specific revision.
        
        Args:
            revision: Target revision to downgrade to
            
        Returns:
            Dict with downgrade results
        """
        logger.info(f"Starting migration downgrade to revision: {revision}")
        
        result = {
            "success": False,
            "error": None,
            "previous_revision": None,
            "new_revision": None
        }
        
        try:
            # Get current status
            status = self.get_migration_status()
            result["previous_revision"] = status.get("current_revision")
            
            if status.get("error"):
                result["error"] = f"Status check failed: {status['error']}"
                return result
            
            # Apply downgrade
            command.downgrade(self.alembic_cfg, revision)
            
            # Get new status
            new_status = self.get_migration_status()
            result["new_revision"] = new_status.get("current_revision")
            result["success"] = True
            
            logger.info(f"Successfully downgraded to revision: {result['new_revision']}")
            
        except Exception as e:
            result["error"] = str(e)
            logger.error(f"Migration downgrade failed: {e}")
        
        return result
    
    def create_migration(self, message: str, autogenerate: bool = True) -> Dict[str, Any]:
        """
        Create a new migration.
        
        Args:
            message: Migration description
            autogenerate: Whether to auto-generate migration from model changes
            
        Returns:
            Dict with creation results
        """
        logger.info(f"Creating new migration: {message}")
        
        result = {
            "success": False,
            "revision_id": None,
            "migration_file": None,
            "error": None
        }
        
        try:
            # Create the migration
            if autogenerate:
                command.revision(self.alembic_cfg, message=message, autogenerate=True)
            else:
                command.revision(self.alembic_cfg, message=message)
            
            # Get the latest revision info
            script_dir = ScriptDirectory.from_config(self.alembic_cfg)
            head_revision = script_dir.get_current_head()
            
            if head_revision:
                result["revision_id"] = head_revision
                # Find the migration file
                revision_map = script_dir.get_revision_map()
                if head_revision in revision_map:
                    revision_obj = revision_map[head_revision]
                    result["migration_file"] = revision_obj.path
            
            result["success"] = True
            logger.info(f"Successfully created migration: {result['revision_id']}")
            
        except Exception as e:
            result["error"] = str(e)
            logger.error(f"Migration creation failed: {e}")
        
        return result
    
    def stamp_database(self, revision: str = "head") -> Dict[str, Any]:
        """
        Stamp database with a specific revision without running migrations.
        
        Useful for marking an existing database as being at a specific revision.
        
        Args:
            revision: Revision to stamp (default: "head")
            
        Returns:
            Dict with stamp results
        """
        logger.info(f"Stamping database with revision: {revision}")
        
        result = {
            "success": False,
            "stamped_revision": None,
            "error": None
        }
        
        try:
            command.stamp(self.alembic_cfg, revision)
            
            # Get the actual revision that was stamped
            if revision == "head":
                script_dir = ScriptDirectory.from_config(self.alembic_cfg)
                result["stamped_revision"] = script_dir.get_current_head()
            else:
                result["stamped_revision"] = revision
            
            result["success"] = True
            logger.info(f"Successfully stamped database with revision: {result['stamped_revision']}")
            
        except Exception as e:
            result["error"] = str(e)
            logger.error(f"Database stamp failed: {e}")
        
        return result
    
    def get_migration_history(self) -> List[Dict[str, Any]]:
        """
        Get migration history.
        
        Returns:
            List of migration information
        """
        try:
            script_dir = ScriptDirectory.from_config(self.alembic_cfg)
            revisions = []
            
            for revision in script_dir.walk_revisions():
                revisions.append({
                    "revision": revision.revision,
                    "down_revision": revision.down_revision,
                    "branch_labels": revision.branch_labels,
                    "message": revision.doc,
                    "path": revision.path
                })
            
            return revisions
            
        except Exception as e:
            logger.error(f"Error getting migration history: {e}")
            return []
    
    def _get_pending_migrations(self, current: str, head: str) -> List[str]:
        """Get list of pending migration revisions."""
        try:
            script_dir = ScriptDirectory.from_config(self.alembic_cfg)
            pending = []
            
            for revision in script_dir.iterate_revisions(head, current):
                if revision.revision != current:
                    pending.append(revision.revision)
            
            return list(reversed(pending))  # Return in application order
            
        except Exception as e:
            logger.error(f"Error getting pending migrations: {e}")
            return []
    
    def _get_all_migrations(self) -> List[str]:
        """Get list of all migration revisions."""
        try:
            script_dir = ScriptDirectory.from_config(self.alembic_cfg)
            all_revisions = []
            
            for revision in script_dir.walk_revisions():
                all_revisions.append(revision.revision)
            
            return list(reversed(all_revisions))  # Return in application order
            
        except Exception as e:
            logger.error(f"Error getting all migrations: {e}")
            return [] 