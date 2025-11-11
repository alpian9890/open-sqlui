"""Database service for managing multiple SQLite databases."""

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Any
import asyncio
from datetime import datetime

from ..core.db_client import DatabaseClient, AsyncDatabaseClient, DatabaseError
from ..core.repository import Repository, AsyncRepository, TableSchema
from ..core.config import get_config


@dataclass
class DatabaseInfo:
    """Information about a database."""
    path: Path
    name: str
    size: int
    modified: datetime
    table_count: int = 0
    is_open: bool = False
    is_readonly: bool = True
    
    @property
    def size_str(self) -> str:
        """Get human-readable size string."""
        size = self.size
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size < 1024:
                return f"{size:.1f} {unit}"
            size /= 1024
        return f"{size:.1f} TB"
    
    @property
    def modified_str(self) -> str:
        """Get formatted modification date."""
        return self.modified.strftime("%Y-%m-%d %H:%M:%S")


class DatabaseService:
    """Service for managing multiple databases."""
    
    def __init__(self, readonly: bool = None):
        """Initialize database service.
        
        Args:
            readonly: Override readonly mode from config
        """
        config = get_config()
        self.readonly = readonly if readonly is not None else config.database.readonly_mode
        self.databases: Dict[str, DatabaseClient] = {}
        self.repositories: Dict[str, Repository] = {}
        self.active_db: Optional[str] = None
        self._metadata_cache: Dict[str, Dict[str, Any]] = {}
    
    def open_database(self, db_path: Path, set_active: bool = True) -> DatabaseInfo:
        """Open a database connection.
        
        Args:
            db_path: Path to the database file
            set_active: Whether to set as active database
            
        Returns:
            DatabaseInfo object
        """
        db_path = db_path.resolve()
        db_key = str(db_path)
        
        # Check if already open
        if db_key not in self.databases:
            # Create client and repository
            client = DatabaseClient(db_path, readonly=self.readonly)
            client.connect()
            
            self.databases[db_key] = client
            self.repositories[db_key] = Repository(client)
            
            # Cache metadata
            self._cache_metadata(db_key)
        
        if set_active:
            self.active_db = db_key
        
        # Get database info
        stat = db_path.stat()
        info = DatabaseInfo(
            path=db_path,
            name=db_path.name,
            size=stat.st_size,
            modified=datetime.fromtimestamp(stat.st_mtime),
            table_count=len(self.get_tables(db_key)),
            is_open=True,
            is_readonly=self.readonly
        )
        
        # Add to recent databases in config
        from ..core.config import get_config_manager
        get_config_manager().add_recent_database(str(db_path))
        
        return info
    
    def close_database(self, db_path: Path = None) -> None:
        """Close a database connection.
        
        Args:
            db_path: Path to the database (None for active)
        """
        if db_path is None and self.active_db:
            db_key = self.active_db
        else:
            db_key = str(db_path.resolve()) if db_path else None
        
        if db_key and db_key in self.databases:
            # Disconnect client
            self.databases[db_key].disconnect()
            
            # Remove from collections
            del self.databases[db_key]
            del self.repositories[db_key]
            
            # Clear metadata cache
            if db_key in self._metadata_cache:
                del self._metadata_cache[db_key]
            
            # Update active database
            if db_key == self.active_db:
                # Set to another open database or None
                self.active_db = next(iter(self.databases.keys()), None)
    
    def close_all_databases(self) -> None:
        """Close all open database connections."""
        db_keys = list(self.databases.keys())
        for db_key in db_keys:
            self.databases[db_key].disconnect()
        
        self.databases.clear()
        self.repositories.clear()
        self._metadata_cache.clear()
        self.active_db = None
    
    def get_active_database(self) -> Optional[DatabaseClient]:
        """Get the active database client."""
        if self.active_db:
            return self.databases.get(self.active_db)
        return None
    
    def get_active_repository(self) -> Optional[Repository]:
        """Get the active repository."""
        if self.active_db:
            return self.repositories.get(self.active_db)
        return None
    
    def set_active_database(self, db_path: Path) -> bool:
        """Set the active database.
        
        Args:
            db_path: Path to the database
            
        Returns:
            True if successful
        """
        db_key = str(db_path.resolve())
        if db_key in self.databases:
            self.active_db = db_key
            return True
        return False
    
    def get_open_databases(self) -> List[DatabaseInfo]:
        """Get list of open databases."""
        databases = []
        for db_key, client in self.databases.items():
            path = Path(db_key)
            if path.exists():
                stat = path.stat()
                info = DatabaseInfo(
                    path=path,
                    name=path.name,
                    size=stat.st_size,
                    modified=datetime.fromtimestamp(stat.st_mtime),
                    table_count=len(self.get_tables(db_key)),
                    is_open=True,
                    is_readonly=self.readonly
                )
                databases.append(info)
        return databases
    
    def get_tables(self, db_path: str = None) -> List[str]:
        """Get list of tables from a database.
        
        Args:
            db_path: Database path (None for active)
            
        Returns:
            List of table names
        """
        db_key = db_path or self.active_db
        if db_key and db_key in self.repositories:
            return self.repositories[db_key].get_all_tables()
        return []
    
    def get_table_schema(self, table_name: str, db_path: str = None) -> Optional[TableSchema]:
        """Get schema for a table.
        
        Args:
            table_name: Name of the table
            db_path: Database path (None for active)
            
        Returns:
            TableSchema object or None
        """
        db_key = db_path or self.active_db
        if db_key and db_key in self.repositories:
            try:
                return self.repositories[db_key].get_table_schema(table_name)
            except DatabaseError:
                return None
        return None
    
    def _cache_metadata(self, db_key: str) -> None:
        """Cache database metadata for performance.
        
        Args:
            db_key: Database key
        """
        if db_key not in self._metadata_cache:
            self._metadata_cache[db_key] = {}
        
        # Cache table list
        if db_key in self.repositories:
            tables = self.repositories[db_key].get_all_tables()
            self._metadata_cache[db_key]['tables'] = tables
            
            # Cache basic table info
            for table in tables:
                try:
                    schema = self.repositories[db_key].get_table_schema(table)
                    self._metadata_cache[db_key][f"schema_{table}"] = schema
                except DatabaseError:
                    pass
    
    def get_cached_metadata(self, db_key: str, key: str) -> Any:
        """Get cached metadata.
        
        Args:
            db_key: Database key
            key: Metadata key
            
        Returns:
            Cached value or None
        """
        if db_key in self._metadata_cache:
            return self._metadata_cache[db_key].get(key)
        return None
    
    def refresh_metadata(self, db_path: str = None) -> None:
        """Refresh metadata cache for a database.
        
        Args:
            db_path: Database path (None for active)
        """
        db_key = db_path or self.active_db
        if db_key:
            self._cache_metadata(db_key)
    
    def toggle_readonly_mode(self) -> bool:
        """Toggle readonly mode for all databases.
        
        Returns:
            New readonly state
        """
        self.readonly = not self.readonly
        
        # Reconnect all databases with new mode
        databases_to_reopen = list(self.databases.keys())
        active = self.active_db
        
        # Close all
        self.close_all_databases()
        
        # Reopen all
        for db_key in databases_to_reopen:
            path = Path(db_key)
            if path.exists():
                self.open_database(path, set_active=(db_key == active))
        
        return self.readonly


class AsyncDatabaseService:
    """Async service for managing multiple databases."""
    
    def __init__(self, readonly: bool = None):
        """Initialize async database service.
        
        Args:
            readonly: Override readonly mode from config
        """
        config = get_config()
        self.readonly = readonly if readonly is not None else config.database.readonly_mode
        self.databases: Dict[str, AsyncDatabaseClient] = {}
        self.repositories: Dict[str, AsyncRepository] = {}
        self.active_db: Optional[str] = None
        self._metadata_cache: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()
    
    async def open_database(self, db_path: Path, set_active: bool = True) -> DatabaseInfo:
        """Open a database connection."""
        async with self._lock:
            db_path = db_path.resolve()
            db_key = str(db_path)
            
            # Check if already open
            if db_key not in self.databases:
                # Create client and repository
                client = AsyncDatabaseClient(db_path, readonly=self.readonly)
                await client.connect()
                
                self.databases[db_key] = client
                self.repositories[db_key] = AsyncRepository(client)
                
                # Cache metadata
                await self._cache_metadata(db_key)
            
            if set_active:
                self.active_db = db_key
            
            # Get database info
            stat = db_path.stat()
            tables = await self.get_tables(db_key)
            info = DatabaseInfo(
                path=db_path,
                name=db_path.name,
                size=stat.st_size,
                modified=datetime.fromtimestamp(stat.st_mtime),
                table_count=len(tables),
                is_open=True,
                is_readonly=self.readonly
            )
            
            # Add to recent databases in config
            from ..core.config import get_config_manager
            get_config_manager().add_recent_database(str(db_path))
            
            return info
    
    async def close_database(self, db_path: Path = None) -> None:
        """Close a database connection."""
        async with self._lock:
            if db_path is None and self.active_db:
                db_key = self.active_db
            else:
                db_key = str(db_path.resolve()) if db_path else None
            
            if db_key and db_key in self.databases:
                # Disconnect client
                await self.databases[db_key].disconnect()
                
                # Remove from collections
                del self.databases[db_key]
                del self.repositories[db_key]
                
                # Clear metadata cache
                if db_key in self._metadata_cache:
                    del self._metadata_cache[db_key]
                
                # Update active database
                if db_key == self.active_db:
                    self.active_db = next(iter(self.databases.keys()), None)
    
    async def close_all_databases(self) -> None:
        """Close all open database connections."""
        async with self._lock:
            for client in self.databases.values():
                await client.disconnect()
            
            self.databases.clear()
            self.repositories.clear()
            self._metadata_cache.clear()
            self.active_db = None
    
    async def get_tables(self, db_path: str = None) -> List[str]:
        """Get list of tables from a database."""
        db_key = db_path or self.active_db
        if db_key and db_key in self.repositories:
            return await self.repositories[db_key].get_all_tables()
        return []
    
    async def get_table_schema(
        self,
        table_name: str,
        db_path: str = None
    ) -> Optional[TableSchema]:
        """Get schema for a table."""
        db_key = db_path or self.active_db
        if db_key and db_key in self.repositories:
            try:
                return await self.repositories[db_key].get_table_schema(table_name)
            except DatabaseError:
                return None
        return None
    
    async def _cache_metadata(self, db_key: str) -> None:
        """Cache database metadata for performance."""
        if db_key not in self._metadata_cache:
            self._metadata_cache[db_key] = {}
        
        # Cache table list
        if db_key in self.repositories:
            tables = await self.repositories[db_key].get_all_tables()
            self._metadata_cache[db_key]['tables'] = tables
            
            # Cache basic table info
            for table in tables:
                try:
                    schema = await self.repositories[db_key].get_table_schema(table)
                    self._metadata_cache[db_key][f"schema_{table}"] = schema
                except DatabaseError:
                    pass
