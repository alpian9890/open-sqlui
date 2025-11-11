"""File browser service for finding SQLite databases."""

import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Set
import sqlite3
import asyncio
from concurrent.futures import ThreadPoolExecutor


@dataclass
class FileInfo:
    """Information about a file."""
    path: Path
    name: str
    size: int
    modified: datetime
    is_directory: bool
    is_database: bool = False
    is_favorite: bool = False
    table_count: Optional[int] = None
    
    @property
    def size_str(self) -> str:
        """Get human-readable size string."""
        if self.is_directory:
            return ""
        
        size = self.size
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size < 1024:
                return f"{size:.1f} {unit}"
            size /= 1024
        return f"{size:.1f} TB"
    
    @property
    def modified_str(self) -> str:
        """Get formatted modification date."""
        return self.modified.strftime("%Y-%m-%d %H:%M")
    
    @property
    def type_str(self) -> str:
        """Get file type string."""
        if self.is_directory:
            return "Directory"
        elif self.is_database:
            return "SQLite Database"
        else:
            return self.path.suffix[1:].upper() if self.path.suffix else "File"


class FileBrowser:
    """Service for browsing and finding SQLite databases."""
    
    # SQLite file extensions
    DB_EXTENSIONS = {'.db', '.sqlite', '.sqlite3', '.db3', '.s3db', '.sl3'}
    
    # Directories to ignore
    IGNORE_DIRS = {
        '.git', '__pycache__', 'node_modules', '.venv', 'venv',
        'env', '.env', '.tox', '.pytest_cache', '.mypy_cache',
        'dist', 'build', '.idea', '.vscode'
    }
    
    def __init__(self, favorites: Optional[List[str]] = None):
        """Initialize file browser.
        
        Args:
            favorites: List of favorite database paths
        """
        self.favorites: Set[str] = set(favorites or [])
        self.current_path = Path.cwd()
        self._db_cache: Dict[str, int] = {}  # Cache of path -> table count
    
    def get_current_directory(self) -> Path:
        """Get current directory path."""
        return self.current_path
    
    def change_directory(self, path: Path) -> bool:
        """Change current directory.
        
        Args:
            path: New directory path
            
        Returns:
            True if successful
        """
        path = path.resolve()
        if path.is_dir() and path.exists():
            self.current_path = path
            return True
        return False
    
    def go_up(self) -> bool:
        """Go to parent directory.
        
        Returns:
            True if successful
        """
        parent = self.current_path.parent
        if parent != self.current_path:
            self.current_path = parent
            return True
        return False
    
    def go_home(self) -> None:
        """Go to home directory."""
        self.current_path = Path.home()
    
    def list_directory(
        self,
        path: Optional[Path] = None,
        show_hidden: bool = False,
        databases_only: bool = False
    ) -> List[FileInfo]:
        """List contents of a directory.
        
        Args:
            path: Directory path (None for current)
            show_hidden: Whether to show hidden files
            databases_only: Show only database files
            
        Returns:
            List of FileInfo objects
        """
        path = path or self.current_path
        files = []
        
        try:
            for item in path.iterdir():
                # Skip hidden files if requested
                if not show_hidden and item.name.startswith('.'):
                    continue
                
                # Skip ignored directories
                if item.is_dir() and item.name in self.IGNORE_DIRS:
                    continue
                
                # Check if it's a database
                is_db = self.is_sqlite_database(item)
                
                # Skip non-databases if requested
                if databases_only and not is_db and not item.is_dir():
                    continue
                
                try:
                    stat = item.stat()
                    info = FileInfo(
                        path=item,
                        name=item.name,
                        size=stat.st_size,
                        modified=datetime.fromtimestamp(stat.st_mtime),
                        is_directory=item.is_dir(),
                        is_database=is_db,
                        is_favorite=str(item) in self.favorites
                    )
                    
                    # Get table count for databases
                    if is_db and str(item) in self._db_cache:
                        info.table_count = self._db_cache[str(item)]
                    
                    files.append(info)
                except (PermissionError, OSError):
                    continue
        except (PermissionError, OSError):
            pass
        
        # Sort: directories first, then by name
        files.sort(key=lambda f: (not f.is_directory, f.name.lower()))
        
        return files
    
    def find_databases(
        self,
        root_path: Optional[Path] = None,
        recursive: bool = True,
        max_depth: int = 3
    ) -> List[FileInfo]:
        """Find all SQLite databases in a directory tree.
        
        Args:
            root_path: Root directory to search (None for current)
            recursive: Whether to search recursively
            max_depth: Maximum recursion depth
            
        Returns:
            List of FileInfo objects for databases
        """
        root_path = root_path or self.current_path
        databases = []
        
        def _search(path: Path, depth: int = 0):
            """Recursive search function."""
            if depth > max_depth:
                return
            
            try:
                for item in path.iterdir():
                    # Skip hidden and ignored directories
                    if item.name.startswith('.'):
                        continue
                    
                    if item.is_dir():
                        if item.name not in self.IGNORE_DIRS and recursive:
                            _search(item, depth + 1)
                    elif self.is_sqlite_database(item):
                        try:
                            stat = item.stat()
                            info = FileInfo(
                                path=item,
                                name=item.name,
                                size=stat.st_size,
                                modified=datetime.fromtimestamp(stat.st_mtime),
                                is_directory=False,
                                is_database=True,
                                is_favorite=str(item) in self.favorites
                            )
                            
                            # Try to get table count
                            table_count = self.get_table_count(item)
                            if table_count is not None:
                                info.table_count = table_count
                                self._db_cache[str(item)] = table_count
                            
                            databases.append(info)
                        except (PermissionError, OSError):
                            continue
            except (PermissionError, OSError):
                pass
        
        _search(root_path)
        
        # Sort by modification date (most recent first)
        databases.sort(key=lambda f: f.modified, reverse=True)
        
        return databases
    
    def is_sqlite_database(self, path: Path) -> bool:
        """Check if a file is a SQLite database.
        
        Args:
            path: File path
            
        Returns:
            True if it's a SQLite database
        """
        if not path.is_file():
            return False
        
        # Check extension
        if path.suffix.lower() not in self.DB_EXTENSIONS:
            # Could still be a SQLite database without standard extension
            # Check magic bytes
            try:
                with open(path, 'rb') as f:
                    header = f.read(16)
                    return header.startswith(b'SQLite format 3\x00')
            except (PermissionError, OSError, IOError):
                return False
        
        # Has SQLite extension, verify it's actually a database
        try:
            with open(path, 'rb') as f:
                header = f.read(16)
                return header.startswith(b'SQLite format 3\x00')
        except (PermissionError, OSError, IOError):
            return False
    
    def get_table_count(self, db_path: Path) -> Optional[int]:
        """Get number of tables in a database.
        
        Args:
            db_path: Path to database file
            
        Returns:
            Number of tables or None if error
        """
        try:
            conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=1.0)
            cursor = conn.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
            count = cursor.fetchone()[0]
            conn.close()
            return count
        except (sqlite3.Error, OSError):
            return None
    
    def add_favorite(self, path: Path) -> None:
        """Add a database to favorites.
        
        Args:
            path: Database path
        """
        self.favorites.add(str(path.resolve()))
    
    def remove_favorite(self, path: Path) -> None:
        """Remove a database from favorites.
        
        Args:
            path: Database path
        """
        self.favorites.discard(str(path.resolve()))
    
    def is_favorite(self, path: Path) -> bool:
        """Check if a database is in favorites.
        
        Args:
            path: Database path
            
        Returns:
            True if in favorites
        """
        return str(path.resolve()) in self.favorites
    
    def get_favorites(self) -> List[FileInfo]:
        """Get list of favorite databases.
        
        Returns:
            List of FileInfo objects for favorites
        """
        favorites = []
        
        for fav_path in self.favorites:
            path = Path(fav_path)
            if path.exists() and self.is_sqlite_database(path):
                try:
                    stat = path.stat()
                    info = FileInfo(
                        path=path,
                        name=path.name,
                        size=stat.st_size,
                        modified=datetime.fromtimestamp(stat.st_mtime),
                        is_directory=False,
                        is_database=True,
                        is_favorite=True
                    )
                    
                    # Get table count
                    if str(path) in self._db_cache:
                        info.table_count = self._db_cache[str(path)]
                    else:
                        table_count = self.get_table_count(path)
                        if table_count is not None:
                            info.table_count = table_count
                            self._db_cache[str(path)] = table_count
                    
                    favorites.append(info)
                except (PermissionError, OSError):
                    continue
        
        return favorites
    
    def get_recent_databases(self, limit: int = 10) -> List[FileInfo]:
        """Get recently modified databases in current directory tree.
        
        Args:
            limit: Maximum number of databases to return
            
        Returns:
            List of FileInfo objects
        """
        databases = self.find_databases(max_depth=2)
        return databases[:limit]


class AsyncFileBrowser:
    """Async file browser service."""
    
    def __init__(self, favorites: Optional[List[str]] = None):
        """Initialize async file browser."""
        self.sync_browser = FileBrowser(favorites)
        self.executor = ThreadPoolExecutor(max_workers=4)
    
    async def list_directory(
        self,
        path: Optional[Path] = None,
        show_hidden: bool = False,
        databases_only: bool = False
    ) -> List[FileInfo]:
        """List directory contents asynchronously."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self.executor,
            self.sync_browser.list_directory,
            path,
            show_hidden,
            databases_only
        )
    
    async def find_databases(
        self,
        root_path: Optional[Path] = None,
        recursive: bool = True,
        max_depth: int = 3
    ) -> List[FileInfo]:
        """Find databases asynchronously."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self.executor,
            self.sync_browser.find_databases,
            root_path,
            recursive,
            max_depth
        )
    
    async def get_table_count(self, db_path: Path) -> Optional[int]:
        """Get table count asynchronously."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self.executor,
            self.sync_browser.get_table_count,
            db_path
        )
