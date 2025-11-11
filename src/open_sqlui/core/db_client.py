"""Safe SQLite database client with async support."""

import aiosqlite
import sqlite3
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, AsyncIterator, Iterator
import asyncio
from threading import Lock


@dataclass
class QueryResult:
    """Result from a database query."""
    columns: List[str]
    rows: List[Tuple[Any, ...]]
    rowcount: int
    lastrowid: Optional[int] = None
    
    @property
    def is_empty(self) -> bool:
        """Check if result has no rows."""
        return len(self.rows) == 0
    
    def to_dicts(self) -> List[Dict[str, Any]]:
        """Convert rows to list of dictionaries."""
        return [dict(zip(self.columns, row)) for row in self.rows]


class DatabaseError(Exception):
    """Base exception for database operations."""
    pass


class ReadOnlyError(DatabaseError):
    """Raised when trying to modify database in read-only mode."""
    pass


class DatabaseClient:
    """Synchronous SQLite database client with safety features."""
    
    def __init__(self, db_path: Path, readonly: bool = True, timeout: float = 5.0):
        """Initialize database client.
        
        Args:
            db_path: Path to SQLite database file
            readonly: Whether to open in read-only mode
            timeout: Connection timeout in seconds
        """
        self.db_path = db_path
        self.readonly = readonly
        self.timeout = timeout
        self._connection: Optional[sqlite3.Connection] = None
        self._lock = Lock()
    
    def connect(self) -> None:
        """Establish database connection."""
        with self._lock:
            if self._connection is None:
                uri = f"file:{self.db_path}"
                if self.readonly:
                    uri += "?mode=ro"
                
                try:
                    self._connection = sqlite3.connect(
                        uri,
                        uri=True,
                        timeout=self.timeout,
                        check_same_thread=False
                    )
                    self._connection.row_factory = sqlite3.Row
                    self._connection.execute("PRAGMA query_only = {}".format(
                        1 if self.readonly else 0
                    ))
                except sqlite3.Error as e:
                    raise DatabaseError(f"Failed to connect to database: {e}")
    
    def disconnect(self) -> None:
        """Close database connection."""
        with self._lock:
            if self._connection:
                self._connection.close()
                self._connection = None
    
    @contextmanager
    def transaction(self):
        """Context manager for transactions."""
        if self.readonly:
            raise ReadOnlyError("Cannot start transaction in read-only mode")
        
        self.connect()
        try:
            self._connection.execute("BEGIN")
            yield self._connection
            self._connection.commit()
        except Exception as e:
            self._connection.rollback()
            raise DatabaseError(f"Transaction failed: {e}")
    
    def execute(self, query: str, params: Optional[Tuple] = None) -> QueryResult:
        """Execute a query with parameters.
        
        Args:
            query: SQL query string
            params: Query parameters
            
        Returns:
            QueryResult object
        """
        if self.readonly and self._is_write_query(query):
            raise ReadOnlyError("Write operations not allowed in read-only mode")
        
        self.connect()
        
        try:
            cursor = self._connection.execute(query, params or ())
            
            # Get column names
            columns = [desc[0] for desc in cursor.description or []]
            
            # Fetch all rows
            rows = cursor.fetchall()
            rows = [tuple(row) for row in rows]  # Convert Row objects to tuples
            
            result = QueryResult(
                columns=columns,
                rows=rows,
                rowcount=cursor.rowcount,
                lastrowid=cursor.lastrowid
            )
            
            # Commit if it's a write operation and not in readonly mode
            if not self.readonly and self._is_write_query(query):
                self._connection.commit()
            
            return result
            
        except sqlite3.Error as e:
            raise DatabaseError(f"Query execution failed: {e}")
    
    def execute_many(self, query: str, params_list: List[Tuple]) -> int:
        """Execute a query multiple times with different parameters.
        
        Args:
            query: SQL query string
            params_list: List of parameter tuples
            
        Returns:
            Total number of affected rows
        """
        if self.readonly:
            raise ReadOnlyError("Write operations not allowed in read-only mode")
        
        self.connect()
        
        try:
            cursor = self._connection.executemany(query, params_list)
            self._connection.commit()
            return cursor.rowcount
        except sqlite3.Error as e:
            self._connection.rollback()
            raise DatabaseError(f"Batch execution failed: {e}")
    
    def get_tables(self) -> List[str]:
        """Get list of all tables in the database."""
        query = """
            SELECT name FROM sqlite_master 
            WHERE type='table' 
            AND name NOT LIKE 'sqlite_%'
            ORDER BY name
        """
        result = self.execute(query)
        return [row[0] for row in result.rows]
    
    def get_table_info(self, table_name: str) -> List[Dict[str, Any]]:
        """Get column information for a table."""
        query = f"PRAGMA table_info({table_name})"
        result = self.execute(query)
        return result.to_dicts()
    
    def get_table_count(self, table_name: str) -> int:
        """Get row count for a table."""
        query = f"SELECT COUNT(*) FROM {table_name}"
        result = self.execute(query)
        return result.rows[0][0] if result.rows else 0
    
    @staticmethod
    def _is_write_query(query: str) -> bool:
        """Check if query is a write operation."""
        query_upper = query.strip().upper()
        write_keywords = ('INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER')
        return any(query_upper.startswith(kw) for kw in write_keywords)


class AsyncDatabaseClient:
    """Asynchronous SQLite database client."""
    
    def __init__(self, db_path: Path, readonly: bool = True, timeout: float = 5.0):
        """Initialize async database client.
        
        Args:
            db_path: Path to SQLite database file
            readonly: Whether to open in read-only mode
            timeout: Connection timeout in seconds
        """
        self.db_path = db_path
        self.readonly = readonly
        self.timeout = timeout
        self._connection: Optional[aiosqlite.Connection] = None
        self._lock = asyncio.Lock()
    
    async def connect(self) -> None:
        """Establish database connection."""
        async with self._lock:
            if self._connection is None:
                uri = f"file:{self.db_path}"
                if self.readonly:
                    uri += "?mode=ro"
                
                try:
                    self._connection = await aiosqlite.connect(
                        uri,
                        uri=True,
                        timeout=self.timeout
                    )
                    self._connection.row_factory = aiosqlite.Row
                    await self._connection.execute("PRAGMA query_only = {}".format(
                        1 if self.readonly else 0
                    ))
                except Exception as e:
                    raise DatabaseError(f"Failed to connect to database: {e}")
    
    async def disconnect(self) -> None:
        """Close database connection."""
        async with self._lock:
            if self._connection:
                await self._connection.close()
                self._connection = None
    
    @asynccontextmanager
    async def transaction(self):
        """Async context manager for transactions."""
        if self.readonly:
            raise ReadOnlyError("Cannot start transaction in read-only mode")
        
        await self.connect()
        try:
            await self._connection.execute("BEGIN")
            yield self._connection
            await self._connection.commit()
        except Exception as e:
            await self._connection.rollback()
            raise DatabaseError(f"Transaction failed: {e}")
    
    async def execute(self, query: str, params: Optional[Tuple] = None) -> QueryResult:
        """Execute a query with parameters.
        
        Args:
            query: SQL query string
            params: Query parameters
            
        Returns:
            QueryResult object
        """
        if self.readonly and self._is_write_query(query):
            raise ReadOnlyError("Write operations not allowed in read-only mode")
        
        await self.connect()
        
        try:
            cursor = await self._connection.execute(query, params or ())
            
            # Get column names
            columns = [desc[0] for desc in cursor.description or []]
            
            # Fetch all rows
            rows = await cursor.fetchall()
            rows = [tuple(row) for row in rows]
            
            result = QueryResult(
                columns=columns,
                rows=rows,
                rowcount=cursor.rowcount,
                lastrowid=cursor.lastrowid
            )
            
            # Commit if it's a write operation
            if not self.readonly and self._is_write_query(query):
                await self._connection.commit()
            
            return result
            
        except Exception as e:
            raise DatabaseError(f"Query execution failed: {e}")
    
    async def execute_many(self, query: str, params_list: List[Tuple]) -> int:
        """Execute a query multiple times with different parameters."""
        if self.readonly:
            raise ReadOnlyError("Write operations not allowed in read-only mode")
        
        await self.connect()
        
        try:
            cursor = await self._connection.executemany(query, params_list)
            await self._connection.commit()
            return cursor.rowcount
        except Exception as e:
            await self._connection.rollback()
            raise DatabaseError(f"Batch execution failed: {e}")
    
    async def get_tables(self) -> List[str]:
        """Get list of all tables in the database."""
        query = """
            SELECT name FROM sqlite_master 
            WHERE type='table' 
            AND name NOT LIKE 'sqlite_%'
            ORDER BY name
        """
        result = await self.execute(query)
        return [row[0] for row in result.rows]
    
    async def get_table_info(self, table_name: str) -> List[Dict[str, Any]]:
        """Get column information for a table."""
        query = f"PRAGMA table_info({table_name})"
        result = await self.execute(query)
        return result.to_dicts()
    
    async def get_table_count(self, table_name: str) -> int:
        """Get row count for a table."""
        query = f"SELECT COUNT(*) FROM {table_name}"
        result = await self.execute(query)
        return result.rows[0][0] if result.rows else 0
    
    @staticmethod
    def _is_write_query(query: str) -> bool:
        """Check if query is a write operation."""
        query_upper = query.strip().upper()
        write_keywords = ('INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER')
        return any(query_upper.startswith(kw) for kw in write_keywords)
