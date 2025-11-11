"""Table service for data operations with pagination and search."""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
import math

from ..core.db_client import QueryResult, DatabaseError, ReadOnlyError
from ..core.repository import Repository, AsyncRepository, TableSchema
from ..services.database_service import DatabaseService, AsyncDatabaseService
from ..core.config import get_config


@dataclass
class PagedResult:
    """Result with pagination information."""
    data: QueryResult
    page: int
    page_size: int
    total_rows: int
    total_pages: int
    has_next: bool
    has_previous: bool
    
    @property
    def start_row(self) -> int:
        """Get starting row number (1-based)."""
        return (self.page - 1) * self.page_size + 1
    
    @property
    def end_row(self) -> int:
        """Get ending row number (1-based)."""
        return min(self.page * self.page_size, self.total_rows)


@dataclass
class DataChange:
    """Represents a pending data change."""
    operation: str  # 'insert', 'update', 'delete'
    table_name: str
    old_data: Optional[Dict[str, Any]] = None
    new_data: Optional[Dict[str, Any]] = None
    where_clause: Optional[str] = None
    where_params: Optional[Tuple] = None


class TableService:
    """Service for table data operations."""
    
    def __init__(self, database_service: DatabaseService):
        """Initialize table service.
        
        Args:
            database_service: Database service instance
        """
        self.db_service = database_service
        self.pending_changes: List[DataChange] = []
        self._undo_stack: List[DataChange] = []
        config = get_config()
        self.default_page_size = config.ui.page_size
    
    def get_repository(self) -> Optional[Repository]:
        """Get active repository."""
        return self.db_service.get_active_repository()
    
    def get_page(
        self,
        table_name: str,
        page: int = 1,
        page_size: Optional[int] = None,
        order_by: Optional[str] = None,
        where: Optional[str] = None,
        params: Optional[Tuple] = None
    ) -> PagedResult:
        """Get paginated data from a table.
        
        Args:
            table_name: Name of the table
            page: Page number (1-based)
            page_size: Number of rows per page
            order_by: ORDER BY clause
            where: WHERE clause
            params: Parameters for WHERE clause
            
        Returns:
            PagedResult object
        """
        repo = self.get_repository()
        if not repo:
            raise DatabaseError("No active database")
        
        # Get total row count
        if where:
            count_query = f"SELECT COUNT(*) FROM {table_name} WHERE {where}"
            count_result = repo.client.execute(count_query, params)
        else:
            count_result = repo.client.execute(f"SELECT COUNT(*) FROM {table_name}")
        
        total_rows = count_result.rows[0][0] if count_result.rows else 0
        
        # Calculate pagination
        page_size = page_size or self.default_page_size
        total_pages = math.ceil(total_rows / page_size) if total_rows > 0 else 1
        page = max(1, min(page, total_pages))
        offset = (page - 1) * page_size
        
        # Get page data
        data = repo.select(
            table_name=table_name,
            where=where,
            params=params,
            order_by=order_by,
            limit=page_size,
            offset=offset
        )
        
        return PagedResult(
            data=data,
            page=page,
            page_size=page_size,
            total_rows=total_rows,
            total_pages=total_pages,
            has_next=page < total_pages,
            has_previous=page > 1
        )
    
    def search_table(
        self,
        table_name: str,
        search_term: str,
        columns: Optional[List[str]] = None,
        page: int = 1,
        page_size: Optional[int] = None
    ) -> PagedResult:
        """Search for data in a table.
        
        Args:
            table_name: Name of the table
            search_term: Term to search for
            columns: Columns to search in
            page: Page number
            page_size: Number of rows per page
            
        Returns:
            PagedResult object
        """
        repo = self.get_repository()
        if not repo:
            raise DatabaseError("No active database")
        
        # Get table schema to find searchable columns
        schema = repo.get_table_schema(table_name)
        
        if not columns:
            # Use all text-like columns
            columns = [
                col['name'] for col in schema.columns
                if col['type'].upper() in ('TEXT', 'VARCHAR', 'CHAR')
            ]
        
        if not columns:
            # No searchable columns
            return PagedResult(
                data=QueryResult(columns=[], rows=[], rowcount=0),
                page=1,
                page_size=page_size or self.default_page_size,
                total_rows=0,
                total_pages=1,
                has_next=False,
                has_previous=False
            )
        
        # Build WHERE clause
        conditions = [f"{col} LIKE ?" for col in columns]
        where_clause = " OR ".join(conditions)
        search_pattern = f"%{search_term}%"
        params = tuple([search_pattern] * len(columns))
        
        # Get paginated results
        return self.get_page(
            table_name=table_name,
            page=page,
            page_size=page_size,
            where=where_clause,
            params=params
        )
    
    def get_record(
        self,
        table_name: str,
        primary_key: str,
        key_value: Any
    ) -> Optional[Dict[str, Any]]:
        """Get a single record by primary key.
        
        Args:
            table_name: Name of the table
            primary_key: Primary key column name
            key_value: Primary key value
            
        Returns:
            Record as dictionary or None
        """
        repo = self.get_repository()
        if not repo:
            return None
        
        result = repo.select(
            table_name=table_name,
            where=f"{primary_key} = ?",
            params=(key_value,),
            limit=1
        )
        
        if result.rows:
            return dict(zip(result.columns, result.rows[0]))
        return None
    
    def insert_record(
        self,
        table_name: str,
        data: Dict[str, Any],
        auto_commit: bool = True
    ) -> int:
        """Insert a new record.
        
        Args:
            table_name: Name of the table
            data: Record data
            auto_commit: Whether to commit immediately
            
        Returns:
            ID of inserted row
        """
        repo = self.get_repository()
        if not repo:
            raise DatabaseError("No active database")
        
        if self.db_service.readonly:
            raise ReadOnlyError("Database is in read-only mode")
        
        # Add to pending changes if not auto-committing
        if not auto_commit:
            change = DataChange(
                operation='insert',
                table_name=table_name,
                new_data=data
            )
            self.pending_changes.append(change)
            return -1  # Temporary ID
        
        # Execute insert
        row_id = repo.insert(table_name, data)
        
        # Add to undo stack
        self._undo_stack.append(DataChange(
            operation='delete',
            table_name=table_name,
            where_clause=f"rowid = ?",
            where_params=(row_id,)
        ))
        
        return row_id
    
    def update_record(
        self,
        table_name: str,
        data: Dict[str, Any],
        where: str,
        params: Optional[Tuple] = None,
        auto_commit: bool = True
    ) -> int:
        """Update records.
        
        Args:
            table_name: Name of the table
            data: New data values
            where: WHERE clause
            params: Parameters for WHERE clause
            auto_commit: Whether to commit immediately
            
        Returns:
            Number of affected rows
        """
        repo = self.get_repository()
        if not repo:
            raise DatabaseError("No active database")
        
        if self.db_service.readonly:
            raise ReadOnlyError("Database is in read-only mode")
        
        # Get old data for undo
        old_data_result = repo.select(
            table_name=table_name,
            where=where,
            params=params
        )
        
        # Add to pending changes if not auto-committing
        if not auto_commit:
            change = DataChange(
                operation='update',
                table_name=table_name,
                old_data=old_data_result.to_dicts()[0] if old_data_result.rows else None,
                new_data=data,
                where_clause=where,
                where_params=params
            )
            self.pending_changes.append(change)
            return len(old_data_result.rows)
        
        # Execute update
        affected = repo.update(table_name, data, where, params)
        
        # Add to undo stack
        if old_data_result.rows:
            for old_row in old_data_result.to_dicts():
                self._undo_stack.append(DataChange(
                    operation='update',
                    table_name=table_name,
                    old_data=data,
                    new_data=old_row,
                    where_clause=where,
                    where_params=params
                ))
        
        return affected
    
    def delete_record(
        self,
        table_name: str,
        where: str,
        params: Optional[Tuple] = None,
        auto_commit: bool = True
    ) -> int:
        """Delete records.
        
        Args:
            table_name: Name of the table
            where: WHERE clause
            params: Parameters for WHERE clause
            auto_commit: Whether to commit immediately
            
        Returns:
            Number of deleted rows
        """
        repo = self.get_repository()
        if not repo:
            raise DatabaseError("No active database")
        
        if self.db_service.readonly:
            raise ReadOnlyError("Database is in read-only mode")
        
        # Get data for undo
        old_data_result = repo.select(
            table_name=table_name,
            where=where,
            params=params
        )
        
        # Add to pending changes if not auto-committing
        if not auto_commit:
            for row in old_data_result.to_dicts():
                change = DataChange(
                    operation='delete',
                    table_name=table_name,
                    old_data=row,
                    where_clause=where,
                    where_params=params
                )
                self.pending_changes.append(change)
            return len(old_data_result.rows)
        
        # Execute delete
        deleted = repo.delete(table_name, where, params)
        
        # Add to undo stack
        for row in old_data_result.to_dicts():
            self._undo_stack.append(DataChange(
                operation='insert',
                table_name=table_name,
                new_data=row
            ))
        
        return deleted
    
    def commit_changes(self) -> int:
        """Commit all pending changes.
        
        Returns:
            Number of changes committed
        """
        if self.db_service.readonly:
            raise ReadOnlyError("Database is in read-only mode")
        
        repo = self.get_repository()
        if not repo:
            raise DatabaseError("No active database")
        
        count = 0
        
        with repo.client.transaction():
            for change in self.pending_changes:
                if change.operation == 'insert':
                    repo.insert(change.table_name, change.new_data)
                elif change.operation == 'update':
                    repo.update(
                        change.table_name,
                        change.new_data,
                        change.where_clause,
                        change.where_params
                    )
                elif change.operation == 'delete':
                    repo.delete(
                        change.table_name,
                        change.where_clause,
                        change.where_params
                    )
                count += 1
        
        self.pending_changes.clear()
        return count
    
    def discard_changes(self) -> int:
        """Discard all pending changes.
        
        Returns:
            Number of changes discarded
        """
        count = len(self.pending_changes)
        self.pending_changes.clear()
        return count
    
    def undo_last_operation(self) -> bool:
        """Undo the last committed operation.
        
        Returns:
            True if successful
        """
        if not self._undo_stack:
            return False
        
        if self.db_service.readonly:
            raise ReadOnlyError("Database is in read-only mode")
        
        repo = self.get_repository()
        if not repo:
            return False
        
        change = self._undo_stack.pop()
        
        try:
            if change.operation == 'insert':
                repo.insert(change.table_name, change.new_data)
            elif change.operation == 'update':
                repo.update(
                    change.table_name,
                    change.new_data,
                    change.where_clause,
                    change.where_params
                )
            elif change.operation == 'delete':
                repo.delete(
                    change.table_name,
                    change.where_clause,
                    change.where_params
                )
            return True
        except DatabaseError:
            return False
    
    def get_pending_changes_count(self) -> int:
        """Get number of pending changes."""
        return len(self.pending_changes)
    
    def get_undo_count(self) -> int:
        """Get number of operations that can be undone."""
        return len(self._undo_stack)


class AsyncTableService:
    """Async service for table data operations."""
    
    def __init__(self, database_service: AsyncDatabaseService):
        """Initialize async table service."""
        self.db_service = database_service
        self.pending_changes: List[DataChange] = []
        self._undo_stack: List[DataChange] = []
        config = get_config()
        self.default_page_size = config.ui.page_size
    
    # Similar async implementations of all methods...
    # (Omitted for brevity, but would follow the same pattern as TableService)
