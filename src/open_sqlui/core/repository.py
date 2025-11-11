"""Repository pattern for database operations."""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path

from .db_client import DatabaseClient, AsyncDatabaseClient, QueryResult, DatabaseError


@dataclass
class TableSchema:
    """Schema information for a table."""
    name: str
    columns: List[Dict[str, Any]]
    row_count: int
    primary_key: Optional[str] = None
    foreign_keys: List[Dict[str, Any]] = None
    indexes: List[Dict[str, Any]] = None
    
    @property
    def column_names(self) -> List[str]:
        """Get list of column names."""
        return [col['name'] for col in self.columns]
    
    @property
    def column_types(self) -> Dict[str, str]:
        """Get mapping of column names to types."""
        return {col['name']: col['type'] for col in self.columns}


class Repository:
    """Base repository for database operations."""
    
    def __init__(self, client: DatabaseClient):
        """Initialize repository.
        
        Args:
            client: Database client instance
        """
        self.client = client
    
    def get_all_tables(self) -> List[str]:
        """Get list of all tables."""
        return self.client.get_tables()
    
    def get_table_schema(self, table_name: str) -> TableSchema:
        """Get complete schema information for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            TableSchema object
        """
        # Get columns
        columns = self.client.get_table_info(table_name)
        
        # Get row count
        row_count = self.client.get_table_count(table_name)
        
        # Find primary key
        primary_key = None
        for col in columns:
            if col.get('pk', 0) > 0:
                primary_key = col['name']
                break
        
        # Get foreign keys
        fk_query = f"PRAGMA foreign_key_list({table_name})"
        fk_result = self.client.execute(fk_query)
        foreign_keys = fk_result.to_dicts() if fk_result.rows else []
        
        # Get indexes
        idx_query = f"PRAGMA index_list({table_name})"
        idx_result = self.client.execute(idx_query)
        indexes = idx_result.to_dicts() if idx_result.rows else []
        
        return TableSchema(
            name=table_name,
            columns=columns,
            row_count=row_count,
            primary_key=primary_key,
            foreign_keys=foreign_keys,
            indexes=indexes
        )
    
    def select(
        self,
        table_name: str,
        columns: Optional[List[str]] = None,
        where: Optional[str] = None,
        params: Optional[Tuple] = None,
        order_by: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None
    ) -> QueryResult:
        """Select data from a table.
        
        Args:
            table_name: Name of the table
            columns: List of columns to select (None for all)
            where: WHERE clause (without WHERE keyword)
            params: Parameters for the WHERE clause
            order_by: ORDER BY clause (without ORDER BY keyword)
            limit: Maximum number of rows to return
            offset: Number of rows to skip
            
        Returns:
            QueryResult object
        """
        # Build SELECT clause
        if columns:
            columns_str = ", ".join(columns)
        else:
            columns_str = "*"
        
        # Build query
        query = f"SELECT {columns_str} FROM {table_name}"
        
        # Add WHERE clause
        if where:
            query += f" WHERE {where}"
        
        # Add ORDER BY clause
        if order_by:
            query += f" ORDER BY {order_by}"
        
        # Add LIMIT and OFFSET
        if limit:
            query += f" LIMIT {limit}"
            if offset:
                query += f" OFFSET {offset}"
        
        return self.client.execute(query, params)
    
    def insert(
        self,
        table_name: str,
        data: Dict[str, Any]
    ) -> int:
        """Insert a row into a table.
        
        Args:
            table_name: Name of the table
            data: Dictionary of column values
            
        Returns:
            ID of the inserted row
        """
        if not data:
            raise ValueError("Cannot insert empty data")
        
        columns = list(data.keys())
        placeholders = ", ".join(["?" for _ in columns])
        columns_str = ", ".join(columns)
        
        query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
        params = tuple(data.values())
        
        result = self.client.execute(query, params)
        return result.lastrowid
    
    def update(
        self,
        table_name: str,
        data: Dict[str, Any],
        where: str,
        params: Optional[Tuple] = None
    ) -> int:
        """Update rows in a table.
        
        Args:
            table_name: Name of the table
            data: Dictionary of column values to update
            where: WHERE clause (without WHERE keyword)
            params: Parameters for the WHERE clause
            
        Returns:
            Number of affected rows
        """
        if not data:
            raise ValueError("Cannot update with empty data")
        
        # Build SET clause
        set_parts = [f"{col} = ?" for col in data.keys()]
        set_clause = ", ".join(set_parts)
        
        # Build query
        query = f"UPDATE {table_name} SET {set_clause} WHERE {where}"
        
        # Combine data values and WHERE parameters
        all_params = list(data.values())
        if params:
            all_params.extend(params)
        
        result = self.client.execute(query, tuple(all_params))
        return result.rowcount
    
    def delete(
        self,
        table_name: str,
        where: str,
        params: Optional[Tuple] = None
    ) -> int:
        """Delete rows from a table.
        
        Args:
            table_name: Name of the table
            where: WHERE clause (without WHERE keyword)
            params: Parameters for the WHERE clause
            
        Returns:
            Number of deleted rows
        """
        query = f"DELETE FROM {table_name} WHERE {where}"
        result = self.client.execute(query, params)
        return result.rowcount
    
    def search(
        self,
        table_name: str,
        search_term: str,
        columns: Optional[List[str]] = None
    ) -> QueryResult:
        """Search for rows containing a term in specified columns.
        
        Args:
            table_name: Name of the table
            search_term: Term to search for
            columns: Columns to search in (None for all text columns)
            
        Returns:
            QueryResult object
        """
        # Get table schema to find searchable columns
        schema = self.get_table_schema(table_name)
        
        if not columns:
            # Use all text-like columns
            columns = [
                col['name'] for col in schema.columns
                if col['type'].upper() in ('TEXT', 'VARCHAR', 'CHAR', 'BLOB')
            ]
        
        if not columns:
            # No searchable columns
            return QueryResult(columns=[], rows=[], rowcount=0)
        
        # Build WHERE clause with LIKE conditions
        conditions = [f"{col} LIKE ?" for col in columns]
        where_clause = " OR ".join(conditions)
        
        # Search term with wildcards
        search_pattern = f"%{search_term}%"
        params = tuple([search_pattern] * len(columns))
        
        return self.select(
            table_name=table_name,
            where=where_clause,
            params=params
        )


class AsyncRepository:
    """Async repository for database operations."""
    
    def __init__(self, client: AsyncDatabaseClient):
        """Initialize async repository.
        
        Args:
            client: Async database client instance
        """
        self.client = client
    
    async def get_all_tables(self) -> List[str]:
        """Get list of all tables."""
        return await self.client.get_tables()
    
    async def get_table_schema(self, table_name: str) -> TableSchema:
        """Get complete schema information for a table."""
        # Get columns
        columns = await self.client.get_table_info(table_name)
        
        # Get row count
        row_count = await self.client.get_table_count(table_name)
        
        # Find primary key
        primary_key = None
        for col in columns:
            if col.get('pk', 0) > 0:
                primary_key = col['name']
                break
        
        # Get foreign keys
        fk_query = f"PRAGMA foreign_key_list({table_name})"
        fk_result = await self.client.execute(fk_query)
        foreign_keys = fk_result.to_dicts() if fk_result.rows else []
        
        # Get indexes
        idx_query = f"PRAGMA index_list({table_name})"
        idx_result = await self.client.execute(idx_query)
        indexes = idx_result.to_dicts() if idx_result.rows else []
        
        return TableSchema(
            name=table_name,
            columns=columns,
            row_count=row_count,
            primary_key=primary_key,
            foreign_keys=foreign_keys,
            indexes=indexes
        )
    
    async def select(
        self,
        table_name: str,
        columns: Optional[List[str]] = None,
        where: Optional[str] = None,
        params: Optional[Tuple] = None,
        order_by: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None
    ) -> QueryResult:
        """Select data from a table."""
        # Build SELECT clause
        if columns:
            columns_str = ", ".join(columns)
        else:
            columns_str = "*"
        
        # Build query
        query = f"SELECT {columns_str} FROM {table_name}"
        
        # Add WHERE clause
        if where:
            query += f" WHERE {where}"
        
        # Add ORDER BY clause
        if order_by:
            query += f" ORDER BY {order_by}"
        
        # Add LIMIT and OFFSET
        if limit:
            query += f" LIMIT {limit}"
            if offset:
                query += f" OFFSET {offset}"
        
        return await self.client.execute(query, params)
    
    async def insert(self, table_name: str, data: Dict[str, Any]) -> int:
        """Insert a row into a table."""
        if not data:
            raise ValueError("Cannot insert empty data")
        
        columns = list(data.keys())
        placeholders = ", ".join(["?" for _ in columns])
        columns_str = ", ".join(columns)
        
        query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
        params = tuple(data.values())
        
        result = await self.client.execute(query, params)
        return result.lastrowid
    
    async def update(
        self,
        table_name: str,
        data: Dict[str, Any],
        where: str,
        params: Optional[Tuple] = None
    ) -> int:
        """Update rows in a table."""
        if not data:
            raise ValueError("Cannot update with empty data")
        
        # Build SET clause
        set_parts = [f"{col} = ?" for col in data.keys()]
        set_clause = ", ".join(set_parts)
        
        # Build query
        query = f"UPDATE {table_name} SET {set_clause} WHERE {where}"
        
        # Combine data values and WHERE parameters
        all_params = list(data.values())
        if params:
            all_params.extend(params)
        
        result = await self.client.execute(query, tuple(all_params))
        return result.rowcount
    
    async def delete(
        self,
        table_name: str,
        where: str,
        params: Optional[Tuple] = None
    ) -> int:
        """Delete rows from a table."""
        query = f"DELETE FROM {table_name} WHERE {where}"
        result = await self.client.execute(query, params)
        return result.rowcount
    
    async def search(
        self,
        table_name: str,
        search_term: str,
        columns: Optional[List[str]] = None
    ) -> QueryResult:
        """Search for rows containing a term in specified columns."""
        # Get table schema to find searchable columns
        schema = await self.get_table_schema(table_name)
        
        if not columns:
            # Use all text-like columns
            columns = [
                col['name'] for col in schema.columns
                if col['type'].upper() in ('TEXT', 'VARCHAR', 'CHAR', 'BLOB')
            ]
        
        if not columns:
            # No searchable columns
            return QueryResult(columns=[], rows=[], rowcount=0)
        
        # Build WHERE clause with LIKE conditions
        conditions = [f"{col} LIKE ?" for col in columns]
        where_clause = " OR ".join(conditions)
        
        # Search term with wildcards
        search_pattern = f"%{search_term}%"
        params = tuple([search_pattern] * len(columns))
        
        return await self.select(
            table_name=table_name,
            where=where_clause,
            params=params
        )
