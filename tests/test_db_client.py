"""Tests for database client."""

import pytest
import sqlite3
import tempfile
from pathlib import Path

from open_sqlui.core.db_client import DatabaseClient, QueryResult, DatabaseError, ReadOnlyError


@pytest.fixture
def test_db():
    """Create a temporary test database."""
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        db_path = Path(f.name)
    
    # Create test database with sample data
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY,
            name TEXT,
            value INTEGER
        )
    """)
    
    cursor.executemany(
        "INSERT INTO test_table (name, value) VALUES (?, ?)",
        [("Alice", 100), ("Bob", 200), ("Charlie", 300)]
    )
    
    conn.commit()
    conn.close()
    
    yield db_path
    
    # Cleanup
    db_path.unlink()


def test_database_client_connect(test_db):
    """Test database connection."""
    client = DatabaseClient(test_db, readonly=True)
    client.connect()
    
    assert client._connection is not None
    
    client.disconnect()
    assert client._connection is None


def test_database_client_execute_select(test_db):
    """Test executing SELECT query."""
    client = DatabaseClient(test_db, readonly=True)
    
    result = client.execute("SELECT * FROM test_table")
    
    assert isinstance(result, QueryResult)
    assert len(result.rows) == 3
    assert result.columns == ['id', 'name', 'value']
    assert result.rows[0][1] == 'Alice'
    
    client.disconnect()


def test_database_client_readonly_mode(test_db):
    """Test read-only mode prevents writes."""
    client = DatabaseClient(test_db, readonly=True)
    
    with pytest.raises(ReadOnlyError):
        client.execute("INSERT INTO test_table (name, value) VALUES ('Dave', 400)")
    
    with pytest.raises(ReadOnlyError):
        client.execute("UPDATE test_table SET value = 500 WHERE id = 1")
    
    with pytest.raises(ReadOnlyError):
        client.execute("DELETE FROM test_table WHERE id = 1")
    
    client.disconnect()


def test_database_client_write_mode(test_db):
    """Test write operations in non-readonly mode."""
    client = DatabaseClient(test_db, readonly=False)
    
    # Test INSERT
    result = client.execute("INSERT INTO test_table (name, value) VALUES ('Dave', 400)")
    assert result.lastrowid is not None
    
    # Test UPDATE
    result = client.execute("UPDATE test_table SET value = 500 WHERE name = 'Dave'")
    assert result.rowcount == 1
    
    # Test DELETE
    result = client.execute("DELETE FROM test_table WHERE name = 'Dave'")
    assert result.rowcount == 1
    
    # Verify changes
    result = client.execute("SELECT COUNT(*) FROM test_table")
    assert result.rows[0][0] == 3  # Back to original count
    
    client.disconnect()


def test_database_client_get_tables(test_db):
    """Test getting list of tables."""
    client = DatabaseClient(test_db, readonly=True)
    
    tables = client.get_tables()
    
    assert 'test_table' in tables
    assert 'sqlite_' not in ''.join(tables)  # System tables should be excluded
    
    client.disconnect()


def test_database_client_get_table_info(test_db):
    """Test getting table schema information."""
    client = DatabaseClient(test_db, readonly=True)
    
    info = client.get_table_info('test_table')
    
    assert len(info) == 3  # Three columns
    assert info[0]['name'] == 'id'
    assert info[0]['type'] == 'INTEGER'
    assert info[0]['pk'] == 1  # Primary key
    
    client.disconnect()


def test_database_client_get_table_count(test_db):
    """Test getting row count for a table."""
    client = DatabaseClient(test_db, readonly=True)
    
    count = client.get_table_count('test_table')
    
    assert count == 3
    
    client.disconnect()


def test_database_client_parameterized_query(test_db):
    """Test parameterized queries for safety."""
    client = DatabaseClient(test_db, readonly=True)
    
    result = client.execute(
        "SELECT * FROM test_table WHERE name = ?",
        ('Alice',)
    )
    
    assert len(result.rows) == 1
    assert result.rows[0][1] == 'Alice'
    
    client.disconnect()


def test_query_result_to_dicts(test_db):
    """Test converting query result to dictionaries."""
    client = DatabaseClient(test_db, readonly=True)
    
    result = client.execute("SELECT * FROM test_table LIMIT 1")
    dicts = result.to_dicts()
    
    assert len(dicts) == 1
    assert dicts[0]['name'] == 'Alice'
    assert dicts[0]['value'] == 100
    
    client.disconnect()
