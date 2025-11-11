"""Export service for CSV and JSON export functionality."""

import csv
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
import pandas as pd

from ..core.db_client import QueryResult, DatabaseError
from ..core.repository import Repository
from ..core.config import get_config


@dataclass
class ExportOptions:
    """Options for data export."""
    format: str  # 'csv' or 'json'
    include_headers: bool = True
    csv_delimiter: str = ","
    csv_quotechar: str = '"'
    json_indent: int = 2
    output_path: Optional[Path] = None
    confirm_overwrite: bool = True


@dataclass
class ExportResult:
    """Result of an export operation."""
    success: bool
    output_path: Path
    row_count: int
    file_size: int
    error: Optional[str] = None
    
    @property
    def size_str(self) -> str:
        """Get human-readable size string."""
        size = self.file_size
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size < 1024:
                return f"{size:.1f} {unit}"
            size /= 1024
        return f"{size:.1f} TB"


class ExportService:
    """Service for exporting database data."""
    
    def __init__(self):
        """Initialize export service."""
        config = get_config()
        self.default_format = config.export.default_format
        self.csv_delimiter = config.export.csv_delimiter
        self.csv_quotechar = config.export.csv_quotechar
        self.json_indent = config.export.json_indent
        self.include_headers = config.export.include_headers
        self.default_directory = config.export.default_directory
        self.confirm_overwrite = config.export.confirm_overwrite
    
    def export_query_result(
        self,
        result: QueryResult,
        output_path: Path,
        options: Optional[ExportOptions] = None
    ) -> ExportResult:
        """Export query result to file.
        
        Args:
            result: Query result to export
            output_path: Output file path
            options: Export options
            
        Returns:
            ExportResult object
        """
        if not options:
            options = self._get_default_options()
        
        # Check if file exists and handle overwrite
        if output_path.exists() and options.confirm_overwrite:
            # In a real app, this would prompt the user
            # For now, we'll append a number to make it unique
            base = output_path.stem
            ext = output_path.suffix
            counter = 1
            while output_path.exists():
                output_path = output_path.parent / f"{base}_{counter}{ext}"
                counter += 1
        
        try:
            if options.format.lower() == 'csv':
                return self._export_to_csv(result, output_path, options)
            elif options.format.lower() == 'json':
                return self._export_to_json(result, output_path, options)
            else:
                return ExportResult(
                    success=False,
                    output_path=output_path,
                    row_count=0,
                    file_size=0,
                    error=f"Unsupported format: {options.format}"
                )
        except Exception as e:
            return ExportResult(
                success=False,
                output_path=output_path,
                row_count=0,
                file_size=0,
                error=str(e)
            )
    
    def export_table(
        self,
        repository: Repository,
        table_name: str,
        output_path: Path,
        options: Optional[ExportOptions] = None,
        where: Optional[str] = None,
        params: Optional[tuple] = None
    ) -> ExportResult:
        """Export entire table or filtered data.
        
        Args:
            repository: Repository instance
            table_name: Name of the table
            output_path: Output file path
            options: Export options
            where: Optional WHERE clause
            params: Optional parameters for WHERE clause
            
        Returns:
            ExportResult object
        """
        try:
            # Get data from table
            result = repository.select(
                table_name=table_name,
                where=where,
                params=params
            )
            
            # Export the result
            return self.export_query_result(result, output_path, options)
            
        except DatabaseError as e:
            return ExportResult(
                success=False,
                output_path=output_path,
                row_count=0,
                file_size=0,
                error=str(e)
            )
    
    def _export_to_csv(
        self,
        result: QueryResult,
        output_path: Path,
        options: ExportOptions
    ) -> ExportResult:
        """Export data to CSV format.
        
        Args:
            result: Query result
            output_path: Output file path
            options: Export options
            
        Returns:
            ExportResult object
        """
        # Create parent directory if needed
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Write CSV file
        with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(
                csvfile,
                delimiter=options.csv_delimiter,
                quotechar=options.csv_quotechar,
                quoting=csv.QUOTE_MINIMAL
            )
            
            # Write headers if requested
            if options.include_headers:
                writer.writerow(result.columns)
            
            # Write data rows
            writer.writerows(result.rows)
        
        # Get file size
        file_size = output_path.stat().st_size
        
        return ExportResult(
            success=True,
            output_path=output_path,
            row_count=len(result.rows),
            file_size=file_size
        )
    
    def _export_to_json(
        self,
        result: QueryResult,
        output_path: Path,
        options: ExportOptions
    ) -> ExportResult:
        """Export data to JSON format.
        
        Args:
            result: Query result
            output_path: Output file path
            options: Export options
            
        Returns:
            ExportResult object
        """
        # Create parent directory if needed
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Convert to list of dictionaries
        data = result.to_dicts()
        
        # Write JSON file
        with open(output_path, 'w', encoding='utf-8') as jsonfile:
            json.dump(
                data,
                jsonfile,
                indent=options.json_indent,
                ensure_ascii=False,
                default=str  # Convert non-serializable types to string
            )
        
        # Get file size
        file_size = output_path.stat().st_size
        
        return ExportResult(
            success=True,
            output_path=output_path,
            row_count=len(result.rows),
            file_size=file_size
        )
    
    def export_to_pandas(
        self,
        result: QueryResult,
        output_path: Path,
        format: str = 'csv'
    ) -> ExportResult:
        """Export using pandas for additional formats.
        
        Args:
            result: Query result
            output_path: Output file path
            format: Export format (csv, excel, parquet, etc.)
            
        Returns:
            ExportResult object
        """
        try:
            # Convert to pandas DataFrame
            df = pd.DataFrame(result.rows, columns=result.columns)
            
            # Create parent directory if needed
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Export based on format
            if format == 'csv':
                df.to_csv(output_path, index=False)
            elif format == 'json':
                df.to_json(output_path, orient='records', indent=2)
            elif format == 'excel' or format == 'xlsx':
                df.to_excel(output_path, index=False)
            elif format == 'parquet':
                df.to_parquet(output_path, index=False)
            elif format == 'html':
                df.to_html(output_path, index=False)
            else:
                return ExportResult(
                    success=False,
                    output_path=output_path,
                    row_count=0,
                    file_size=0,
                    error=f"Unsupported pandas format: {format}"
                )
            
            # Get file size
            file_size = output_path.stat().st_size
            
            return ExportResult(
                success=True,
                output_path=output_path,
                row_count=len(result.rows),
                file_size=file_size
            )
            
        except Exception as e:
            return ExportResult(
                success=False,
                output_path=output_path,
                row_count=0,
                file_size=0,
                error=str(e)
            )
    
    def _get_default_options(self) -> ExportOptions:
        """Get default export options from config.
        
        Returns:
            ExportOptions object
        """
        return ExportOptions(
            format=self.default_format,
            include_headers=self.include_headers,
            csv_delimiter=self.csv_delimiter,
            csv_quotechar=self.csv_quotechar,
            json_indent=self.json_indent,
            confirm_overwrite=self.confirm_overwrite
        )
    
    def suggest_filename(
        self,
        table_name: str,
        format: str = None
    ) -> str:
        """Suggest a filename for export.
        
        Args:
            table_name: Name of the table
            format: Export format
            
        Returns:
            Suggested filename
        """
        format = format or self.default_format
        
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        extension = {
            'csv': 'csv',
            'json': 'json',
            'excel': 'xlsx',
            'parquet': 'parquet',
            'html': 'html'
        }.get(format.lower(), format.lower())
        
        return f"{table_name}_{timestamp}.{extension}"
    
    def validate_path(self, path: Path) -> tuple[bool, Optional[str]]:
        """Validate an export path.
        
        Args:
            path: Path to validate
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        # Check if parent directory exists
        if not path.parent.exists():
            return False, f"Directory does not exist: {path.parent}"
        
        # Check if parent directory is writable
        if not os.access(path.parent, os.W_OK):
            return False, f"Directory is not writable: {path.parent}"
        
        # Check if file exists and is not writable
        if path.exists() and not os.access(path, os.W_OK):
            return False, f"File exists and is not writable: {path}"
        
        return True, None
