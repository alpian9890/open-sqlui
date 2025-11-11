"""Command-line interface for Open SQLui."""

import click
import sys
from pathlib import Path
from typing import Optional

from . import __version__
from .app import OpenSQLuiApp
from .core.config import get_config_manager
from .services.file_browser import FileBrowser


@click.command()
@click.version_option(version=__version__, prog_name="open-sqlui")
@click.option(
    '-d', '--database',
    type=click.Path(exists=True, path_type=Path),
    help='Path to SQLite database to open on startup'
)
@click.option(
    '-r', '--readonly',
    is_flag=True,
    help='Open in read-only mode (overrides config)'
)
@click.option(
    '-e', '--editable',
    is_flag=True,
    help='Open in edit mode (overrides config)'
)
@click.option(
    '--theme',
    type=click.Choice(['dark', 'light', 'auto'], case_sensitive=False),
    help='Set UI theme'
)
@click.option(
    '--config-dir',
    type=click.Path(path_type=Path),
    help='Custom configuration directory'
)
@click.option(
    '--reset-config',
    is_flag=True,
    help='Reset configuration to defaults'
)
@click.option(
    '--list-recent',
    is_flag=True,
    help='List recently opened databases and exit'
)
@click.option(
    '--find-databases',
    type=click.Path(exists=True, path_type=Path),
    help='Find all databases in directory and exit'
)
@click.argument(
    'database_path',
    type=click.Path(exists=True, path_type=Path),
    required=False
)
def main(
    database: Optional[Path],
    readonly: bool,
    editable: bool,
    theme: Optional[str],
    config_dir: Optional[Path],
    reset_config: bool,
    list_recent: bool,
    find_databases: Optional[Path],
    database_path: Optional[Path]
) -> None:
    """Open SQLui - Terminal SQLite Database Manager
    
    A powerful and user-friendly TUI for managing SQLite databases.
    
    Examples:
    
        # Open the app in current directory
        open-sqlui
        
        # Open a specific database
        open-sqlui mydatabase.db
        
        # Open in read-only mode
        open-sqlui -r mydatabase.db
        
        # Find all databases in a directory
        open-sqlui --find-databases /path/to/dir
    """
    
    # Handle utility commands first
    if reset_config:
        config_manager = get_config_manager()
        config_manager.reset_to_defaults()
        click.echo("Configuration reset to defaults")
        return
    
    if list_recent:
        config_manager = get_config_manager()
        config = config_manager.config
        
        if config.database.recent_databases:
            click.echo("Recently opened databases:")
            for i, db_path in enumerate(config.database.recent_databases, 1):
                p = Path(db_path)
                exists = "✓" if p.exists() else "✗"
                click.echo(f"  {i}. [{exists}] {db_path}")
        else:
            click.echo("No recently opened databases")
        return
    
    if find_databases:
        browser = FileBrowser()
        click.echo(f"Searching for databases in {find_databases}...")
        
        databases = browser.find_databases(find_databases, recursive=True)
        
        if databases:
            click.echo(f"Found {len(databases)} database(s):")
            for db_info in databases:
                size = db_info.size_str
                tables = f"({db_info.table_count} tables)" if db_info.table_count is not None else ""
                click.echo(f"  • {db_info.path} - {size} {tables}")
        else:
            click.echo("No databases found")
        return
    
    # Load configuration with custom directory if provided
    if config_dir:
        config_manager = get_config_manager()
        config_manager.config_dir = config_dir
        config_manager.load()
    else:
        config_manager = get_config_manager()
    
    config = config_manager.config
    
    # Apply command-line overrides
    if readonly and editable:
        click.echo("Error: Cannot use both --readonly and --editable", err=True)
        sys.exit(1)
    
    if readonly:
        config.database.readonly_mode = True
    elif editable:
        config.database.readonly_mode = False
    
    if theme:
        config.ui.theme = theme
    
    # Determine which database to open
    db_to_open = database_path or database
    
    if db_to_open:
        # Verify it's a SQLite database
        browser = FileBrowser()
        if not browser.is_sqlite_database(db_to_open):
            click.echo(f"Error: {db_to_open} is not a valid SQLite database", err=True)
            sys.exit(1)
        
        # Set as default path for this session
        config.database.default_path = str(db_to_open)
    
    # Create and run the app
    try:
        app = OpenSQLuiApp()
        app.run()
    except KeyboardInterrupt:
        click.echo("\nInterrupted by user")
        sys.exit(0)
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
