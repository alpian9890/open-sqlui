"""Configuration management for Open SQLui."""

import os
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Dict, Any, Optional, List
import yaml
import toml
from typing_extensions import Self


@dataclass
class KeyBindings:
    """Keyboard shortcut configuration."""
    navigate_up: str = "up"
    navigate_down: str = "down"
    navigate_left: str = "left"
    navigate_right: str = "right"
    select: str = "enter"
    back: str = "escape"
    help: str = "?"
    quit: str = "q"
    search: str = "/"
    refresh: str = "f5"
    export: str = "ctrl+e"
    sql_console: str = "ctrl+s"
    new_query: str = "ctrl+n"
    execute_query: str = "f9"
    toggle_readonly: str = "ctrl+r"
    page_up: str = "pageup"
    page_down: str = "pagedown"


@dataclass
class UISettings:
    """UI appearance settings."""
    theme: str = "dark"  # dark, light, auto
    show_row_numbers: bool = True
    show_status_bar: bool = True
    show_breadcrumbs: bool = True
    highlight_syntax: bool = True
    show_table_counts: bool = True
    max_column_width: int = 50
    page_size: int = 100
    show_null_as: str = "NULL"
    date_format: str = "%Y-%m-%d %H:%M:%S"


@dataclass
class DatabaseSettings:
    """Database connection and behavior settings."""
    readonly_mode: bool = True
    auto_commit: bool = False
    connection_timeout: int = 5
    query_timeout: int = 30
    max_connections: int = 5
    cache_metadata: bool = True
    history_size: int = 100
    default_path: Optional[str] = None
    recent_databases: List[str] = field(default_factory=list)
    favorites: List[str] = field(default_factory=list)


@dataclass
class ExportSettings:
    """Export configuration."""
    default_format: str = "csv"  # csv, json
    csv_delimiter: str = ","
    csv_quotechar: str = '"'
    json_indent: int = 2
    include_headers: bool = True
    default_directory: Optional[str] = None
    confirm_overwrite: bool = True


@dataclass
class Config:
    """Main configuration class."""
    keybindings: KeyBindings = field(default_factory=KeyBindings)
    ui: UISettings = field(default_factory=UISettings)
    database: DatabaseSettings = field(default_factory=DatabaseSettings)
    export: ExportSettings = field(default_factory=ExportSettings)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Self:
        """Create config from dictionary."""
        keybindings = KeyBindings(**data.get("keybindings", {}))
        ui = UISettings(**data.get("ui", {}))
        database = DatabaseSettings(**data.get("database", {}))
        export = ExportSettings(**data.get("export", {}))
        
        return cls(
            keybindings=keybindings,
            ui=ui,
            database=database,
            export=export
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert config to dictionary."""
        return {
            "keybindings": asdict(self.keybindings),
            "ui": asdict(self.ui),
            "database": asdict(self.database),
            "export": asdict(self.export)
        }


class ConfigManager:
    """Manages loading and saving configuration."""
    
    DEFAULT_CONFIG_DIR = Path.home() / ".config" / "open-sqlui"
    DEFAULT_CONFIG_FILE = "config.yaml"
    
    def __init__(self, config_dir: Optional[Path] = None):
        """Initialize config manager.
        
        Args:
            config_dir: Custom config directory (defaults to ~/.config/open-sqlui)
        """
        self.config_dir = config_dir or self.DEFAULT_CONFIG_DIR
        self.config_file = self.config_dir / self.DEFAULT_CONFIG_FILE
        self.config = Config()
        
    def ensure_config_dir(self) -> None:
        """Create config directory if it doesn't exist."""
        self.config_dir.mkdir(parents=True, exist_ok=True)
        
    def load(self) -> Config:
        """Load configuration from file or create default."""
        self.ensure_config_dir()
        
        # Try loading from YAML first
        if self.config_file.exists():
            try:
                with open(self.config_file, 'r') as f:
                    data = yaml.safe_load(f) or {}
                    self.config = Config.from_dict(data)
            except Exception as e:
                print(f"Error loading config: {e}")
                self.config = Config()
        else:
            # Try loading from TOML as fallback
            toml_file = self.config_dir / "config.toml"
            if toml_file.exists():
                try:
                    with open(toml_file, 'r') as f:
                        data = toml.load(f)
                        self.config = Config.from_dict(data)
                except Exception as e:
                    print(f"Error loading TOML config: {e}")
                    self.config = Config()
            else:
                # Create default config
                self.config = Config()
                self.save()
        
        # Apply environment variable overrides
        self._apply_env_overrides()
        
        return self.config
    
    def save(self) -> None:
        """Save current configuration to file."""
        self.ensure_config_dir()
        
        try:
            with open(self.config_file, 'w') as f:
                yaml.dump(self.config.to_dict(), f, default_flow_style=False)
        except Exception as e:
            print(f"Error saving config: {e}")
    
    def _apply_env_overrides(self) -> None:
        """Apply environment variable overrides to config."""
        # Database path
        if db_path := os.environ.get("OPEN_SQLUI_DB_PATH"):
            self.config.database.default_path = db_path
        
        # Read-only mode
        if readonly := os.environ.get("OPEN_SQLUI_READONLY"):
            self.config.database.readonly_mode = readonly.lower() in ("true", "1", "yes")
        
        # Theme
        if theme := os.environ.get("OPEN_SQLUI_THEME"):
            if theme in ("dark", "light", "auto"):
                self.config.ui.theme = theme
        
        # Page size
        if page_size := os.environ.get("OPEN_SQLUI_PAGE_SIZE"):
            try:
                self.config.ui.page_size = int(page_size)
            except ValueError:
                pass
        
        # Export directory
        if export_dir := os.environ.get("OPEN_SQLUI_EXPORT_DIR"):
            self.config.export.default_directory = export_dir
    
    def add_recent_database(self, path: str) -> None:
        """Add a database to recent list."""
        if path in self.config.database.recent_databases:
            self.config.database.recent_databases.remove(path)
        
        self.config.database.recent_databases.insert(0, path)
        
        # Keep only last 10 recent databases
        self.config.database.recent_databases = self.config.database.recent_databases[:10]
        
        self.save()
    
    def add_favorite_database(self, path: str) -> None:
        """Add a database to favorites."""
        if path not in self.config.database.favorites:
            self.config.database.favorites.append(path)
            self.save()
    
    def remove_favorite_database(self, path: str) -> None:
        """Remove a database from favorites."""
        if path in self.config.database.favorites:
            self.config.database.favorites.remove(path)
            self.save()
    
    def get_keybinding(self, action: str) -> str:
        """Get keybinding for an action."""
        return getattr(self.config.keybindings, action, "")
    
    def reset_to_defaults(self) -> None:
        """Reset configuration to defaults."""
        self.config = Config()
        self.save()


# Global config instance
_config_manager: Optional[ConfigManager] = None


def get_config_manager() -> ConfigManager:
    """Get the global config manager instance."""
    global _config_manager
    if _config_manager is None:
        _config_manager = ConfigManager()
        _config_manager.load()
    return _config_manager


def get_config() -> Config:
    """Get the current configuration."""
    return get_config_manager().config
