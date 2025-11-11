"""Main Textual application for Open SQLui."""

from pathlib import Path
from typing import Optional

from rich.text import Text
from textual import on
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Container, Horizontal, Vertical, ScrollableContainer
from textual.reactive import reactive
from textual.screen import ModalScreen, Screen
from textual.widgets import (
    Button,
    DataTable,
    DirectoryTree,
    Footer,
    Header,
    Input,
    Label,
    ListItem,
    ListView,
    Placeholder,
    Static,
    TextArea,
    Tree,
)
from textual.message import Message

from .core.config import get_config, get_config_manager, Config
from .services.database_service import DatabaseService, DatabaseInfo
from .services.table_service import TableService
from .services.file_browser import FileBrowser, FileInfo


class HelpScreen(ModalScreen):
    """Modal screen showing help and keyboard shortcuts."""
    
    BINDINGS = [
        ("escape", "dismiss", "Close"),
    ]
    
    def compose(self) -> ComposeResult:
        """Compose help screen layout."""
        with Container(id="help-dialog"):
            yield Static("# Open SQLui - Keyboard Shortcuts", id="help-title")
            
            shortcuts = [
                ("Navigation", [
                    ("↑/↓/←/→", "Navigate with arrow keys"),
                    ("Tab", "Next widget"),
                    ("Shift+Tab", "Previous widget"),
                    ("Enter", "Select/Open"),
                    ("Escape", "Go back/Cancel"),
                    ("PageUp/PageDown", "Navigate pages"),
                ]),
                ("Database", [
                    ("Ctrl+O", "Open database"),
                    ("Ctrl+W", "Close database"),
                    ("Ctrl+Tab", "Switch database"),
                    ("Ctrl+R", "Toggle read-only mode"),
                    ("F5", "Refresh"),
                ]),
                ("Data", [
                    ("Ctrl+N", "New record"),
                    ("Ctrl+E", "Export data"),
                    ("Ctrl+S", "SQL console"),
                    ("/", "Search"),
                    ("Ctrl+Z", "Undo"),
                ]),
                ("Application", [
                    ("?", "Show this help"),
                    ("Q", "Quit application"),
                ]),
            ]
            
            for section, items in shortcuts:
                yield Static(f"\n## {section}", classes="help-section")
                for key, desc in items:
                    yield Static(f"  {key:20} {desc}", classes="help-item")
    
    def on_mount(self) -> None:
        """Set focus on mount."""
        self.query_one("#help-dialog").focus()


class StatusBar(Static):
    """Custom status bar widget."""
    
    database_path = reactive("")
    table_name = reactive("")
    row_count = reactive(0)
    readonly = reactive(True)
    message = reactive("")
    
    def render(self) -> Text:
        """Render status bar."""
        parts = []
        
        if self.database_path:
            db_name = Path(self.database_path).name
            parts.append(f"📁 {db_name}")
        
        if self.table_name:
            parts.append(f"📊 {self.table_name}")
            if self.row_count > 0:
                parts.append(f"({self.row_count} rows)")
        
        mode = "🔒 Read-only" if self.readonly else "✏️  Edit mode"
        parts.append(mode)
        
        if self.message:
            parts.append(f"| {self.message}")
        
        return Text(" │ ".join(parts))


class DatabaseTreeWidget(Tree):
    """Tree widget for database tables."""
    
    def __init__(self, *args, **kwargs):
        """Initialize database tree."""
        super().__init__("No Database", *args, **kwargs)
        self.database_service: Optional[DatabaseService] = None
        self.table_service: Optional[TableService] = None
    
    def set_services(self, db_service: DatabaseService, table_service: TableService) -> None:
        """Set service references."""
        self.database_service = db_service
        self.table_service = table_service
    
    def load_database(self, db_path: Path) -> None:
        """Load database structure into tree."""
        if not self.database_service:
            return
        
        # Open database
        info = self.database_service.open_database(db_path)
        
        # Clear and set root
        self.clear()
        self.root.label = f"🗄️ {info.name}"
        
        # Get tables
        tables = self.database_service.get_tables()
        
        # Add tables to tree
        for table in tables:
            schema = self.database_service.get_table_schema(table)
            if schema:
                table_node = self.root.add(
                    f"📊 {table} ({schema.row_count} rows)",
                    expand=False
                )
                
                # Add columns
                for col in schema.columns:
                    col_type = col.get('type', 'UNKNOWN')
                    col_name = col.get('name', '')
                    is_pk = col.get('pk', 0) > 0
                    
                    icon = "🔑" if is_pk else "📝"
                    table_node.add_leaf(f"{icon} {col_name} ({col_type})")
        
        self.root.expand()


class FileExplorerWidget(DirectoryTree):
    """File explorer for finding databases."""
    
    def __init__(self, path: str = ".", **kwargs):
        """Initialize file explorer."""
        super().__init__(path, **kwargs)
        self.file_browser = FileBrowser()
        self.show_only_databases = False
    
    def filter_paths(self, paths):
        """Filter paths to show only databases if requested."""
        if not self.show_only_databases:
            return paths
        
        filtered = []
        for path in paths:
            if path.is_dir() or self.file_browser.is_sqlite_database(path):
                filtered.append(path)
        return filtered


class TableDataWidget(DataTable):
    """Widget for displaying table data."""
    
    def __init__(self, **kwargs):
        """Initialize table data widget."""
        super().__init__(**kwargs)
        self.cursor_type = "row"
        self.zebra_stripes = True
        self.table_service: Optional[TableService] = None
        self.current_table: Optional[str] = None
        self.current_page = 1
    
    def set_service(self, table_service: TableService) -> None:
        """Set table service."""
        self.table_service = table_service
    
    def load_table(self, table_name: str, page: int = 1) -> None:
        """Load table data."""
        if not self.table_service:
            return
        
        self.current_table = table_name
        self.current_page = page
        
        # Clear current data
        self.clear(columns=True)
        
        # Get page data
        try:
            result = self.table_service.get_page(
                table_name=table_name,
                page=page
            )
            
            # Add columns
            for col in result.data.columns:
                self.add_column(col, key=col)
            
            # Add rows
            for row in result.data.rows:
                self.add_row(*row)
            
            # Update status
            self.post_message(
                TableDataUpdated(
                    table_name=table_name,
                    page=result.page,
                    total_pages=result.total_pages,
                    total_rows=result.total_rows
                )
            )
        except Exception as e:
            self.add_column("Error")
            self.add_row(str(e))


class TableDataUpdated(Message):
    """Message sent when table data is updated."""
    
    def __init__(self, table_name: str, page: int, total_pages: int, total_rows: int):
        """Initialize message."""
        super().__init__()
        self.table_name = table_name
        self.page = page
        self.total_pages = total_pages
        self.total_rows = total_rows


class OpenSQLuiApp(App):
    """Main Open SQLui application."""
    
    CSS = """
    #sidebar {
        width: 30;
        dock: left;
        background: $panel;
        border-right: solid $primary;
    }
    
    #main-content {
        background: $surface;
    }
    
    #status-bar {
        height: 1;
        dock: bottom;
        background: $panel;
        color: $text-muted;
    }
    
    #help-dialog {
        width: 80;
        height: 40;
        background: $surface;
        border: thick $primary;
        padding: 1 2;
    }
    
    #help-title {
        text-style: bold;
        color: $primary;
    }
    
    .help-section {
        margin-top: 1;
        text-style: bold;
        color: $secondary;
    }
    
    .help-item {
        color: $text;
    }
    
    DatabaseTreeWidget {
        background: $panel;
        padding: 0 1;
    }
    
    FileExplorerWidget {
        background: $panel;
        padding: 0 1;
    }
    
    TableDataWidget {
        height: 100%;
    }
    """
    
    BINDINGS = [
        Binding("ctrl+o", "open_database", "Open Database", priority=True),
        Binding("ctrl+w", "close_database", "Close Database"),
        Binding("ctrl+tab", "switch_database", "Switch Database"),
        Binding("ctrl+r", "toggle_readonly", "Toggle Read-only"),
        Binding("f5", "refresh", "Refresh"),
        Binding("ctrl+s", "sql_console", "SQL Console"),
        Binding("ctrl+e", "export_data", "Export"),
        Binding("/", "search", "Search"),
        Binding("?", "help", "Help"),
        Binding("q", "quit", "Quit"),
    ]
    
    def __init__(self):
        """Initialize application."""
        super().__init__()
        
        # Load configuration
        self.config = get_config()
        self.config_manager = get_config_manager()
        
        # Initialize services
        self.database_service = DatabaseService()
        self.table_service = TableService(self.database_service)
        self.file_browser = FileBrowser(self.config.database.favorites)
        
        # Current state
        self.current_database: Optional[Path] = None
        self.current_table: Optional[str] = None
    
    def compose(self) -> ComposeResult:
        """Compose application layout."""
        yield Header()
        
        with Horizontal():
            # Sidebar with database tree
            with Vertical(id="sidebar"):
                yield Label("📁 Database Explorer")
                yield DatabaseTreeWidget(id="db-tree")
                yield FileExplorerWidget(".", id="file-tree", show_root=False, show_guides=False)
            
            # Main content area
            with Vertical(id="main-content"):
                yield TableDataWidget(id="table-data", show_header=True, show_row_labels=True)
        
        yield StatusBar(id="status-bar")
        yield Footer()
    
    def on_mount(self) -> None:
        """Initialize on mount."""
        # Get widgets
        self.db_tree = self.query_one("#db-tree", DatabaseTreeWidget)
        self.file_tree = self.query_one("#file-tree", FileExplorerWidget)
        self.table_data = self.query_one("#table-data", TableDataWidget)
        self.status_bar = self.query_one("#status-bar", StatusBar)
        
        # Set service references
        self.db_tree.set_services(self.database_service, self.table_service)
        self.table_data.set_service(self.table_service)
        
        # Update status bar
        self.status_bar.readonly = self.database_service.readonly
        
        # Load default database if configured
        if self.config.database.default_path:
            path = Path(self.config.database.default_path)
            if path.exists() and self.file_browser.is_sqlite_database(path):
                self.open_database_file(path)
        
        # Set initial focus
        self.file_tree.focus()
    
    @on(DirectoryTree.FileSelected)
    def on_file_selected(self, event: DirectoryTree.FileSelected) -> None:
        """Handle file selection from file tree."""
        path = event.path
        if self.file_browser.is_sqlite_database(path):
            self.open_database_file(path)
    
    @on(Tree.NodeSelected)
    def on_tree_node_selected(self, event: Tree.NodeSelected) -> None:
        """Handle node selection in database tree."""
        if event.node.parent and not event.node.children:
            # It's a table node
            label = str(event.node.label)
            if "📊" in label:
                # Extract table name
                table_name = label.split("📊")[1].split("(")[0].strip()
                self.load_table_data(table_name)
    
    def open_database_file(self, path: Path) -> None:
        """Open a database file."""
        try:
            # Open in database service
            info = self.database_service.open_database(path)
            
            # Update tree
            self.db_tree.load_database(path)
            
            # Update state
            self.current_database = path
            
            # Update status bar
            self.status_bar.database_path = str(path)
            self.status_bar.message = f"Opened {info.name}"
            
            # Show notification
            self.notify(f"Database opened: {info.name}", severity="information")
            
        except Exception as e:
            self.notify(f"Failed to open database: {e}", severity="error")
    
    def load_table_data(self, table_name: str) -> None:
        """Load table data into data grid."""
        try:
            self.current_table = table_name
            self.table_data.load_table(table_name)
            
            # Update status bar
            self.status_bar.table_name = table_name
            
        except Exception as e:
            self.notify(f"Failed to load table: {e}", severity="error")
    
    @on(TableDataUpdated)
    def on_table_data_updated(self, event: TableDataUpdated) -> None:
        """Handle table data update."""
        self.status_bar.table_name = event.table_name
        self.status_bar.row_count = event.total_rows
        
        if event.total_pages > 1:
            self.status_bar.message = f"Page {event.page}/{event.total_pages}"
        else:
            self.status_bar.message = ""
    
    def action_open_database(self) -> None:
        """Open database action."""
        # This would open a file picker dialog
        # For now, focus on file tree
        self.file_tree.focus()
        self.notify("Use the file explorer to select a database", severity="information")
    
    def action_close_database(self) -> None:
        """Close current database."""
        if self.current_database:
            self.database_service.close_database(self.current_database)
            self.db_tree.clear()
            self.db_tree.root.label = "No Database"
            self.table_data.clear(columns=True)
            
            self.current_database = None
            self.current_table = None
            
            self.status_bar.database_path = ""
            self.status_bar.table_name = ""
            self.status_bar.row_count = 0
            self.status_bar.message = "Database closed"
            
            self.notify("Database closed", severity="information")
    
    def action_toggle_readonly(self) -> None:
        """Toggle read-only mode."""
        new_state = self.database_service.toggle_readonly_mode()
        self.status_bar.readonly = new_state
        
        mode = "read-only" if new_state else "edit"
        self.notify(f"Switched to {mode} mode", severity="information")
    
    def action_refresh(self) -> None:
        """Refresh current view."""
        if self.current_database:
            self.db_tree.load_database(self.current_database)
            
        if self.current_table:
            self.table_data.load_table(self.current_table)
        
        self.notify("Refreshed", severity="information")
    
    def action_help(self) -> None:
        """Show help screen."""
        self.push_screen(HelpScreen())
    
    def action_sql_console(self) -> None:
        """Open SQL console."""
        self.notify("SQL console not yet implemented", severity="warning")
    
    def action_export_data(self) -> None:
        """Export data."""
        self.notify("Export not yet implemented", severity="warning")
    
    def action_search(self) -> None:
        """Search in data."""
        self.notify("Search not yet implemented", severity="warning")
    
    def action_quit(self) -> None:
        """Quit application."""
        self.database_service.close_all_databases()
        self.exit()


def run_app() -> None:
    """Run the Open SQLui application."""
    app = OpenSQLuiApp()
    app.run()
