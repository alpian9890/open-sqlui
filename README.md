# Open SQLui - Terminal SQLite Database Manager

A powerful and user-friendly terminal user interface (TUI) for managing SQLite databases. Open SQLui provides an intuitive, keyboard-driven interface for browsing, querying, and managing SQLite databases directly from your terminal.

## Features

- 🗄️ **Database Management**: Open multiple SQLite databases simultaneously
- 📊 **Table Browser**: View and navigate through tables with pagination
- 🔍 **Search & Filter**: Search data across tables and columns
- 🔒 **Safety First**: Read-only mode by default with confirmations for destructive operations
- 📁 **File Explorer**: Built-in file browser to find SQLite databases
- 💾 **Data Export**: Export data to CSV and JSON formats
- ⌨️ **Keyboard Navigation**: Complete keyboard-driven interface with arrow keys and shortcuts
- 🎨 **Themes**: Dark and light themes with customizable appearance
- ⚙️ **Configuration**: Persistent configuration with environment variable overrides

## Installation

### From Source (Development)

```bash
# Clone the repository
git clone https://github.com/alpian9890/open-sqlui.git
cd open-sqlui

# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install in editable mode
pip install -e .
```

### Using pip (when published)

```bash
pip install open-sqlui
```

## Usage

### Basic Usage

```bash
# Launch Open SQLui in current directory
open-sqlui

# Open a specific database
open-sqlui mydatabase.db

# Open in read-only mode (default)
open-sqlui -r database.db

# Open in edit mode
open-sqlui -e database.db

# Open with a specific theme
open-sqlui --theme dark database.db
```

### Command-Line Options

```bash
open-sqlui --help
```

Options:
- `-d, --database PATH`: Path to SQLite database to open on startup
- `-r, --readonly`: Force read-only mode
- `-e, --editable`: Enable edit mode
- `--theme [dark|light|auto]`: Set UI theme
- `--config-dir PATH`: Custom configuration directory
- `--reset-config`: Reset configuration to defaults
- `--list-recent`: List recently opened databases
- `--find-databases PATH`: Find all databases in directory

### Keyboard Shortcuts

#### Navigation
- `↑/↓/←/→`: Navigate with arrow keys
- `Tab`: Next widget
- `Shift+Tab`: Previous widget
- `Enter`: Select/Open
- `Escape`: Go back/Cancel
- `PageUp/PageDown`: Navigate pages

#### Database Operations
- `Ctrl+O`: Open database
- `Ctrl+W`: Close database
- `Ctrl+Tab`: Switch database
- `Ctrl+R`: Toggle read-only mode
- `F5`: Refresh

#### Data Operations
- `Ctrl+N`: New record
- `Ctrl+E`: Export data
- `Ctrl+S`: SQL console
- `/`: Search
- `Ctrl+Z`: Undo

#### Application
- `?`: Show help
- `Q`: Quit application

## Configuration

Configuration is stored in `~/.config/open-sqlui/config.yaml` and can be customized:

```yaml
keybindings:
  navigate_up: up
  navigate_down: down
  select: enter
  back: escape
  help: "?"
  quit: q

ui:
  theme: dark
  show_row_numbers: true
  show_status_bar: true
  page_size: 100

database:
  readonly_mode: true
  auto_commit: false
  connection_timeout: 5

export:
  default_format: csv
  csv_delimiter: ","
  json_indent: 2
```

### Environment Variables

Override configuration with environment variables:

- `OPEN_SQLUI_DB_PATH`: Default database path
- `OPEN_SQLUI_READONLY`: Set read-only mode (true/false)
- `OPEN_SQLUI_THEME`: UI theme (dark/light/auto)
- `OPEN_SQLUI_PAGE_SIZE`: Number of rows per page
- `OPEN_SQLUI_EXPORT_DIR`: Default export directory

## Safety Features

1. **Read-only by default**: Databases open in read-only mode unless explicitly changed
2. **Confirmation dialogs**: All destructive operations require confirmation
3. **Transaction support**: Batch operations are wrapped in transactions
4. **Undo buffer**: Recent operations can be undone
5. **Backup recommendations**: Prompts to backup before major operations

## File Management

Open SQLui includes a built-in file explorer that:
- Automatically detects SQLite databases by extension (.db, .sqlite, .sqlite3, etc.)
- Verifies files by checking SQLite magic bytes
- Shows database metadata (size, tables, last modified)
- Supports favorites for quick access
- Remembers recently opened databases

## Exporting Data

Export data in multiple formats:

1. **CSV Export**
   - Configurable delimiter and quote character
   - Optional headers
   - Excel-compatible output

2. **JSON Export**
   - Pretty-printed with indentation
   - Array of objects format
   - UTF-8 encoding

3. **Additional Formats** (via pandas)
   - Excel (.xlsx)
   - Parquet
   - HTML

## Development

### Project Structure

```
open-sqlui/
├── src/
│   └── open_sqlui/
│       ├── core/          # Core database functionality
│       ├── services/      # Business logic services
│       ├── screens/       # TUI screens
│       ├── widgets/       # Custom TUI widgets
│       ├── utils/         # Utility functions
│       ├── app.py         # Main application
│       └── cli.py         # CLI entry point
├── tests/                 # Test suite
├── pyproject.toml         # Project configuration
└── README.md              # Documentation
```

### Running Tests

```bash
# Install development dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Run with coverage
pytest --cov=src/open_sqlui

# Run linting
ruff check src/

# Format code
black src/
```

### Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on how to contribute.

## Requirements

- Python 3.8+
- Linux/macOS/Windows terminal with 256-color support
- SQLite 3.x

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Acknowledgments

- Built with [Textual](https://github.com/Textualize/textual) - Modern TUI framework
- Uses [Rich](https://github.com/Textualize/rich) for terminal formatting
- SQLite database engine

## Support

- **Issues**: [GitHub Issues](https://github.com/alpian9890/open-sqlui/issues)
- **Discussions**: [GitHub Discussions](https://github.com/alpian9890/open-sqlui/discussions)
- **Documentation**: [Wiki](https://github.com/alpian9890/open-sqlui/wiki)

## Roadmap

- [ ] SQL query editor with syntax highlighting
- [ ] Visual query builder
- [ ] Schema migration tools
- [ ] Database comparison and diff
- [ ] Import from various formats
- [ ] Plugin system
- [ ] Remote database support
- [ ] Database backup and restore
- [ ] Performance profiling tools

---

**Open SQLui** - Making SQLite database management accessible and safe from the terminal.
