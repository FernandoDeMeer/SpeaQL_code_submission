# Database Configuration Guide for SCA Streamlit App

This document explains how to configure database connections for the SCA Streamlit App through the `config.yaml` file.

## Overview

The SCA Streamlit App supports flexible database configuration through the `database` section in `config.yaml`. You can choose from three different database configuration types to suit your needs.

## Database Configuration Types

### 1. Default Database (`type: "default"`)

Uses the built-in Netflix SQLite database that comes with the application.

```yaml
database:
  type: "default"
  default:
    path: "netflixdb.sqlite"  # Relative to streamlit folder
    engine_options:
      check_same_thread: false
```

**Configuration Options:**
- `path`: Path to the SQLite file relative to the streamlit directory
- `engine_options`: SQLAlchemy engine options
  - `check_same_thread`: Set to false to allow multi-threaded access (recommended for Streamlit)

**Use Cases:**
- Quick setup and testing
- Using the provided Netflix sample database
- Development and demos

### 2. Custom Database (`type: "custom"`)

Allows you to specify a custom database URL with full control over engine options.

```yaml
database:
  type: "custom"
  custom:
    url: "sqlite+pysqlite:///path/to/your/database.sqlite"
    engine_options:
      echo: false
      execution_options:
        sqlite_raw_colnames: true
      connect_args:
        check_same_thread: false
        timeout: 20
        pragmas:
          foreign_keys: "ON"
          journal_mode: "WAL"
          synchronous: "NORMAL"
```

**Configuration Options:**
- `url`: Full SQLAlchemy database URL
- `engine_options`: Complete SQLAlchemy engine configuration
  - `echo`: Set to true to log all SQL statements (useful for debugging)
  - `execution_options`: Engine-level execution options
  - `connect_args`: Database-specific connection arguments

**Use Cases:**
- Production deployments
- Custom SQLite databases with specific requirements
- Advanced SQLite configurations with pragmas
- Using databases in different locations

### 3. Environment-Based Database (`type: "from_env"`)

Reads the database URL from an environment variable, with fallback support.

```yaml
database:
  type: "from_env"
  from_env:
    url_env_var: "DATABASE_URL"
    default_url: "sqlite+pysqlite:///netflixdb.sqlite"
    engine_options:
      echo: false
```

**Configuration Options:**
- `url_env_var`: Name of the environment variable containing the database URL
- `default_url`: Fallback URL if the environment variable is not found
- `engine_options`: SQLAlchemy engine options

**Use Cases:**
- Deployment environments
- CI/CD pipelines
- Multi-environment deployments (dev/staging/prod)
- Security-conscious deployments (keeping credentials out of config files)

## SQLite-Specific Configuration

Since the application is designed for SQLite databases, here are SQLite-specific configuration options:

### Basic SQLite URL Formats

```yaml
# Absolute path
url: "sqlite+pysqlite:///absolute/path/to/database.sqlite"

# Relative path (from current working directory)
url: "sqlite+pysqlite:///relative/path/database.sqlite"

```

### SQLite Engine Options

```yaml
engine_options:
  echo: false                    # Log SQL statements
  execution_options:
    sqlite_raw_colnames: true    # Preserve column names exactly as in SQLite
  connect_args:
    check_same_thread: false     # Allow multi-threaded access
    timeout: 20                  # Connection timeout in seconds
    pragmas:                     # SQLite PRAGMA statements
      foreign_keys: "ON"         # Enable foreign key constraints
      journal_mode: "WAL"        # Write-Ahead Logging mode
      synchronous: "NORMAL"      # Synchronization level
```

## Security Notes

- **Environment Variables**: Use `from_env` type for production to keep database paths out of version control
- **File Permissions**: Ensure SQLite database files have appropriate read/write permissions
- **Path Security**: Use absolute paths in production to avoid path traversal issues

## Testing Configuration

To test your database configuration without running the full app:

```python
from streamlit.sca_streamlit_app import SCAStreamlitApp

# Test with specific config file
app = SCAStreamlitApp.create_with_config("path/to/your/config.yaml")
print(f"Connected successfully. Tables: {app.table_names}")
```

## File Locations

- Main config: `streamlit/config.yaml`
- Example configs: `streamlit/db_config_example_*.yaml`
- This documentation: `streamlit/db_CONFIG_README.md`
