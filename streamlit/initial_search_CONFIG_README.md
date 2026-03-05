# Configuration Guide for SCA Streamlit App Initial Search

This document explains how to configure the SCA Streamlit App's initial search page behavior.

## Configuration File

The app reads its configuration from `config.yaml` in the streamlit directory. If the file doesn't exist, the app will use default settings (full search mode).

## Configuration Options

### Search Modes

The app supports three search modes:

1. **Full Mode** (`search_mode: "full"`): 
   - Default behavior
   - Shows all tables and columns for selection
   - Users can search any table/column combination

2. **Restricted Single Table Mode** (`search_mode: "restricted_single_table"`):
   - Pre-defines a single table and optionally the columns to search
   - Users only enter search values
   - Useful for specific databases where you want to limit user access to one table

3. **Restricted Multi Table Mode** (`search_mode: "restricted_multi_table"`):
   - Pre-defines multiple tables, each with predefined columns to search
   - Users choose from table aliases and enter search values
   - Useful for allowing users to select records from different databases 

### Configuration Structure

```yaml
# Search mode: "full", "restricted_single_table", or "restricted_multi_table"
search_mode: "full"

# Settings for restricted single table mode
restricted_search:
  table: "table_name"           # Required: table to search in
  columns:                      # Optional: specific columns to search
    - "column1"
    - "column2"
  title: "Custom Page Title"    # Optional: custom page title
  instructions: "Custom instructions for users"  # Optional: custom instructions

# Settings for restricted multi table mode
restricted_search:
  tables:                       # Required: dictionary of table_name -> columns
    "actual_table_name_1":
      - "column1"
      - "column2"
    "actual_table_name_2":
      - "column3"
      - "column4"
  table_aliases:               # Required: dictionary of display_name -> actual_table_name
    "companies Database": "actual_table_name_1"
    "HNW Individuals": "actual_table_name_2"
  title: "Custom Page Title"    # Optional: custom page title
  instructions: "Custom instructions for users"  # Optional: custom instructions

# Settings for full mode
full_search:
  title: "Custom Page Title"    # Optional: custom page title
  instructions: "Custom instructions for users"  # Optional: custom instructions
```

## Examples

### Example 1: Full Mode (Default)

```yaml
search_mode: "full"

full_search:
  title: "Database Explorer"
  instructions: "🔍 Select a table and column to begin exploring the database."
```

### Example 2: Restricted Single Table Mode - Netflix Database

```yaml
search_mode: "restricted_single_table"

restricted_search:
  table: "netflix_titles"
  columns: 
    - "title"
    - "description"
  title: "Netflix Content Search"
  instructions: "🎬 Enter keywords to search Netflix titles and descriptions."
```

### Example 3: Restricted Single Table Mode - Single Column

```yaml
search_mode: "restricted_single_table"

restricted_search:
  table: "users"
  columns: 
    - "username"
  title: "User Lookup"
  instructions: "👤 Enter a username to search for user records."
```

### Example 4: Restricted Single Table Mode - All Columns

```yaml
search_mode: "restricted_single_table"

restricted_search:
  table: "products"
  # columns not specified = all columns available
  title: "Product Search"
  instructions: "🛍️ Search through all product information."
```

### Example 5: Restricted Multi Table Mode

```yaml
search_mode: "restricted_multi_table"

restricted_search:
  tables:
    "companies":
      - "company_name"
      - "description"
      - "industry"
    "individuals":
      - "name"
      - "biography"
      - "occupation"
  table_aliases:
    "Companies": "companies"
    "HNW Individuals": "individuals"
  title: "Financial Database Search"
  instructions: "💼 Select a database and enter search terms to find companies or individuals."
```

## File Locations

- Main config: `streamlit/config.yaml`
- Example configs: `streamlit/initial_search_config_example_*.yaml`

## Validation

The app will validate the configuration and show error messages if:
- The specified table doesn't exist in the database
- The specified columns don't exist in the table
- Required fields are missing in restricted modes
- For multi-table mode: tables dictionary or table_aliases dictionary is missing
- For multi-table mode: table aliases reference non-existent tables

## Default Behavior

If no configuration file is found, the app defaults to:
- Full search mode
- Standard title and instructions
- Access to all tables and columns in the database
