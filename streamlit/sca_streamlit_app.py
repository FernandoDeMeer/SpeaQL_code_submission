import os
import re
import streamlit as st

st.set_page_config(layout="wide")
import pandas as pd
import networkx as nx
from dotenv import load_dotenv
from typing import Dict, Any, Tuple
from pyvis.network import Network
from sqlalchemy import MetaData, Table, select
from sqlalchemy.engine import Engine
from pathlib import Path
from sca_gui_db_utils import SCAGuiDBUtils
from dbkgexp.rdb_explorer import RDBExplorer
from sqlalchemy import event
import sqlite3
import yaml
import math
import random

from streamlit_llm_handler import StreamlitLLMHandler
from dbkgexp.rel_node import RelationalNode
from session_logger import SessionLogger
from dbkgexp.rdb_explorer import stable_row_id


class SCAStreamlitApp:
    """
    A class to handle the Streamlit application for the SCA project.
    This class initializes the Streamlit app and provides methods to interact with the database.
    
    The database configuration is read from config.yaml file, allowing flexible database setup.
    
    Usage Examples:
    
    1. Default configuration from config.yaml:
       app = SCAStreamlitApp()
       
    2. Specific config file:
       app = SCAStreamlitApp.create_with_config("path/to/config.yaml")
    
    3. Custom engine with default config:
       engine = create_engine("sqlite:///my_db.sqlite")
       app = SCAStreamlitApp(engine)
       
    Database Configuration (in config.yaml):
    - type: "default" (Netflix SQLite), "custom" (custom URL), "from_env" (environment variable)
    - Each type has specific configuration options for engine creation

    Initial Search Page Configuration (in config.yaml):
    - search_mode: "full", "restricted_single_table", or "restricted_multi_table"
    - Each mode has specific configuration options for the initial search page
    """

    @staticmethod
    def _load_config() -> Dict[str, Any]:
        """Load configuration from config.yaml file."""
        config_path = Path(__file__).parent / "config.yaml"
        try:
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
                return config
        except FileNotFoundError:
            # Return default configuration if file not found
            return {
                "database": {
                    "type": "default",
                    "default": {
                        "path": "netflixdb.sqlite",
                        "engine_options": {"check_same_thread": False}
                    }
                },
                "search_mode": "full",
                "full_search": {
                    "title": "SCA - DB Exploration App",
                    "instructions": "🔍 **How to start the DB Exploration:** Please select a table and a column to search for a specific value."
                }
            }

    @classmethod
    def _create_engine_from_config(cls, config: Dict[str, Any]) -> Engine:
        """Create SQLAlchemy engine based on configuration."""
        db_config = config.get("database", {})
        db_type = db_config.get("type", "default")
        
        if db_type == "default":
            # Default Netflix SQLite database
            default_config = db_config.get("default", {})
            db_path = default_config.get("path", "netflixdb.sqlite")
            sqlite_path = Path(__file__).parent / db_path
            
            engine_options = default_config.get("engine_options", {})
            check_same_thread = engine_options.get("check_same_thread", False)
            
            db_url = f"sqlite+pysqlite:///{sqlite_path}"
            if not check_same_thread:
                db_url += "?check_same_thread=False"
            
            return SCAGuiDBUtils.get_engine(db_url=db_url)
        
        elif db_type == "custom":
            # Custom database configuration
            custom_config = db_config.get("custom", {})
            db_url = custom_config.get("url")
            if not db_url:
                raise ValueError("Custom database configuration requires 'url' field")
            
            engine_options = custom_config.get("engine_options", {})
            return SCAGuiDBUtils.get_engine(db_url=db_url, engine_options=engine_options)
        
        elif db_type == "from_env":
            # Environment-based configuration
            env_config = db_config.get("from_env", {})
            url_env_var = env_config.get("url_env_var", "DATABASE_URL")
            default_url = env_config.get("default_url", "sqlite+pysqlite:///netflixdb.sqlite")
            
            db_url = os.getenv(url_env_var, default_url)
            engine_options = env_config.get("engine_options", {})
            
            return SCAGuiDBUtils.get_engine(db_url=db_url, engine_options=engine_options)
        
        else:
            raise ValueError(f"Unknown database type: {db_type}")

    @staticmethod
    def _create_default_engine():
        """Create the default SQLite engine for the Netflix database."""
        sqlite_path = Path(__file__).parent / "netflixdb.sqlite"
        return SCAGuiDBUtils.get_engine(
            db_url=f"sqlite+pysqlite:///{sqlite_path}?check_same_thread=False"
        )

    @classmethod
    def create_with_config(cls, config_path: str = None):
        """
        Factory method to create SCAStreamlitApp using configuration file.
        :param config_path: Path to config file. If None, uses default config.yaml
        :return: SCAStreamlitApp instance configured from file
        """
        if config_path:
            with open(config_path, 'r') as file:
                config = yaml.safe_load(file)
        else:
            config = cls._load_config()
        
        engine = cls._create_engine_from_config(config)
        return cls(engine, config)

    @classmethod
    def create_with_default_db(cls):
        """
        Factory method to create SCAStreamlitApp with the default Netflix database.
        :return: SCAStreamlitApp instance with default configuration
        """
        config = cls._load_config()
        return cls(None, config)

    @classmethod 
    def create_with_custom_db(cls, db_url: str, **engine_kwargs):
        """
        Factory method to create SCAStreamlitApp with a custom database.
        :param db_url: Database URL for SQLAlchemy
        :param engine_kwargs: Additional arguments for create_engine
        :return: SCAStreamlitApp instance with custom database
        """
        engine = SCAGuiDBUtils.get_engine(db_url=db_url, engine_options=engine_kwargs)
        config = cls._load_config()
        return cls(engine, config)

    def __init__(self, engine: Engine = None, config: Dict[str, Any] = None):
        """
        Initialize the SCAStreamlitApp with a SQLAlchemy engine and configuration.
        :param engine: SQLAlchemy engine connected to the database. If None, creates engine from config.
        :param config: Configuration dictionary. If None, loads from config.yaml.
        """
        # Load configuration first
        self.config = config if config is not None else self._load_config()
        
        # Create engine based on config or use provided engine
        if engine is not None:
            self.engine = engine
        else:
            self.engine = self._create_engine_from_config(self.config)
                
        with self.engine.connect() as conn:
            pass  # Ensure the connection is successful
        self.metadata = MetaData()
        self.metadata.reflect(bind=self.engine, resolve_fks=True, extend_existing=True)
        self.table_names = list(self.metadata.tables.keys())

        # Initialize LLM handler
        self.st_llm_handler = StreamlitLLMHandler(
            api_key=os.getenv("OPENAI_API_KEY"), model="openai/gpt-4o-mini"
        )
    
    def _create_node_key(self, table_name: str, primary_key: Any) -> Tuple[str, str]:
        """
        Helper method to create consistent node keys as tuples of two strings.
        Ensures that primary_key is always converted to a string for consistency.
        """
        return (str(table_name), str(primary_key))

    def get_node_attributes_from_config(self, database_key: str = None) -> Dict[str, str]:
        """
        Get node attributes configuration from config.yaml.
        
        Args:
            database_key: Specific database configuration to use. If None, uses db_name from database config.
            
        Returns:
            Dictionary mapping table names to attribute names for node display.
        """
        db_labels = self.config.get('DB_LABELS', {})
        
        if not db_labels:
            return {}
        
        if database_key:
            return db_labels.get(database_key, {})
        
        # Try to get db_name from database configuration
        db_config = self.config.get('database', {})
        db_name = db_config.get('db_name', '')
        
        if db_name and db_name in db_labels:
            return db_labels[db_name]
        
        # Fallback: if only one config, use it
        if len(db_labels) == 1:
            return list(db_labels.values())[0]
        
        # Multiple configs available but no db_name match, return empty dict
        return {}

    def run(self):
        """Main controller for the Streamlit app."""

        # Initialize the app state
        if "page" not in st.session_state:
            st.session_state.page = "search"  # default entry page
        if "current_record" not in st.session_state:
            st.session_state.current_record = None
        if "chat_history" not in st.session_state:
            st.session_state.chat_history = []
        if "persistent_nodes" not in st.session_state:
            st.session_state.persistent_nodes = {}
        if "logger" not in st.session_state:
            st.session_state.logger = SessionLogger()

        self.logger = st.session_state.logger

        # Render the initial search page
        if st.session_state.page == "search":
            self.initial_search_page()
        elif (
            st.session_state.page == "explore" and "current_record" in st.session_state
        ):
            self.explore_page()
        else:
            st.error("Invalid state. Please start by selecting a record.")

    def initial_search_page(self):
        """
        Start the Streamlit page and set up the UI for selecting records.
        Behavior depends on the configuration mode (full or restricted).
        """
        search_mode = self.config.get("search_mode", "full")

        if search_mode == "restricted_single_table":
            self._restricted_single_table_search_page()
        elif search_mode == "restricted_multi_table":
            self._restricted_multi_table_search_page()
        else:
            self._full_search_page()

    def _full_search_page(self):
        """
        Full search page - allows selection of any table and column.
        """
        config = self.config.get("full_search", {})
        title = config.get("title", "SCA - DB Exploration App")
        instructions = config.get("instructions", 
                                "🔍 **How to start the DB Exploration:** Please select a table and a column to search for a specific value.")
        
        st.title(title)
        st.info(instructions)

        # Dropdown for table selection
        st.session_state.selected_table = st.selectbox(
            "Select a table:", self.table_names
        )

        if st.session_state.selected_table:
            # Fetch columns for the selected table
            table = Table(
                st.session_state.selected_table,
                self.metadata,
                autoload_with=self.engine,
            )
            columns = [col.name for col in table.columns]

            # Dropdown for column selection
            selected_column = st.selectbox("Select a column:", columns)

            # Input for value to search
            search_value = st.text_input("Enter the value to search:")

            if st.button("Search"):
                if selected_column and search_value:
                    self._perform_search(st.session_state.selected_table, selected_column, search_value)
                else:
                    st.error("Please select a column and enter a value to search.")

        self._display_search_results()

    def _restricted_single_table_search_page(self):
        """
        Restricted single table search page - uses a pre-configured single table and columns.
        """
        config = self.config.get("restricted_search", {})
        title = config.get("title", "SCA - DB Exploration App")
        instructions = config.get("instructions", 
                                "🔍 **Search Database:** Enter keywords to search through the database.")
        
        # Get the pre-configured table and columns
        predefined_table = config.get("table")
        predefined_columns = config.get("columns", [])
        
        if not predefined_table:
            st.error("Restricted mode requires a predefined table in the configuration.")
            return
        
        if predefined_table not in self.table_names:
            st.error(f"Configured table '{predefined_table}' not found in database.")
            return
        
        st.title(title)
        st.info(instructions)
        
        # Set the selected table (hidden from user)
        st.session_state.selected_table = predefined_table
        
        # Get columns for the predefined table
        table = Table(
            predefined_table,
            self.metadata,
            autoload_with=self.engine,
        )
        all_columns = [col.name for col in table.columns]
        
        # Use predefined columns if specified, otherwise use all columns
        available_columns = predefined_columns if predefined_columns else all_columns
        
        # Validate that predefined columns exist in the table
        invalid_columns = [col for col in available_columns if col not in all_columns]
        if invalid_columns:
            st.error(f"Configured columns not found in table '{predefined_table}': {invalid_columns}")
            return
                
        # Column selection (if multiple columns are available)
        if len(available_columns) > 1:
            selected_column = st.selectbox("Select a column to search:", available_columns)
        else:
            selected_column = available_columns[0]

        # Input for value to search
        search_value = st.text_input("Enter the value to search:")

        if st.button("Search"):
            if search_value:
                self.logger.log(key="search_value", value=search_value)
                self._perform_search(predefined_table, selected_column, search_value)
            else:
                st.error("Please enter a value to search.")

        self._display_search_results()

    def _restricted_multi_table_search_page(self):
        """
        Restricted multi-table search page. Asks the user to select a table. Each table has predefined columns.
        """
        config = self.config.get("restricted_search", {})
        title = config.get("title", "SCA - DB Exploration App")
        instructions = config.get("instructions", 
                                "🔍 **Search Database:** Enter keywords to search through the database.")
        
        # Get the pre-configured tables and columns
        predefined_tables = config.get("tables", {})
        
        if not predefined_tables:
            st.error("Restricted mode requires predefined tables in the configuration.")
            return
        
        st.title(title)
        st.info(instructions)

        # Show table aliases instead of actual names

        table_aliases = config.get("table_aliases", {})
        
        # Dropdown for table selection
        st.session_state.selected_table = st.selectbox(
            "Select a table:", list(table_aliases.keys())
        )
        st.session_state.selected_table = table_aliases.get(st.session_state.selected_table)

        if st.session_state.selected_table:
            # Get predefined columns for the selected table
            available_columns = predefined_tables.get(st.session_state.selected_table, [])
            
            if not available_columns:
                st.error(f"No columns configured for table '{st.session_state.selected_table}'.")
                return
            
            # Column selection (if multiple columns are available)
            if len(available_columns) > 1:
                selected_column = st.selectbox("Select a column to search:", available_columns)
            else:
                selected_column = available_columns[0]
                st.write(f"**Search column:** {selected_column}")

            # Input for value to search
            search_value = st.text_input("Enter the value to search:")

            if st.button("Search"):
                if selected_column and search_value:
                    self._perform_search(st.session_state.selected_table, selected_column, search_value)
                else:
                    st.error("Please select a column and enter a value to search.")

        self._display_search_results()

    def _perform_search(self, table_name: str, column_name: str, search_value: str):
        """
        Perform the actual search operation.
        """
        # Always wrap input with wildcards for "contains" search
        pattern = f"%{search_value.strip()}%"
        results = SCAGuiDBUtils.query_table_by_column(
            self.engine,
            table_name,
            column_name,
            pattern,
            limit=500,
            case_insensitive=True,
            allow_wildcard=True,
        )
        query_results_df = pd.DataFrame(results)
        if not query_results_df.empty:
            st.session_state.query_results_df = query_results_df
        else:
            st.warning("No results found. Tip: try a broader term.")

    def _display_search_results(self):
        """
        Display search results and handle row selection.
        """
        if "query_results_df" in st.session_state:
            st.write("Select a row to start the DB exploration:")
            event = st.dataframe(
                st.session_state.query_results_df,
                use_container_width=True,
                hide_index=True,
                selection_mode="single-row",
                on_select="rerun",
                key="results_table",
            )

            if event and event.selection.rows:
                selected_row = st.session_state.query_results_df.iloc[
                    event.selection.rows[0]
                ]
                st.session_state.current_record = selected_row.to_dict()
                # Make all attribute values strings
                st.session_state.current_record = {
                    k: str(v) for k, v in st.session_state.current_record.items()
                }
                st.write("Selected record:")
                # Display the selected record as a table
                st.table(st.session_state.current_record)
                self.logger.log(key="selected_record", value=st.session_state.current_record)
                st.session_state.page = "explore"
                st.info(
                    "🔍 **Tip**: If you want to select a different initial record, please refresh the page."
                )
                self.explore_page()

    def explore_page_node_selection(self, node_attributes):
        """
        Handle node selection interface including dropdown and manual entry.
        Returns the selected node ID or empty string if none selected.
        """
        # Node selection options - both dropdown and manual entry
        node_count = len(st.session_state.explored_nodes) if hasattr(st.session_state, 'explored_nodes') and st.session_state.explored_nodes else 0
        st.write(f"**Node Inspection Options** ({node_count} nodes available):")
        
        # Create two columns for node selection methods
        col_dropdown, col_manual = st.columns([2, 1])
        
        with col_dropdown:
            # Dropdown for node selection from explored nodes
            node_options = ["Select a node..."]
            if hasattr(st.session_state, 'explored_nodes') and st.session_state.explored_nodes:
                # Create readable node options with table name and display attribute
                unique_options = set()  # Use set to ensure uniqueness
                for (table_name, primary_key), node in st.session_state.explored_nodes.items():
                    node_id = f"{table_name}:{primary_key}"
                    
                    # Try to get a readable label from node attributes
                    display_label = str(primary_key)
                    if node_attributes and table_name in node_attributes:
                        attr_config = node_attributes[table_name]
                        
                        # Handle both single attribute (string) and multiple attributes (list)
                        if isinstance(attr_config, list):
                            # Multiple attributes - combine them
                            attr_values = []
                            for attr_name in attr_config:
                                if node.data and attr_name in node.data and node.data[attr_name]:
                                    attr_values.append(str(node.data[attr_name]))
                            if attr_values:
                                display_label = " ".join(attr_values)
                        else:
                            # Single attribute (string)
                            attr_name = attr_config
                            if node.data and attr_name in node.data and node.data[attr_name]:
                                display_label = str(node.data[attr_name])
                    
                    # Create option with format: "table_name: display_label (node_id)"
                    option_label = f"{table_name}: {display_label} ({node_id})"
                    unique_options.add(option_label)
                
                # Convert set to sorted list and add to node_options
                node_options.extend(sorted(unique_options))
            
            selected_node_option = st.selectbox(
                "Select a node from explored nodes:",
                options=node_options,
                help="Choose from the list of explored nodes to view their details"
            )
            
            # Extract node_id from selected option
            if selected_node_option != "Select a node...":
                # Extract node_id from the format "table_name: display_label (node_id)"
                match = re.search(r'\(([^)]+)\)$', selected_node_option)
                if match:
                    enter_node_id = match.group(1)
                else:
                    enter_node_id = ""
            else:
                enter_node_id = ""
        
        with col_manual:
            # Manual entry option
            manual_node_id = st.text_input(
                "Or enter manually:",
                key="manual_node_input",
                placeholder="table_name:primary_key",
                help="Manually enter a node ID if not in the dropdown"
            )
            
            # Use manual input if provided, otherwise use dropdown selection
            if manual_node_id.strip():
                enter_node_id = manual_node_id.strip()
        
        # Display current selection
        if enter_node_id:
            st.success(f"🎯 **Selected Node:** `{enter_node_id}`")
        
        return enter_node_id

    def explore_page_exploration_header(self):
        """
        Handle the exploration header section including subheader and info box.
        Returns the selected graph type.
        """
        st.subheader("🗂️ DB Graph Exploration")
        
        # Graph visualization info
        st.info(
            """
            💡 **How to inspect nodes:**
            
            Use the dropdown to **select from explored nodes** or manually **enter a node ID** (format: `table_name:primary_key`) to view **details in the sidebar**.
            Once selected, you can choose to **create a new graph** based on the selected node to explore further **relationships** in the database.
            Use the graph visualization options below to customize the graph display: 

            - **Standard Graph**: Displays the relationships between nodes as they are in the database.
            - **Grouped by Table**: Organizes nodes by their respective tables for easier graph navigation.
            """
        )

        # Initialize graph mode if not exists
        if "graph_mode" not in st.session_state:
            st.session_state.graph_mode = "Standard Graph"
        
        # Graph type selection
        graph_type = st.selectbox(
            "Select graph visualization type:",
            ["Standard Graph", "Grouped by Table"],
            index=0 if st.session_state.graph_mode == "Standard Graph" else 1,
            help="Standard Graph: Shows all nodes in a single graph. Grouped by Table: Groups nodes by their table for better organization.",
            key="graph_type_selector"
        )
        
        # Update session state when selection changes
        if graph_type != st.session_state.graph_mode:
            st.session_state.graph_mode = graph_type
            st.rerun()
        
        return graph_type


    def explore_page_rdb_explore(self):
        if st.session_state.current_record:
            rdb_explorer = RDBExplorer(
                self.engine, st.session_state.selected_table
            )  # Pass the selected table name
            try:
                primary_key = rdb_explorer._get_primary_key(rdb_explorer.table)
                rdb_explorer.explore(
                    table_name=st.session_state.selected_table,
                    primary_key=st.session_state.current_record[primary_key[0]],
                    depth=2,
                )
            except:
                # The current record does not have a primary key so we need to create a node first
                # Create a unique identifier for the row since there's no primary key
                row_hash = stable_row_id(st.session_state.current_record)
                row_identifier = f"no_pk_{row_hash}"
                
                # Create the node manually and add it to explored_nodes
                table = rdb_explorer._get_table(st.session_state.selected_table)
                node = RelationalNode(
                    tableName=st.session_state.selected_table,
                    primaryKey=str(row_identifier),
                    data=st.session_state.current_record,
                    foreignKeys=table.foreign_keys if table.foreign_keys else None,
                )
                node_key = self._create_node_key(st.session_state.selected_table, row_identifier)
                rdb_explorer.explored_nodes[node_key] = node
                
                # Now explore from this existing node
                rdb_explorer.explore_from_existing_node(
                    table_name=st.session_state.selected_table,
                    node_identifier=row_identifier, 
                    depth=2
                )
                
            st.session_state.explored_nodes = rdb_explorer.explored_nodes

            return rdb_explorer
    
    def load_persistent_nodes(self, rdb_explorer):
        """
        Add persistent nodes to explored_nodes if they're not already present.
        """
        if not hasattr(st.session_state, 'persistent_nodes') or not st.session_state.persistent_nodes:
            return
        
        for node_id, relational_node in st.session_state.persistent_nodes.items():
            try:
                # Parse node_id format: "table_name:primary_key"
                parts = node_id.split(":", 1)
                if len(parts) != 2:
                    continue
                
                table_name, primary_key = parts
                node_key = self._create_node_key(table_name, primary_key)
                
                # Skip if this node is already in explored_nodes
                if node_key in st.session_state.explored_nodes:
                    continue
                
                # Add the persistent node to the current explored_nodes
                st.session_state.explored_nodes[node_key] = relational_node
                    
            except Exception as e:
                # If there's any error processing this node, skip it
                continue
    def explore_page_graph_visualization(self, rdb_explorer, graph_type, node_attributes, enter_node_id):
        """
        Handle the graph visualization including building the graph and rendering it with Pyvis.
        """
        if not st.session_state.current_record:
            st.warning("Please select a record to explore the database.")
            return
        
        # Load persistent nodes and merge them with current explored_nodes
        self.load_persistent_nodes(rdb_explorer)
        
        # Build graph based on selected type using the unified method
        if graph_type == "Grouped by Table":
            st.session_state.kg_graph = rdb_explorer.build_graph_with_options(
                explored_nodes=rdb_explorer.explored_nodes,
                relevant_nodes= st.session_state.relevant_nodes if hasattr(st.session_state, 'relevant_nodes') else None,
                persistent_nodes=st.session_state.persistent_nodes if hasattr(st.session_state, 'persistent_nodes') else None,
                group_by_table=True,
                node_attributes=node_attributes if node_attributes else None
            )
        else:
            st.session_state.kg_graph = rdb_explorer.build_graph_with_options(
                explored_nodes=rdb_explorer.explored_nodes,
                relevant_nodes= st.session_state.relevant_nodes if hasattr(st.session_state, 'relevant_nodes') else None,
                persistent_nodes=st.session_state.persistent_nodes if hasattr(st.session_state, 'persistent_nodes') else None,
                group_by_table=False,
                node_attributes=node_attributes if node_attributes else None
            )
        
        # Handle node selection in sidebar
        if enter_node_id:
            st.sidebar.subheader(f"Node: {enter_node_id}")
            enter_node_id = (enter_node_id.split(":")[0], enter_node_id.split(":")[1])
            if enter_node_id in st.session_state.explored_nodes.keys():
                # Display node data as a 2-column table instead of JSON
                node_data = st.session_state.explored_nodes[enter_node_id].data
                
                # Create a DataFrame for the table display
                if node_data:
                    df_data = []
                    for attribute, value in node_data.items():
                        df_data.append({"Attribute": attribute, "Value": str(value)})
                    
                    node_df = pd.DataFrame(df_data)
                    st.sidebar.dataframe(
                        node_df,
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            "Attribute": st.column_config.TextColumn("Attribute", width="small"),
                            "Value": st.column_config.TextColumn("Value", width="medium")
                        }
                    )
                else:
                    st.sidebar.write("No data available for this node.")
                
                # Add a button to keep this node in the graph
                if st.sidebar.button("Keep this node in the graph"):
                    # Parse the node ID to get table and primary key
                    table_name = re.match(r"(.+):(.+)", enter_node_id).group(1)
                    primary_key = re.match(r"(.+):(.+)", enter_node_id).group(2)
                    node_key = self._create_node_key(table_name, primary_key)
                    
                    # Check if this node exists in explored_nodes
                    if node_key in st.session_state.explored_nodes.keys():
                        # Store the actual RelationalNode object in persistent_nodes
                        st.session_state.persistent_nodes[enter_node_id] = st.session_state.explored_nodes[node_key]
                        st.sidebar.success(f"Node {enter_node_id} will be kept in future graphs!")
                    else:
                        st.sidebar.warning(f"Node {enter_node_id} not found in current exploration.")
                
                # Add a button to create a new graph based on the selected node
                if st.sidebar.button("Create new graph from this node"):
                    # Change the selected table based on the node ID
                    st.session_state.selected_table = enter_node_id[0]
                    # Change the current record to the selected node data
                    st.session_state.current_record = (
                        st.session_state.explored_nodes[enter_node_id].data
                    )
                    # Trigger a rerun to rebuild the graph with the new record
                    st.rerun()
            else:
                st.sidebar.warning("Node not found in graph.")

        # Create a Pyvis network graph with physics options for better node separation
        net = Network(
            notebook=True, 
            height="700px", 
            width="100%", 
            cdn_resources="remote"
        )
        
        # Configure physics to improve node separation and reduce bouncing
        net.set_options("""
        var options = {
          "physics": {
            "enabled": true,
            "stabilization": {
              "enabled": true, 
              "iterations": 200,
              "updateInterval": 25,
              "onlyDynamicEdges": false,
              "fit": true
            },
            "barnesHut": {
              "gravitationalConstant": -8000,
              "centralGravity": 0.3,
              "springLength": 95,
              "springConstant": 0.04,
              "damping": 0.15,
              "avoidOverlap": 1
            },
            "maxVelocity": 20,
            "minVelocity": 0.1,
            "solver": "barnesHut",
            "timestep": 0.35
          },
          "interaction": {
            "dragNodes": true,
            "dragView": true,
            "zoomView": true
          }
        }
        """)

        # Get current validated and non-matching nodes for highlighting
        validated_node_ids = set()
        non_matching_node_ids = set()

        if (
            hasattr(st.session_state, "current_relevant_nodes")
            and st.session_state.current_relevant_nodes
        ):
            validated_node_ids = {
                node.node_id for node in st.session_state.current_relevant_nodes
            }

        if (
            hasattr(st.session_state, "current_non_matching_nodes")
            and st.session_state.current_non_matching_nodes
        ):
            non_matching_node_ids = {
                node.node_id
                for node in st.session_state.current_non_matching_nodes
            }

        # Get color map for styling
        color_map = SCAGuiDBUtils.get_color_map(self.engine)

        # Pre-position normal nodes (table_name:primary_key) in a cluster for faster equilibrium
        # Collect normal nodes (not table_group nodes)
        normal_nodes = []
        table_group_nodes = []
        
        for node in st.session_state.kg_graph.nodes:
            if node.startswith("table_group:"):
                table_group_nodes.append(node)
            else:
                normal_nodes.append(node)
        
        # Create position mapping for all nodes
        all_node_positions = {}
        cluster_radius = 100  # Compact cluster radius
        center_x, center_y = 0, 0  # Center of the cluster
        
        # Group normal nodes by table type
        nodes_by_table = {}
        for node in normal_nodes:
            table_name = node.split(":")[0]
            if table_name not in nodes_by_table:
                nodes_by_table[table_name] = []
            nodes_by_table[table_name].append(node)
        
        # Position normal nodes by table in different regions of the circular pattern
        table_names = list(nodes_by_table.keys())
        
        for table_idx, table_name in enumerate(table_names):
            table_nodes = nodes_by_table[table_name]
            
            if len(table_names) == 1:
                # Single table - use center region
                table_center_angle = 0
            else:
                # Multiple tables - distribute around circle
                table_center_angle = 2 * math.pi * table_idx / len(table_names)
            
            # Calculate center position for this table's region
            table_region_radius = cluster_radius * 0.6  # Distance from center for table regions
            table_center_x = center_x + table_region_radius * math.cos(table_center_angle)
            table_center_y = center_y + table_region_radius * math.sin(table_center_angle)
            
            # Position nodes within this table's region
            table_cluster_radius = min(50, cluster_radius * 0.4)  # Radius for nodes within table region
            
            for node_idx, node in enumerate(table_nodes):
                if len(table_nodes) == 1:
                    # Single node at table region center
                    x, y = table_center_x, table_center_y
                else:
                    # Multiple nodes in circular pattern within table region
                    node_angle = 2 * math.pi * node_idx / len(table_nodes)
                    # Add some randomness to avoid perfect overlap
                    radius = table_cluster_radius * (0.3 + 0.7 * random.random())
                    x = table_center_x + radius * math.cos(node_angle)
                    y = table_center_y + radius * math.sin(node_angle)
                
                all_node_positions[node] = (x, y)
        
        # Position table_group nodes near their child tables
        for table_group_node in table_group_nodes:
            # Parse table_group node ID: "table_group:related_table:related_pk:child_table"
            parts = table_group_node.split(":")
            if len(parts) >= 4:
                child_table = parts[3]
                
                # Find normal nodes that belong to this child table
                child_table_nodes = [node for node in normal_nodes if node.startswith(f"{child_table}:")]
                
                if child_table_nodes:
                    # Position table_group node near the center of its child table nodes
                    child_positions = [all_node_positions[child_node] for child_node in child_table_nodes]
                    avg_x = sum(pos[0] for pos in child_positions) / len(child_positions)
                    avg_y = sum(pos[1] for pos in child_positions) / len(child_positions)
                    
                    # Offset slightly to avoid overlap
                    offset_distance = 30
                    offset_angle = random.random() * 2 * math.pi
                    table_group_x = avg_x + offset_distance * math.cos(offset_angle)
                    table_group_y = avg_y + offset_distance * math.sin(offset_angle)
                    
                    all_node_positions[table_group_node] = (table_group_x, table_group_y)
                else:
                    # Fallback: position at cluster center if no child nodes found
                    all_node_positions[table_group_node] = (center_x, center_y)

        # Add nodes to the network
        for node in st.session_state.kg_graph.nodes:
            table_name = re.match(r"(.+):(.+)", node).group(1)
            primary_key = re.match(r"(.+):(.+)", node).group(2)

            # Get node label from graph data or create one using node_attributes
            node_label = node  # Default fallback to node ID
            
            # Try to get the label from the graph node data first
            if 'label' in st.session_state.kg_graph.nodes[node]:
                node_label = st.session_state.kg_graph.nodes[node]['label']
            else:
                # If no label in graph, create one using node_attributes and explored_nodes
                if hasattr(st.session_state, 'explored_nodes') and st.session_state.explored_nodes:
                    node_key = self._create_node_key(table_name, primary_key)
                    if node_key in st.session_state.explored_nodes:
                        rel_node = st.session_state.explored_nodes[node_key]
                        
                        # Use node_attributes to determine display label
                        if node_attributes and table_name in node_attributes:
                            attr_config = node_attributes[table_name]
                            
                            # Handle both single attribute (string) and multiple attributes (list)
                            if isinstance(attr_config, list):
                                # Multiple attributes - combine them
                                attr_values = []
                                for attr_name in attr_config:
                                    if rel_node.data and attr_name in rel_node.data and rel_node.data[attr_name]:
                                        attr_values.append(str(rel_node.data[attr_name]))
                                if attr_values:
                                    node_label = " ".join(attr_values)
                                else:
                                    node_label = str(primary_key)  # Fallback to primary key
                            else:
                                # Single attribute (string)
                                attr_name = attr_config
                                if rel_node.data and attr_name in rel_node.data and rel_node.data[attr_name]:
                                    node_label = str(rel_node.data[attr_name])
                                else:
                                    node_label = str(primary_key)  # Fallback to primary key
                        else:
                            node_label = str(primary_key)  # Fallback to primary key

            # Determine node color based on validation status
            if node in validated_node_ids:
                # Green for validated nodes
                node_color = "#28a745"
                border_color = "#1e7e34"
                border_width = 3
            elif node in non_matching_node_ids:
                # Light orange for non-matching nodes
                node_color = "#ff9f40"
                border_color = "#ff6600"
                border_width = 3
            else:
                # Default color from color map
                node_color = color_map.get(table_name, "#6FB6DD")
                border_color = "#000000"
                border_width = 2

            # Prepare node positioning
            node_options = {
                "n_id": node,
                "label": node_label,  # Use the computed label instead of node ID
                "color": node_color,
                "borderWidth": border_width,
                "borderColor": border_color,
                "title": f"{table_name}:{primary_key}"  # Show node ID as tooltip
            }
            
            # Add initial positioning for normal and table_group nodes to improve equilibrium convergence
            if node in all_node_positions:
                x, y = all_node_positions[node]
                node_options["x"] = x
                node_options["y"] = y
            
            net.add_node(**node_options)
        
        # Add edges to the network
        for edge in st.session_state.kg_graph.edges:
            # Edges are tuples of (source, target)
            net.add_edge(edge[0], edge[1])

        net.show("kg_graph.html")

        # Inject click tracking for nodes with JS
        with open("kg_graph.html", "r") as f:
            html_content = f.read()

        # Center the graph in the column
        st.markdown(
            "<div style='display: flex; justify-content: center;'>",
            unsafe_allow_html=True,
        )
        st.components.v1.html(html_content, height=725, width=None)
        st.markdown("</div>", unsafe_allow_html=True)

        # Add a legend for extracted node colors
        st.info(
            """
        **Legend for extracted nodes:**
        - 🟢 **Green nodes**: Validated by LLM (match database contents)
        - 🟠 **Orange nodes**: Non-matching (require manual verification)
        - **Other colors**: Non-relevant standard nodes (by table type)
        """
        )
        # Add disclaimer for large graphs
        st.info(
            "⚠️ **Note:** For large graphs, only a subset of nodes is shown for a better visualization."
        )

    def explore_page_chat_interface(self):
        """
        Handle the chat interface for interacting with the Knowledge Assistant.
        """
        st.subheader("💬 Chat with the Knowledge Assistant")

        st.info(
            """
            🔎 ****Ask questions** to the Knowledge Assistant about information contained in the graph:**

            The assistant will **extract relevant nodes** and provide a comprehensive summary of its findings.\n 
            **Extracted nodes** will be **compared against the database** and classified as:\n
                - ✅ **Green nodes**: Attributes match the knowledge graph/database (validated)\n
                - ⚠️ **Orange nodes**: Potential mismatches or hallucinations (requires manual verification, attributes may belong to other related records or may have been hallucinated)\n

            **Important:** Always verify the information contained in the summary as it may have been hallucinated by the Knowledge Assistant.
        """
        )
        
        # Add CSS for chat message styling
        st.markdown(
            """
        <style>
        .user-message {
            background-color: #E3F2FD;
            padding: 10px;
            border-radius: 10px;
            margin: 5px 0;
        }
        .assistant-message {
            background-color: #E8F5E8;
            padding: 10px;
            border-radius: 10px;
            margin: 5px 0;
        }
        .valguardrail-message {
            background-color: #E8F5E8;
            padding: 10px;
            border: 1px solid #FFEEBA;
            border-radius: 10px;
            margin: 5px 0;
        }
        .nonvalguardrail-message {
            background-color: #FFF3CD;
            color: #856404;
            padding: 10px;
            border: 1px solid #FFEAA7;
            border-radius: 10px;
            margin: 5px 0;
        }
        </style>
        """,
            unsafe_allow_html=True,
        )

        # Use the StreamlitLLMHandler to render the chat interface
        if hasattr(st.session_state, "explored_nodes"):
            self.st_llm_handler.render_chat_interface(
                st.session_state.explored_nodes
            )
        else:
            st.info(
                "Please explore the database first to enable the chat functionality."
            )

    def explore_page(self):
        """
        Method to handle the database exploration and LLM interactions.
        We want to have 2 vertical sections:
        - Left: RDB Explorer with an interactive visualization of the selected KG
        - Right: Chat interface for submitting user queries to the LLM
        """
        # Initialize layout: 2 columns
        col1, col2 = st.columns([1, 1])

        if "messages" not in st.session_state:
            st.session_state["messages"] = []

        with col1:
            # Call the exploration header function
            graph_type = self.explore_page_exploration_header()
            
            # Get node attributes from config using db_name
            db_config = self.config.get('database', {})
            db_name = db_config.get('db_name', '')
            db_labels = self.config.get('DB_LABELS', {})
            node_attributes = {}
            
            # Use db_name to automatically select the correct DB_LABELS configuration
            if db_name and db_name in db_labels:
                node_attributes = db_labels[db_name]
            else:
                if graph_type == "Grouped by Table":
                    st.warning("⚠️ No DB_LABELS configuration found in config.yaml")
                    st.info("ℹ️ Please add a DB_LABELS section to config.yaml to specify node attributes for the grouped visualization.")
                    st.stop()            
            
            # Ensure we have explored nodes before creating the UI
            rdb_explorer = self.explore_page_rdb_explore()
            
            # Call the node selection function
            enter_node_id = self.explore_page_node_selection(node_attributes)
            
            # Call the graph visualization function
            self.explore_page_graph_visualization(rdb_explorer, graph_type, node_attributes, enter_node_id)

        with col2:
            # Call the chat interface function
            self.explore_page_chat_interface()


from pathlib import Path

if __name__ == "__main__":
    # Example 1: Use configuration from config.yaml (recommended)
    streamlit_app_flow = SCAStreamlitApp()
    
    # Example 2: Use specific config file
    # streamlit_app_flow = SCAStreamlitApp.create_with_config("path/to/config.yaml")
    
    # Example 3: Use custom engine with default config (legacy)
    # import sqlite3
    # from sqlalchemy import create_engine
    # sqlite_path = "streamlit/netflixdb.sqlite"
    # custom_engine = create_engine(
    #     f"sqlite+pysqlite:///{sqlite_path}?mode=ro&cache=shared&immutable=1",
    #     execution_options={"sqlite_raw_colnames": True},
    # )
    # streamlit_app_flow = SCAStreamlitApp(custom_engine)
    
    # Example 4: Use factory method with custom database (legacy)
    # streamlit_app_flow = SCAStreamlitApp.create_with_custom_db(
    #     "postgresql://user:pass@localhost/dbname"
    # )

    streamlit_app_flow.run()
