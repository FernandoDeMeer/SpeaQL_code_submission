import hashlib
import json
import sqlalchemy as sa
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker
from typing import Dict, Any, Optional, List, Union, Tuple
import networkx as nx
import pandas as pd

from dbkgexp.rel_node import RelationalNode

def stable_row_id(row:dict) -> str:
    """
    Generate a stable unique identifier for a row based on its content.
    This is useful for rows without primary keys.
    """
    row_string = json.dumps(row, sort_keys=True, default=str)
    return hashlib.sha256(row_string.encode()).hexdigest()


class RDBExplorer:
    """
    A class to explore relational databases using SQLAlchemy.
    It starts with an initial table and primary key and builds
    a graph of related nodes based on foreign key relationships.
    """

    def __init__(self, engine: Engine, initial_table: str):
        self.engine = engine
        self.metadata = sa.MetaData()
        self.metadata.reflect(bind=self.engine)
        self.table = self._get_table(initial_table)
        self.primary_key_attr = self._get_primary_key(self.table)
        self.explored_nodes: Dict[Tuple[str, str], RelationalNode] = {}
    
    def _create_node_key(self, table_name: str, primary_key: Any) -> Tuple[str, str]:
        """
        Helper method to create consistent node keys as tuples of two strings.
        Ensures that primary_key is always converted to a string for consistency.
        """
        return (str(table_name), str(primary_key))

    def _get_table(self, table_name: str) -> sa.Table:
        """
        Helper method to get a SQLAlchemy Table object by name from the metadata.
        """
        table = self.metadata.tables.get(table_name)
        if table is None:
            raise ValueError(f"Table '{table_name}' not found in the database.")
        return table

    def _get_primary_key(self, table: sa.Table) -> List[sa.Column]:
        """
        Helper method to get primary key columns for a given table.
        """
        return list(table.primary_key.columns.keys())

    def _get_primary_key_safe(self, table: sa.Table) -> Optional[List[str]]:
        """
        Helper method to safely get primary key columns for a given table.
        Returns None if the table has no primary key.
        """
        if table.primary_key and len(table.primary_key.columns) > 0:
            return list(table.primary_key.columns.keys())
        return None

    def _build_pk_clause(self, table: sa.Table, primary_key: Any) -> Dict[str, Any]:
        """
        Normalize primary key input into a column -> value mapping.
        Supports scalar PKs, tuples/lists for composite PKs, or explicit dicts.
        """
        pk_columns = self._get_primary_key(table)

        if isinstance(primary_key, dict):
            missing = [col for col in pk_columns if col not in primary_key]
            if missing:
                raise ValueError(
                    f"Missing primary key values for columns: {', '.join(missing)}"
                )
            return {col: primary_key[col] for col in pk_columns}

        if isinstance(primary_key, (list, tuple)):
            if len(primary_key) != len(pk_columns):
                raise ValueError(
                    f"Expected {len(pk_columns)} primary key values, got {len(primary_key)}"
                )
            return {col: primary_key[idx] for idx, col in enumerate(pk_columns)}

        if len(pk_columns) != 1:
            raise ValueError("Composite primary key requires tuple/list/dict input")

        return {pk_columns[0]: primary_key}

    def _fetch_rows_by_values(
        self, table: sa.Table, column_values: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Fetch rows from a table that match the provided column -> value filters.
        Returns a list of row dictionaries.
        """
        with self.engine.connect() as conn:
            where_clauses = [table.c[attr] == value for attr, value in column_values.items()]
            query = sa.select(table).where(*where_clauses)
            cursor = conn.execute(query)
            rows = cursor.fetchall() if cursor else []

            return [
                {column.name: row[idx] for idx, column in enumerate(table.columns)}
                for row in rows
            ]

    def _fetch_data_pk(
        self, table: sa.Table, primary_key: Any
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch data for a specific primary key from the table.
        Raises ValueError if no data is found.
        """
        pk_clause = self._build_pk_clause(table, primary_key)
        rows = self._fetch_rows_by_values(table, pk_clause)
        if rows:
            return rows[0]

    def _fetch_data_fk(
        self, table: sa.Table, foreign_key: sa.ForeignKey, foreign_key_value: Any
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Fetch data for a specific foreign key value from a child table.
        Returns a list of dictionaries, each representing a row.
        """
        fk_clause = {foreign_key.parent.name: foreign_key_value}
        rows = self._fetch_rows_by_values(table, fk_clause)
        if rows:
            return rows

    def get_related_tables(
        self, table: sa.Table
    ) -> Tuple[
        List[Tuple[sa.Table, List[sa.ForeignKey]]],
        List[Tuple[sa.Table, List[sa.ForeignKey]]],
    ]:
        """
        Get a list of tables related to the given table. Related tables are determined by foreign key relationships:

        Args: table (sa.Table): The table to find related tables for.
        Returns:
            related_parent_tables (List[Tuple[sa.Table, List[sa.ForeignKey]]]): A list of tuples containing related parent tables and grouped foreign keys (grouped per constraint).
            related_child_tables (List[Tuple[sa.Table, List[sa.ForeignKey]]]): A list of tuples containing related child tables and grouped foreign keys (grouped per constraint).
        """
        related_parent_tables: List[Tuple[sa.Table, List[sa.ForeignKey]]] = []
        for fk_constraint in table.foreign_key_constraints:
            fk_elements = list(fk_constraint.elements)
            if not fk_elements:
                continue

            target_tables = {fk.column.table for fk in fk_elements}
            if len(target_tables) != 1:
                # Skip ambiguous constraints spanning multiple target tables
                continue

            related_parent_tables.append((fk_elements[0].column.table, fk_elements))

        # Then, find all foreign keys in other (child) tables that point to the primary key of the given table
        related_child_tables: List[Tuple[sa.Table, List[sa.ForeignKey]]] = []
        for other_table in sorted(self.metadata.tables.values(), key=lambda t: t.name):
            if other_table is table:
                continue

            for fk_constraint in other_table.foreign_key_constraints:
                fk_elements = list(fk_constraint.elements)
                if not fk_elements:
                    continue

                target_tables = {fk.column.table for fk in fk_elements}
                if table in target_tables and len(target_tables) == 1:
                    related_child_tables.append((other_table, fk_elements))

        return related_parent_tables, related_child_tables

    def explore(
        self,
        table_name: Optional[str] = None,
        primary_key: Optional[str] = None,
        depth: int = 1,
    ):
        """
        Method to recursively explore the database starting from a given table and primary key.
        Args:
            table_name (Optional[str]): The name of the table to start exploration from.
            primary_key (Optional[str]): The primary key of the table to start exploration from.
            depth (int): Number of levels (recursion depth i.e., how many levels of foreign keys to explore).
        Returns:
            Dict[(str, str), RelationalNode]: A dictionary of explored nodes with their table names and primary keys.
        """
        explored_nodes = self.recursive_explore(
            table_name, primary_key, current_depth=0, max_depth=depth
        )
        return explored_nodes

    def _process_parent_relationships(self, node: RelationalNode, table: sa.Table, current_depth: int, max_depth: int):
        """
        Helper method to process parent relationships for a given node.
        """
        if current_depth >= max_depth:
            return
            
        related_parents, _ = self.get_related_tables(table)
        
        # Process parent relationships
        for related_table, fk_group in related_parents:
            if not node.data:
                continue

            parent_lookup_values = {}
            for fk in fk_group:
                child_value = node.data.get(fk.parent.name)
                if child_value is None:
                    parent_lookup_values = {}
                    break
                parent_lookup_values[fk.column.name] = child_value

            if not parent_lookup_values:
                continue

            parent_row = self._fetch_rows_by_values(related_table, parent_lookup_values)
            if not parent_row:
                continue

            parent_row_data = parent_row[0]
            pk_columns = self._get_primary_key_safe(related_table)
            if not pk_columns:
                continue

            if len(pk_columns) == 1:
                related_pk = parent_row_data.get(pk_columns[0])
                if related_pk is None:
                    continue
            else:
                related_pk_values = tuple(parent_row_data.get(col) for col in pk_columns)
                if any(value is None for value in related_pk_values):
                    continue
                related_pk = related_pk_values

            self.recursive_explore(
                related_table.name, related_pk, current_depth + 1, max_depth
            )

    def recursive_explore(
        self, table_name: str, primary_key: Any, current_depth: int, max_depth: int
    ) -> Dict[Tuple[str, str], RelationalNode]:
        """
        Recursive method to explore based on foreign key relationships in the database.
        Args:
            table_name (str): The name of the table to explore.
            primary_key (str): The current primary key of the node being explored.
            current_depth (int): Current depth in the recursion.
            max_depth (int): Maximum depth to explore.
        Returns:
            Dict[(str, str), RelationalNode]: A dictionary of explored nodes with their table names and primary keys.
        """

        table = self._get_table(table_name)
        node_key = self._create_node_key(table_name, primary_key)
        if node_key in self.explored_nodes:
            return self.explored_nodes

        node_data = self._fetch_data_pk(table, primary_key)
        node = RelationalNode(
            tableName=table_name,
            primaryKey=str(primary_key),
            data=node_data,
            foreignKeys=table.foreign_keys if table.foreign_keys else None,
        )
        self.explored_nodes[node_key] = node

        if current_depth >= max_depth:
            return self.explored_nodes

        related_parents, related_children = self.get_related_tables(table)

        # Process parent relationships for the current node
        self._process_parent_relationships(node, table, current_depth, max_depth)

        # Process child relationships
        for related_table, fk_group in related_children:
            if not node.data:
                continue

            child_lookup_values = {}
            for fk in fk_group:
                parent_value = node.data.get(fk.column.name)
                if parent_value is None:
                    child_lookup_values = {}
                    break
                child_lookup_values[fk.parent.name] = parent_value

            if not child_lookup_values:
                continue

            children_rows = self._fetch_rows_by_values(related_table, child_lookup_values)
            if not children_rows:
                continue

            pk_columns = self._get_primary_key_safe(related_table)
            if pk_columns:
                # Child table has primary key, continue recursion
                for row in children_rows:
                    if len(pk_columns) == 1:
                        related_pk = row.get(pk_columns[0])
                        if related_pk is None:
                            continue
                    else:
                        related_pk_values = tuple(row.get(col) for col in pk_columns)
                        if any(value is None for value in related_pk_values):
                            continue
                        related_pk = related_pk_values

                    self.recursive_explore(
                        related_table.name,
                        related_pk,
                        current_depth + 1,
                        max_depth,
                    )
            else:
                # Child table has no primary key, create nodes but don't recurse further
                for row in children_rows:
                    row_hash = stable_row_id(row)
                    row_identifier = f"no_pk_{row_hash}"
                    child_node_key = self._create_node_key(related_table.name, row_identifier)
                    if child_node_key not in self.explored_nodes:
                        child_node = RelationalNode(
                            tableName=related_table.name,
                            primaryKey=str(row_identifier),
                            data=row,
                            foreignKeys=related_table.foreign_keys if related_table.foreign_keys else None,
                        )
                        self.explored_nodes[child_node_key] = child_node

                        # Process parent relationships for this child node even though it has no primary key
                        self._process_parent_relationships(child_node, related_table, current_depth + 1, max_depth)

        return self.explored_nodes

    def explore_from_existing_node(
        self, table_name: str, node_identifier: str, depth: int = 1
    ) -> Dict[Tuple[str, str], RelationalNode]:
        """
        Method to explore the database starting from an existing node that's already in self.explored_nodes.
        This is useful for nodes without primary keys that were discovered during previous explorations.
        
        Args:
            table_name (str): The name of the table of the existing node.
            node_identifier (str): The identifier of the existing node (could be primary key or generated identifier).
            depth (int): Number of levels (recursion depth i.e., how many levels of foreign keys to explore).
        Returns:
            Dict[(str, str), RelationalNode]: A dictionary of explored nodes with their table names and identifiers.
        """
        node_key = self._create_node_key(table_name, node_identifier)
        if node_key not in self.explored_nodes:
            raise ValueError(f"Node ({table_name}, {node_identifier}) not found in explored_nodes.")

        return self.recursive_explore_from_node_no_pk(
            table_name, node_identifier, current_depth=0, max_depth=depth
        )

    def recursive_explore_from_node_no_pk(
        self, table_name: str, node_identifier: str, current_depth: int, max_depth: int
    ) -> Dict[Tuple[str, str], RelationalNode]:
        """
        Recursive method to explore based on foreign key relationships starting from an existing node.
        This function is called on nodes without a primary key.

        Args:
            table_name (str): The name of the table of the existing node.
            node_identifier (str): The identifier of the existing node.
            current_depth (int): Current depth in the recursion.
            max_depth (int): Maximum depth to explore.
        Returns:
            Dict[(str, str), RelationalNode]: A dictionary of explored nodes with their table names and identifiers.
        """
        if current_depth >= max_depth:
            return self.explored_nodes
            
        node_key = self._create_node_key(table_name, node_identifier)
        node = self.explored_nodes[node_key]
        table = self._get_table(table_name)
        
        # Process parent relationships
        self._process_parent_relationships(node, table, current_depth, max_depth)

        return self.explored_nodes

    def _get_node_label_and_display_attr(self, node: RelationalNode, table_name: str, primary_key: Any, node_attributes: Optional[Dict[str, Union[str, List[str]]]] = None) -> Tuple[str, Union[str, List[str], None]]:
        """
        Helper method to determine the node label and display attribute(s) from node data.
        
        Args:
            node: The RelationalNode to get the label for
            table_name: Name of the table
            primary_key: Primary key value
            node_attributes: Optional dict mapping table names to attribute names (string or list of strings)
        
        Returns:
            Tuple of (node_label, display_attr_used)
        """
        display_attr = node_attributes.get(table_name) if node_attributes else None
        
        if display_attr and node.data:
            # Handle case where display_attr is a list
            if isinstance(display_attr, list):
                # Always combine all non-empty attributes when list is provided
                values = []
                used_attrs = []
                for attr in display_attr:
                    if attr in node.data and node.data[attr]:
                        values.append(str(node.data[attr]))
                        used_attrs.append(attr)
                
                if values:
                    # Join multiple values with " " separator
                    combined_label = " ".join(values)
                    return combined_label, used_attrs
                else:
                    # No attributes had values, return primary key and original list
                    return str(primary_key), display_attr
            # Handle case where display_attr is a single string
            elif isinstance(display_attr, str):
                if display_attr in node.data and node.data[display_attr]:
                    return str(node.data[display_attr]), display_attr
        
        # Fallback to primary key if no specific attribute is specified or found
        return str(primary_key), display_attr

    def build_graph(
        self, 
        explored_nodes: Dict[Tuple[str, str], RelationalNode],
        node_attributes: Optional[Dict[str, Union[str, List[str]]]] = None
    ) -> nx.DiGraph:
        """
        Build a directed graph of related nodes using the explored nodes.
        
        Args:
            explored_nodes: Dictionary of explored nodes with their table names and primary keys
            node_attributes: Optional dict mapping table names to attribute names (string or list of strings) to use for node representation.
                           When a list is provided, all non-empty attribute values are combined with spaces.
        """
        # Default node attributes if not provided
        if node_attributes is None:
            node_attributes = {}
            
        graph = nx.DiGraph()

        for (table_name, primary_key), node in explored_nodes.items():
            node_id = f"{table_name}:{primary_key}"
            
            # Get node label and display attribute using helper method
            node_label, display_attr_used = self._get_node_label_and_display_attr(
                node, table_name, primary_key, node_attributes
            )
            
            graph.add_node(
                node_id, 
                label=node_label, 
                data=node.data,
                node_type="entity",
                table_name=table_name,
                primary_key=primary_key,
                display_attribute=display_attr_used
            )

        # Add edges based on foreign key relationships
        for (table_name, primary_key), node in explored_nodes.items():
            node_id = f"{table_name}:{primary_key}"
            if node.foreignKeys is not None:
                for fk in node.foreignKeys:
                    related_table = fk.column.table.name
                    try:
                        related_pk = node.data.get(fk.parent.name)
                        if related_pk is not None:
                            related_node_id = f"{related_table}:{related_pk}"
                            # Only add edge if the related node exists in explored_nodes
                            related_node_key = (str(related_table), str(related_pk))
                            if related_node_key in explored_nodes:
                                graph.add_edge(node_id, related_node_id, label=fk.name)
                    except AttributeError:
                        continue  # Skip if data is not available

        return graph

    def build_graph_group_by_table(
        self, 
        explored_nodes: Dict[Tuple[str, str], RelationalNode],
        node_attributes: Optional[Dict[str, Union[str, List[str]]]] = None
    ) -> nx.DiGraph:
        """
        Build a digraph in which nodes of the same table connected to the same parent node 
        are grouped together.
        
        For each node with a primary key, we group all of the child nodes of the 
        same table by linking them to a "table" node linked to the parent node and each child node.
        
        Args:
            explored_nodes: Dictionary of explored nodes with their table names and primary keys
            node_attributes: Optional dict mapping table names to attribute names (string or list of strings) to use for node representation.
                           When a list is provided, all non-empty attribute values are combined with spaces.
                           e.g., {'users': 'name', 'orders': 'order_number'} or {'users': ['name', 'username'], 'orders': ['order_number', 'id']}
        
        Returns:
            nx.DiGraph: A directed graph where nodes are grouped by tables as described.
        """
        # Default node attributes if not provided
        if node_attributes is None:
            node_attributes = {}
        
        graph = nx.DiGraph()
        
        # First, add all individual nodes to the graph
        for (table_name, primary_key), node in explored_nodes.items():
            # Get node label and display attribute using helper method
            node_label, display_attr_used = self._get_node_label_and_display_attr(
                node, table_name, primary_key, node_attributes
            )
            
            node_id = f"{table_name}:{primary_key}"
            graph.add_node(
                node_id,
                label=node_label,
                node_type="entity",
                table_name=table_name,
                primary_key=primary_key,
                data=node.data,
                display_attribute=display_attr_used
            )
        
        # Track which tables have been grouped for each parent node
        parent_table_groups = {}  # parent_node_id -> set of child table names
        parent_child_counts = {}  # (parent_node_id, child_table) -> count of children
        
        # First pass: Count children of each table type for each parent
        for (table_name, primary_key), node in explored_nodes.items():
            if node.foreignKeys is not None:
                for fk in node.foreignKeys:
                    related_table = fk.column.table.name
                    related_pk = node.data.get(fk.parent.name) if node.data else None
                    
                    if related_pk is not None and (related_table, related_pk) in explored_nodes:
                        parent_node_id = f"{related_table}:{related_pk}"
                        child_table = table_name
                        
                        key = (parent_node_id, child_table)
                        parent_child_counts[key] = parent_child_counts.get(key, 0) + 1
        
        # Second pass: Create table groups only when there are multiple children of the same type
        for (table_name, primary_key), node in explored_nodes.items():
            node_id = f"{table_name}:{primary_key}"
            
            if node.foreignKeys is not None:
                for fk in node.foreignKeys:
                    related_table = fk.column.table.name
                    related_pk = node.data.get(fk.parent.name) if node.data else None
                    
                    if related_pk is not None:
                        related_node_id = f"{related_table}:{related_pk}"
                        
                        # Only process if the related node exists in explored_nodes
                        related_node_key = (str(related_table), str(related_pk))
                        if related_node_key in explored_nodes:
                            parent_node_id = related_node_id
                            child_table = table_name
                            
                            # Check if this parent has multiple children of this table type
                            child_count = parent_child_counts.get((parent_node_id, child_table), 0)
                            
                            if child_count > 1:
                                # Multiple children of this type - create/use table group
                                if parent_node_id not in parent_table_groups:
                                    parent_table_groups[parent_node_id] = set()
                                
                                # Create table group if this is the first child of this type for this parent
                                if child_table not in parent_table_groups[parent_node_id]:
                                    parent_table_groups[parent_node_id].add(child_table)
                                    
                                    # Create a table group node
                                    table_group_id = f"table_group:{related_table}:{related_pk}:{child_table}"
                                    table_group_label = f"{child_table.title()} Table"
                                    
                                    graph.add_node(
                                        table_group_id,
                                        label=table_group_label,
                                        node_type="table_group",
                                        parent_table=related_table,
                                        child_table=child_table,
                                        parent_key=related_pk
                                    )
                                    
                                    # Connect parent node to table group
                                    graph.add_edge(
                                        parent_node_id,
                                        table_group_id,
                                        label=f"has_{child_table}",
                                        edge_type="parent_to_table_group"
                                    )
                                
                                # Connect child node to its table group
                                table_group_id = f"table_group:{related_table}:{related_pk}:{child_table}"
                                graph.add_edge(
                                    table_group_id,
                                    node_id,
                                    label="contains",
                                    edge_type="table_group_to_child"
                                )
                            else:
                                # Only one child of this type - use direct relationship
                                graph.add_edge(
                                    node_id,
                                    related_node_id,
                                    label=fk.name,
                                    edge_type="direct_foreign_key"
                                )
        
        return graph
        
    def filter_explored_nodes(
        self,
        explored_nodes: Dict[Tuple[str, str], RelationalNode], max_nodes_per_table: int = 100,
        relevant_nodes: Optional[Dict[Tuple[str, str], RelationalNode]] = None,
        persistent_nodes: Optional[Dict[Tuple[str, str], RelationalNode]] = None
    ) -> Dict[Tuple[str, str], RelationalNode]:
        """
        Filter explored nodes to limit the number of nodes per table for visualization purposes.
        After limiting nodes per table, removes any disconnected nodes (nodes with no connections
        to other nodes in the filtered set).
        
        Args:
            explored_nodes: Dictionary of explored nodes with their table names and primary keys
            max_nodes_per_table: Maximum number of nodes to keep per table
        Returns:
            Dict[(str, str), RelationalNode]: Filtered dictionary of explored nodes with disconnected nodes removed
        """
        # Step 0: Apply per-table limit
        table_node_counts: Dict[str, int] = {}
        filtered_nodes: Dict[Tuple[str, str], RelationalNode] = {}

        for (table_name, primary_key), node in explored_nodes.items():
            count = table_node_counts.get(table_name, 0)
            if count < max_nodes_per_table:
                filtered_nodes[(table_name, primary_key)] = node
                table_node_counts[table_name] = count + 1

        # Step 1: If relevant_nodes or persistent_nodes are provided, ensure they are included in the filtered set
        if relevant_nodes:
            for node_id in relevant_nodes:
                if node_id not in filtered_nodes.keys():
                    try:
                        filtered_nodes[node_id] = explored_nodes[node_id]
                    except KeyError:
                        # If the node_id is just the primaryKey without a table name, we try to look for the primary key across all explored nodes
                        graph_data_keys = pd.DataFrame(explored_nodes.keys(), columns=["table_name", "primary_key"])
                        matching_key = graph_data_keys[graph_data_keys["primary_key"] == node_id]
                        if len(matching_key) > 0:
                            table_name = matching_key.iloc[0]["table_name"]
                            primary_key = matching_key.iloc[0]["primary_key"]
                            node_id = (table_name, primary_key)
                            try:
                                filtered_nodes[node_id] = explored_nodes[node_id]
                            except KeyError:
                                pass
                            filtered_nodes[node_id] = explored_nodes[node_id]
                            filtered_nodes.pop(primary_key, None)
        if persistent_nodes:
            for node_id in persistent_nodes:
                if node_id not in filtered_nodes:
                    try:
                        filtered_nodes[node_id] = explored_nodes[node_id]
                    except KeyError:
                        pass

        # Step 2: Build connection map to identify disconnected nodes
        # A node is connected if it has at least one edge (incoming or outgoing) to another node in filtered_nodes
        incoming_index: Dict[Tuple[str, str], List[Tuple[str, str]]] = {key: [] for key in filtered_nodes.keys()}
        node_connections: Dict[Tuple[str, str], int] = {key: 0 for key in filtered_nodes.keys()}

        for (table_name, primary_key), node in filtered_nodes.items():
            if node.foreignKeys is None:
                continue

            child_key = (table_name, primary_key)
            for fk in node.foreignKeys:
                related_table = fk.column.table.name
                related_pk = node.data.get(fk.parent.name) if node.data else None

                if related_pk is None:
                    continue

                parent_key = (str(related_table), str(related_pk))
                if parent_key in filtered_nodes:
                    incoming_index[parent_key].append(child_key)

        for parent_key, children in incoming_index.items():
            for child_key in children:
                node_connections[parent_key] += 1
                node_connections[child_key] += 1
        
        # Step 3: If any node has zero connections, remove it from filtered_nodes unless it's in relevant_nodes or persistent_nodes
        final_filtered_nodes: Dict[Tuple[str, str], RelationalNode] = {}
        for node_key, connection_count in node_connections.items():
            if connection_count > 0 or node_key in (relevant_nodes or {}) or node_key in (persistent_nodes or {}):
                final_filtered_nodes[node_key] = filtered_nodes[node_key]

        return final_filtered_nodes

    def build_graph_with_options(
        self,
        explored_nodes: Dict[Tuple[str, str], RelationalNode],
        relevant_nodes: Optional[Dict[Tuple[str, str], RelationalNode]] = None,
        persistent_nodes: Optional[Dict[Tuple[str, str], RelationalNode]] = None,
        group_by_table: bool = False,
        node_attributes: Optional[Dict[str, Union[str, List[str]]]] = None
    ) -> nx.DiGraph:
        """
        Build a graph with options for different visualization types.
        
        Args:
            explored_nodes: Dictionary of explored nodes with their table names and primary keys
            group_by_table: If True, return grouped graphs by table; if False, return standard graph
            node_attributes: Optional dict mapping table names to attribute names (string or list of strings) for node representation.
                           When a list is provided, all non-empty attribute values are combined with spaces.
        
        Returns:
            nx.DiGraph: Graph based on the specified options
        """
        explored_nodes = self.filter_explored_nodes(explored_nodes=explored_nodes,
                                                    relevant_nodes=relevant_nodes,
                                                    persistent_nodes=persistent_nodes)

        if group_by_table:
            return self.build_graph_group_by_table(explored_nodes, node_attributes)
        else:
            return self.build_graph(explored_nodes, node_attributes)
