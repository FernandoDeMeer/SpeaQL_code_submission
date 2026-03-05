# sca_gui_db_utils.py
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
from sqlalchemy import MetaData, Table, create_engine, inspect, text, select
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError


class SCAGuiDBUtils:
    """
    A class to handle utility functions for the SCA application GUI.
    """

    """
    SQLite-first utilities for your Streamlit app.
    - Engine avoids sqlite detect_types to prevent double datetime parsing.
    - Query helpers return plain dicts.
    - KG helpers: get_color_map.
    """

    # ---------- Engine ---------- #

    @staticmethod
    def get_engine(db_url: str, engine_options: Optional[Dict[str, Any]] = None) -> Engine:
        connect_args: Dict[str, Any] = {}
        if db_url.startswith("sqlite+pysqlite://"):
            # honor URI query params (?mode=ro&cache=shared&immutable=1) and allow Streamlit threads
            connect_args = {"check_same_thread": False, "uri": True}
        
        # Prepare engine kwargs with defaults
        engine_kwargs = {"future": True, "connect_args": connect_args}
        
        # Merge in any additional engine options provided by the user
        if engine_options:
            engine_kwargs.update(engine_options)
            # If user provided connect_args, merge them with our defaults
            if "connect_args" in engine_options:
                merged_connect_args = connect_args.copy()
                merged_connect_args.update(engine_options["connect_args"])
                engine_kwargs["connect_args"] = merged_connect_args
        
        engine = create_engine(db_url, **engine_kwargs)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return engine

    # ---------- Introspection ---------- #

    @staticmethod
    def get_db_tables(engine: Engine):
        """
        Get the list of tables in the database.
        :param engine: SQLAlchemy engine connected to the database.
        :return: List of table names.
        """
        metadata = MetaData()
        metadata.reflect(bind=engine)
        tables = list(metadata.tables.keys())
        return tables

    @staticmethod
    def get_table_columns(engine: Engine, table_name: str):
        """
        Get the columns of a specific table.
        :param engine: SQLAlchemy engine connected to the database.
        :param table_name: Name of the table to query.
        :return: List of column names.
        """
        table = Table(table_name, MetaData(), autoload_with=engine)
        return [column.name for column in table.columns]

    @staticmethod
    def _reflect_table(engine: Engine, table_name: str) -> Table:
        md = MetaData()
        return Table(table_name, md, autoload_with=engine)

    # ---------- KG helpers ---------- #

    @staticmethod
    def get_color_map(engine: Engine):
        """
        Get a deterministic color map for the tables in the database using colors.
        Generates unlimited unique colors for any number of tables.
        :param engine: SQLAlchemy engine connected to the database.
        :return: Dictionary mapping table names to colors.
        """
        import hashlib
        import colorsys

        def hash_to_distinct_color(text: str) -> str:
            """
            Convert a string to a deterministic distinct color with low saturation using HSL color space.
            """
            # Get deterministic hash
            hash_value = int(hashlib.sha256(text.encode()).hexdigest()[:8], 16)

            # Use golden ratio for better hue distribution to maximize distinctness
            golden_ratio = 0.618033988749
            hue = (hash_value * golden_ratio) % 1.0

            # Low saturation for muted colors (20-50%)
            saturation = 0.2 + (hash_value % 30) / 100.0

            # Medium lightness for good contrast and visibility (50-80%)
            lightness = 0.5 + (hash_value % 30) / 100.0

            # Convert HSL to RGB
            rgb = colorsys.hls_to_rgb(hue, lightness, saturation)

            # Convert to hex
            hex_color = "#{:02x}{:02x}{:02x}".format(
                int(rgb[0] * 255), int(rgb[1] * 255), int(rgb[2] * 255)
            )

            return hex_color

        tables = SCAGuiDBUtils.get_db_tables(engine)
        color_map = {table: hash_to_distinct_color(table) for table in tables}

        return color_map

    # ---------- Query helpers ---------- #

    @staticmethod
    def query_table_by_column(
        engine: Engine,
        table_name: str,
        column_name: str,
        value: Any,
        limit: int = 200,
        case_insensitive: bool = True,
        allow_wildcard: bool = True,
    ) -> List[dict]:
        """
        Query table by one column/value using textual SQL to avoid datetime processors.
        """
        tbl = SCAGuiDBUtils._reflect_table(engine, table_name)
        if column_name not in tbl.c:
            raise KeyError(f"Column '{column_name}' not found in table '{table_name}'.")

        params: Dict[str, Any] = {}
        if isinstance(value, str):
            raw = value.strip()
            pattern = raw.replace("*", "%") if allow_wildcard else raw
            has_wild = allow_wildcard and any(ch in pattern for ch in ("%", "_"))
            if has_wild:
                where_sql = (
                    f"WHERE LOWER({column_name}) LIKE LOWER(:pattern)"
                    if case_insensitive
                    else f"WHERE {column_name} LIKE :pattern"
                )
                params["pattern"] = pattern
            else:
                where_sql = (
                    f"WHERE LOWER({column_name}) = LOWER(:val)"
                    if case_insensitive
                    else f"WHERE {column_name} = :val"
                )
                params["val"] = raw
        else:
            where_sql = f"WHERE {column_name} = :val"
            params["val"] = value

        limit_sql = (
            f" LIMIT {int(limit)}" if isinstance(limit, int) and limit > 0 else ""
        )
        sql = text(f"SELECT * FROM {table_name} {where_sql}{limit_sql}")

        try:
            with engine.connect() as conn:
                res = conn.execute(sql, params)
                return [dict(r._mapping) for r in res.fetchall()]
        except SQLAlchemyError as exc:
            raise RuntimeError(
                f"Failed querying {table_name}.{column_name} with value={value!r}"
            ) from exc
