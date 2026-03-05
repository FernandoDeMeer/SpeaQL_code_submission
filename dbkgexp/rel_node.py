from pydantic import BaseModel, Field
from typing import Dict, Any, List, Union, Optional


class RelationalNode(BaseModel):
    """
    Represents a single node/entity in a relational database.
    The structure is designed to be recursive, allowing for nested relationships,
    as nodes are connected to other nodes via foreign keys.
    """

    # Name of the table this node belongs to
    tableName: str = Field()
    # Primary key of the node, which is typically a unique identifier for the row in the table
    primaryKey: Optional[str] = Field(default=None)  # Always stored as string for consistency
    # Foreign keys of the node, which link to other nodes in the database
    foreignKeys: Any = Field(default=None)  # Can be None or a list of foreign keys
    # Data associated with the node, which is a dictionary of column names and their values
    data: Optional[Dict[str, Any]] = Field(default=None)
