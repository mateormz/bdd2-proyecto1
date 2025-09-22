import re
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

# Tipos de índices
class IndexType(Enum):
    SEQUENTIAL = "SEQ"
    AVLTREE = "AVL"
    ISAM = "ISAM"
    BTREE = "BTree"
    EXTENDIBLE_HASH = "ExtHash"
    RTREE = "RTree"

# Tipos de datos
class DataType(Enum):
    INT = "INT"
    FLOAT = "FLOAT"
    VARCHAR = "VARCHAR"
    DATE = "DATE"
    ARRAY = "ARRAY"

# Columna de la tabla (atributo)
@dataclass
class Column:
    name: str
    data_type: DataType
    size: Optional[int] = None
    is_key: bool = False
    index_type: Optional[IndexType] = None
    is_array: bool = False
    array_type: Optional[DataType] = None


# Statements
@dataclass
class CreateTableStatement:
    table_name: str
    columns: List[Column]
    from_file: Optional[str] = None
    using_index: Optional[Tuple[IndexType, str]] = None

@dataclass
class SelectStatement:
    table_name: str
    columns: List[str]              # ['*'] para select all
    where_clause: Optional[Dict[str, Any]] = None
    spatial_query: Optional[Dict[str, Any]] = None

@dataclass
class InsertStatement:
    table_name: str
    values: List[Any]

@dataclass
class DeleteStatement:
    table_name: str
    where_clause: Dict[str, Any]

class SQLParserError(Exception):
    pass

# Update
# Transaction