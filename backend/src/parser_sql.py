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

# Parser
class ParserSQL:
    def __init__(self):
        self.tokens: List[str] = []
        self.current_token: int = 0

    def parse(self, sql_query: str):
        sql_query = self._normalize_query(sql_query)
        self.tokens = self._tokenize(sql_query)
        self.current_token = 0

        if not self.tokens:
            raise SQLParserError("Query vacía")

        command = self.tokens[0].upper()
        if command == "CREATE":
            return self._parse_create_table()
        elif command == "SELECT":
            return self._parse_select()
        elif command == "INSERT":
            return self._parse_insert()
        elif command == "DELETE":
            return self._parse_delete()
        else:
            raise SQLParserError(f"Comando no soportado: {command}")
    
    # Quita comentarios y espacios extra
    def _normalize_query(self, query: str) -> str:
        query = re.sub(r'--.*', '', query) # "-- Comentario"
        query = re.sub(r'/\*.*?\*/', '', query, flags=re.DOTALL) # "/* Comentario */"
        return ' '.join(query.split()).strip()
    
    def _tokenize(self, query: str) -> List[str]:
        pattern = r'''
            "([^"]*)"                    |  # strings dobles
            '([^']*)'                    |  # strings simples
            \b\d+\.\d+\b                |  # decimales
            \b\d+\b                     |  # enteros
            \b[A-Za-z_][A-Za-z0-9_]*\b  |  # identificadores
            [(),\[\]{}]                 |  # delimitadores
            [<>=!]+                     |  # operadores
            \S                             # otro símbolo
        '''
        tokens: List[str] = []
        for m in re.finditer(pattern, query, re.VERBOSE):
            tokens.append(m.group(0))
        return tokens