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
    
    # Cursor
    def _current_token(self) -> str:
        return "" if self.current_token >= len(self.tokens) else self.tokens[self.current_token]

    def _peek_token(self, offset: int = 1) -> str:
        pos = self.current_token + offset
        return "" if pos >= len(self.tokens) else self.tokens[pos]

    def _consume_token(self) -> str:
        tok = self._current_token()
        self.current_token += 1
        return tok

    def _expect_token(self, expected: str) -> str:
        tok = self._consume_token()
        if tok.upper() != expected.upper():
            raise SQLParserError(f"Se esperaba '{expected}', se encontró '{tok}'")
        return tok
    
    def _parse_create_table(self):
        self._consume_token()        # CREATE
        self._expect_token("TABLE")
        table_name = self._consume_token()

        if self._current_token().upper() == "FROM":
            return self._parse_create_from_file(table_name)
        else:
            return self._parse_create_with_schema(table_name)
        
    def _parse_create_with_schema(self, table_name: str):
        self._expect_token("(")
        columns: List[Column] = []

        while self._current_token() != ")":
            columns.append(self._parse_column_definition())
            if self._current_token() == ",":
                self._consume_token()
            elif self._current_token() != ")":
                raise SQLParserError("Se esperaba ',' o ')' después de una columna")

        self._expect_token(")")
        return CreateTableStatement(table_name, columns)

    def _parse_column_definition(self) -> Column:
        name = self._consume_token()

        data_type_str = self._consume_token().upper()
        size = None
        is_array = False
        array_type = None

        if data_type_str == "VARCHAR":
            self._expect_token("[")
            size = int(self._consume_token())
            self._expect_token("]")
            data_type = DataType.VARCHAR
        elif data_type_str == "ARRAY":
            is_array = True
            self._expect_token("[")
            array_type_str = self._consume_token().upper()
            self._expect_token("]")
            array_type = DataType(array_type_str)
            data_type = DataType.ARRAY
        else:
            data_type = DataType(data_type_str)

        is_key = False
        index_type: Optional[IndexType] = None
        while self.current_token < len(self.tokens) and self._current_token() not in [",", ")"]:
            modifier = self._consume_token().upper()
            if modifier == "KEY":
                is_key = True
            elif modifier == "INDEX":
                index_name = self._consume_token().upper()
                index_map = {
                    "SEQ": IndexType.SEQUENTIAL, "AVL": IndexType.AVLTREE,
                    "ISAM": IndexType.ISAM, "BTREE": IndexType.BTREE,
                    "EXTHASH": IndexType.EXTENDIBLE_HASH, "RTREE": IndexType.RTREE
                }
                index_type = index_map.get(index_name)
                if not index_type:
                    raise SQLParserError(f"Tipo de índice no soportado: {index_name}")

        return Column(name, data_type, size, is_key, index_type, is_array, array_type)