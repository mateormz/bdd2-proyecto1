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
            [+-]?\d+\.\d+                |  # decimales
            [+-]?\d+                     |  # enteros
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
    
    def _parse_create_from_file(self, table_name: str):
        self._expect_token("FROM")
        self._expect_token("FILE")
        file_path = self._consume_token().strip('"\'')  # permite comillas

        using_index = None
        if self._current_token().upper() == "USING":
            self._consume_token()
            self._expect_token("INDEX")
            index_name = self._consume_token().upper()
            self._expect_token("(")
            column_name = self._consume_token().strip('"\'')
            self._expect_token(")")
            index_map = {
                "ISAM": IndexType.ISAM, "SEQ": IndexType.SEQUENTIAL,
                "BTREE": IndexType.BTREE, "EXTHASH": IndexType.EXTENDIBLE_HASH,
                "RTREE": IndexType.RTREE
            }
            index_type = index_map.get(index_name)
            if not index_type:
                raise SQLParserError(f"Tipo de índice no soportado: {index_name}")
            using_index = (index_type, column_name)

        return CreateTableStatement(table_name, [], file_path, using_index)

    def _parse_select(self):
        self._consume_token()  # SELECT

        columns: List[str] = []
        if self._current_token() == "*":
            columns = ["*"]
            self._consume_token()
        else:
            while True:
                columns.append(self._consume_token())
                if self._current_token() == ",": self._consume_token()
                else: break

        self._expect_token("FROM")
        table_name = self._consume_token()

        where_clause = None
        spatial_query = None
        if self._current_token().upper() == "WHERE":
            self._consume_token()
            where_clause, spatial_query = self._parse_where_clause()

        return SelectStatement(table_name, columns, where_clause, spatial_query)
    
    def _parse_where_clause(self):
        column = self._consume_token()
        operator = self._consume_token()

        if operator.upper() == "IN" and self._current_token() == "(":
            return None, self._parse_spatial_query(column)

        if operator.upper() == "BETWEEN":
            a = self._parse_value()
            self._expect_token("AND")
            b = self._parse_value()
            return {"type": "range", "column": column, "start": a, "end": b}, None
        else:
            if self._current_token() in ("", ")"):
                raise SQLParserError("Falta valor en cláusula WHERE")
            val = self._parse_value()
            return {"type": "equality", "column": column, "operator": operator, "value": val}, None

    def _parse_spatial_query(self, column: str):
        self._expect_token("(")
        head = self._consume_token()
        self._expect_token(",")
        payload = self._parse_value()
        self._expect_token(")")

        if head.lower() == "point":
            if not (isinstance(payload, list) and (2 <= len(payload) <= 3)):
                raise SQLParserError("Esperaba [lon, lat] o [lon, lat, radio]")
            point = payload[:2]
            radio = payload[2] if len(payload) == 3 else None
            return {"type": "spatial_range", "column": column, "point": point, "radio": radio}
        else:
            k = int(head)
            if not (isinstance(payload, list) and len(payload) == 2):
                raise SQLParserError("Esperaba [lon, lat] para k-NN")
            return {"type": "spatial_knn", "column": column, "point": payload, "k": k}
        
    def _parse_value(self):
        tok = self._consume_token()

        if tok.startswith('"') and tok.endswith('"'):
            return tok[1:-1]
        if tok.startswith("'") and tok.endswith("'"):
            return tok[1:-1]

        try:
            return float(tok) if '.' in tok else int(tok)
        except ValueError:
            pass

        if tok == "[":
            vals = []
            while self._current_token() != "]":
                vals.append(self._parse_value())
                if self._current_token() == ",": self._consume_token()
            self._expect_token("]")
            return vals

        return tok  # identificador o string sin comillas

    def validate_statement(self, statement) -> bool:
        if isinstance(statement, CreateTableStatement):
            if statement.from_file and not statement.using_index:
                raise SQLParserError("CREATE TABLE FROM FILE requiere USING INDEX")
            return True
        elif isinstance(statement, (SelectStatement, InsertStatement, DeleteStatement)):
            return True
        return False
    
def parse_sql(query: str):
    parser = ParserSQL()
    return parser.parse(query)

if __name__ == "__main__":
    tests = [
        # SELECT *
        "select * from Empleados",

        # SELECT columnas específicas
        "SELECT nombre, salario FROM Empleados",

        # SELECT con igualdad (numérica)
        "select * from Empleados where id = 42",

        # SELECT con igualdad (string; ¡con comillas!)
        "select * from Empleados where nombre = 'Ana'",

        # SELECT con BETWEEN numérico
        "select * from Empleados where salario between 50000 and 100000",

        # SELECT con BETWEEN string (rango lexicográfico)
        "select * from Empleados where nombre between 'A' and 'M'",

        # SELECT espacial: rango por punto + radio (2 o 3 valores)
        "select * from Restaurantes where ubicacion in (point, [12.5, -77.0, 3])",

        # SELECT espacial: k-NN (k, [lon, lat])
        "select * from Restaurantes where ubicacion in (5, [12.5, -77.0])",

        # SELECT con otro operador (el parser lo captura como 'operator')
        "select * from Empleados where edad >= 30",

        # Caso inválido (para ver el error): falta valor
        "select * from Empleados where id ="
    ]

    for i, q in enumerate(tests, 1):
        try:
            print(f"\n== Test {i} ==")
            print(q)
            result = parse_sql(q)
            print("Resultado:", result)
        except Exception as e:
            print("ERROR:", e)