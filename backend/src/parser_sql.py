import re
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum


class IndexType(Enum):
    SEQUENTIAL = "SEQ"
    AVLTREE = "AVL"
    ISAM = "ISAM"
    BTREE = "BTree"
    BPTREE_CLUSTERED = "BPTreeClustered"
    EXTENDIBLE_HASH = "ExtHash"
    RTREE = "RTree"


class DataType(Enum):
    INT = "INT"
    FLOAT = "FLOAT"
    VARCHAR = "VARCHAR"
    DATE = "DATE"
    ARRAY = "ARRAY"


@dataclass
class Column:
    name: str
    data_type: DataType
    size: Optional[int] = None
    is_key: bool = False
    index_type: Optional[IndexType] = None
    is_array: bool = False
    array_type: Optional[DataType] = None


@dataclass
class CreateTableStatement:
    table_name: str
    columns: List[Column]
    from_file: Optional[str] = None
    using_index: Optional[Tuple[IndexType, str]] = None


@dataclass
class SelectStatement:
    table_name: str
    columns: List[str]
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

    def _normalize_query(self, query: str) -> str:
        query = re.sub(r'--.*', '', query)
        query = re.sub(r'/\*.*?\*/', '', query, flags=re.DOTALL)
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
        self._consume_token()
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

        index_map = {
            # Sequential
            "SEQ": IndexType.SEQUENTIAL,
            "SEQUENTIAL": IndexType.SEQUENTIAL,
            "SEQUENTIAL_ORDERED": IndexType.SEQUENTIAL,
            "SEQUENTIAL_ORDERED_FILE": IndexType.SEQUENTIAL,
            # AVL
            "AVL": IndexType.AVLTREE,
            "AVLTREE": IndexType.AVLTREE,
            # ISAM
            "ISAM": IndexType.ISAM,
            # B-Tree
            "BTREE": IndexType.BTREE,
            "BPTREE": IndexType.BTREE,
            "B+TREE": IndexType.BTREE,
            # B+Tree Clustered
            "BPTREE_CLUSTERED": IndexType.BPTREE_CLUSTERED,
            "B+TREE_CLUSTERED": IndexType.BPTREE_CLUSTERED,
            "BPTREECLUSTERED": IndexType.BPTREE_CLUSTERED,
            "B+TREE_CLUSTER": IndexType.BPTREE_CLUSTERED,
            "B+TREEFILE": IndexType.BPTREE_CLUSTERED,
            # Extensible Hash
            "EXTHASH": IndexType.EXTENDIBLE_HASH,
            "EXTENDIBLE_HASH": IndexType.EXTENDIBLE_HASH,
            "EXT_HASH": IndexType.EXTENDIBLE_HASH,
            # R-Tree
            "RTREE": IndexType.RTREE,
            "RTREEE": IndexType.RTREE,  # tolerancia a typo común
            "RTREE_INDEX": IndexType.RTREE,
            "RTREEFILE": IndexType.RTREE,
            "RTREE_FILE": IndexType.RTREE,
        }

        while self.current_token < len(self.tokens) and self._current_token() not in [",", ")"]:
            modifier = self._consume_token().upper()
            if modifier == "KEY":
                is_key = True
            elif modifier == "INDEX":
                idx_name = self._consume_token().upper()
                index_type = index_map.get(idx_name)
                if not index_type:
                    raise SQLParserError(f"Tipo de índice no soportado: {idx_name}")

        return Column(name, data_type, size, is_key, index_type, is_array, array_type)

    def _parse_create_from_file(self, table_name: str):
        self._expect_token("FROM")
        self._expect_token("FILE")
        file_path = self._consume_token().strip('"\'')

        using_index = None
        if self._current_token().upper() == "USING":
            self._consume_token()
            self._expect_token("INDEX")
            idx_name = self._consume_token().upper()
            self._expect_token("(")
            column_name = self._consume_token().strip('"\'')
            self._expect_token(")")

            index_map = {
                # Sequential
                "SEQ": IndexType.SEQUENTIAL,
                "SEQUENTIAL": IndexType.SEQUENTIAL,
                "SEQUENTIAL_ORDERED": IndexType.SEQUENTIAL,
                "SEQUENTIAL_ORDERED_FILE": IndexType.SEQUENTIAL,
                # ISAM
                "ISAM": IndexType.ISAM,
                # B-Tree
                "BTREE": IndexType.BTREE,
                "BPTREE": IndexType.BTREE,
                "B+TREE": IndexType.BTREE,
                # B+Tree Clustered
                "BPTREE_CLUSTERED": IndexType.BPTREE_CLUSTERED,
                "B+TREE_CLUSTERED": IndexType.BPTREE_CLUSTERED,
                "BPTREECLUSTERED": IndexType.BPTREE_CLUSTERED,
                "B+TREE_CLUSTER": IndexType.BPTREE_CLUSTERED,
                "B+TREEFILE": IndexType.BPTREE_CLUSTERED,
                # Extensible Hash
                "EXTHASH": IndexType.EXTENDIBLE_HASH,
                "EXTENDIBLE_HASH": IndexType.EXTENDIBLE_HASH,
                "EXT_HASH": IndexType.EXTENDIBLE_HASH,
                # R-Tree
                "RTREE": IndexType.RTREE,
            }
            index_type = index_map.get(idx_name)
            if not index_type:
                raise SQLParserError(f"Tipo de índice no soportado: {idx_name}")
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
                if self._current_token() == ",":
                    self._consume_token()
                else:
                    break

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

        # Rango espacial (point, [...])
        if head.lower() == "point":
            if not isinstance(payload, list):
                raise SQLParserError("Esperaba lista para el punto: [x, y], [x, y, r] o [x, y, z, r]")

            n = len(payload)
            if n == 2:
                point = payload
                radio = None
            elif n == 3:
                point = payload[:2]
                radio = payload[2]
            elif n == 4:
                # 3D con radio (x, y, z, r)
                point = payload[:3]
                radio = payload[3]
            else:
                raise SQLParserError("Formato inválido: usa [x,y], [x,y,r] o [x,y,z,r]")

            return {"type": "spatial_range", "column": column, "point": point, "radio": radio}

        try:
            k = int(head)
        except ValueError:
            raise SQLParserError("k inválido para consulta k-NN")

        if not (isinstance(payload, list) and (len(payload) == 2 or len(payload) == 3)):
            raise SQLParserError("Esperaba [x, y] (2D) o [x, y, z] (3D) para k-NN")

        return {"type": "spatial_knn", "column": column, "point": payload, "k": k}

    def _parse_delete(self):
        self._consume_token()  # DELETE
        self._expect_token("FROM")
        table_name = self._consume_token()
        self._expect_token("WHERE")
        where_clause, _ = self._parse_where_clause()
        if not where_clause:
            raise SQLParserError("DELETE requiere WHERE válido")
        return DeleteStatement(table_name, where_clause)

    def _parse_insert(self):
        self._consume_token()  # INSERT
        self._expect_token("INTO")
        table_name = self._consume_token()
        self._expect_token("VALUES")
        self._expect_token("(")

        values: List[Any] = []
        while self._current_token() != ")":
            values.append(self._parse_value())
            if self._current_token() == ",":
                self._consume_token()
            elif self._current_token() != ")":
                raise SQLParserError("Se esperaba ',' o ')' en VALUES")

        self._expect_token(")")
        return InsertStatement(table_name, values)

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
                if self._current_token() == ",":
                    self._consume_token()
            self._expect_token("]")
            return vals
        return tok

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
    stmt = parser.parse(query)
    parser.validate_statement(stmt)
    return stmt


if __name__ == "__main__":
    tests = [
        """CREATE TABLE Inventario3D (
            id INT KEY INDEX SEQUENTIAL,
            x FLOAT INDEX RTree,
            y FLOAT,
            z FLOAT,
            nombre VARCHAR[32]
        )""",

        "select * from Inventario3D",
        # SELECT =
        "select * from Inventario3D where id = 123",
        # SELECT between
        "select * from Inventario3D where nombre between 'A' and 'Z'",
        # INSERT
        "insert into Inventario3D values (1, 10.0, 20.0, 5.0, 'Caja A')",
        # Rango 2D sin radio
        "select * from Inventario3D where x in (point, [10.0, 20.0])",
        # Rango 2D con radio
        "select * from Inventario3D where x in (point, [10.0, 20.0, 3.5])",
        # Rango 3D con radio
        "select * from Inventario3D where x in (point, [10.0, 20.0, 5.0, 2.0])",
        # k-NN 2D
        "select * from Inventario3D where x in (5, [10.0, 20.0])",
        # k-NN 3D
        "select * from Inventario3D where x in (3, [10.0, 20.0, 5.0])",
    ]
    for i, q in enumerate(tests, 1):
        try:
            print(f"\n== Test {i} ==")
            print(q)
            print(parse_sql(q))
        except Exception as e:
            print("ERROR:", e)