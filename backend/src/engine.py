from __future__ import annotations
import os, csv
from typing import Any, Dict, List, Iterable, Optional, Tuple
from dataclasses import dataclass

from core.schema import Schema, Field, Kind
from parser_sql import (
    ParserSQL, CreateTableStatement, SelectStatement,
    InsertStatement, DeleteStatement, IndexType, DataType, Column
)
from catalog import Catalog

from index.isam import ISAMFile
from index.sequential import SequentialOrderedFile
from index.ext_hash import ExtendibleHashing
from index.rtree_adapter import RTreeAdapter
from index.bptree import BPlusClusteredFile

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
OUT_DIR = os.path.join(ROOT, "out")
os.makedirs(OUT_DIR, exist_ok=True)


def _schema_from_columns(cols: List[Column]) -> Schema:
    fields: List[Field] = []
    for c in cols:
        if c.data_type == DataType.INT:
            fields.append(Field(c.name, Kind.INT, fmt="i"))
        elif c.data_type == DataType.FLOAT:
            fields.append(Field(c.name, Kind.FLOAT, fmt="f"))
        elif c.data_type == DataType.VARCHAR:
            if c.size is None or int(c.size) <= 0:
                raise ValueError(f"VARCHAR de la columna '{c.name}' requiere tamaño positivo.")
            fields.append(Field(c.name, Kind.CHAR, size=int(c.size)))
        elif c.data_type == DataType.DATE:
            fields.append(Field(c.name, Kind.DATE))
        elif c.data_type == DataType.ARRAY:
            raise ValueError("ARRAY requiere una representación fija. Usa VARCHAR[n] con tu formato serializado.")
    return Schema(fields)

def _load_csv_rows(path: str) -> List[Dict[str, Any]]:
    with open(path, newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        return list(rdr)

def _coerce_to_schema(schema: Schema, row: Dict[str, Any]) -> Dict[str, Any]:
    return schema.coerce_row(row)

def _ensure_isam_storage_schema(user_schema: Schema) -> Schema:
    if any(f.name == user_schema.deleted_name for f in user_schema.fields):
        return user_schema
    new_fields = list(user_schema.fields) + [Field(user_schema.deleted_name, Kind.INT, fmt="B")]
    return Schema(new_fields, deleted_name=user_schema.deleted_name)


class BaseAdapter:
    def build_from_csv(self, csv_path: str): ...
    def search(self, key: Any) -> List[Dict[str, Any]]: ...
    def range_search(self, a: Any, b: Any) -> Iterable[Dict[str, Any]]: ...
    def add(self, rec: Dict[str, Any]): ...
    def remove(self, key: Any) -> int: ...


class ISAMAdapter(BaseAdapter):
    def __init__(self, base_name: str, schema: Schema, key_field: str):
        self.schema = schema; self.key = key_field
        base = os.path.join(OUT_DIR, f"{base_name}.dat")
        self.idx = ISAMFile(base, schema, key_field)

    def build_from_csv(self, csv_path: str):
        self.idx.build_from_csv(csv_path, delimiter=",")

    def search(self, key: Any) -> List[Dict[str, Any]]:
        r = self.idx.search(key)
        return [r] if r else []

    def range_search(self, a: Any, b: Any) -> Iterable[Dict[str, Any]]:
        return self.idx.rangeSearch(a, b)

    def add(self, rec: Dict[str, Any]):
        self.idx.insert(_coerce_to_schema(self.schema, rec))

    def remove(self, key: Any) -> int:
        return 1 if self.idx.delete(key) else 0

    def scan(self, limit: int = 200) -> List[Dict[str, Any]]:
        out = []
        with open(self.idx.filename, 'rb') as f:
            f.seek(0, os.SEEK_END)
            total_size = f.tell()
        total_pages = total_size // self.idx.page_size

        for pidx in range(total_pages):
            page = self.idx._read_data_page(pidx)
            for rec in page.records:
                if rec.get(self.schema.deleted_name, 0) == 0:
                    out.append(rec)
                    if len(out) >= limit: return out
            nxt = page.next_overflow
            while nxt != -1:
                page = self.idx._read_data_page(nxt)
                for rec in page.records:
                    if rec.get(self.schema.deleted_name, 0) == 0:
                        out.append(rec)
                        if len(out) >= limit: return out
                nxt = page.next_overflow
        return out

class SequentialAdapter(BaseAdapter):
    def __init__(self, base_name: str, schema: Schema, key_field: str):
        self.schema = schema; self.key = key_field
        self.seq = SequentialOrderedFile(os.path.join(OUT_DIR, base_name), schema, key_field)

    def build_from_csv(self, csv_path: str):
        rows = [_coerce_to_schema(self.schema, r) for r in _load_csv_rows(csv_path)]
        self.seq.bulk_load(rows)

    def search(self, key: Any) -> List[Dict[str, Any]]:
        return self.seq.search(key)

    def range_search(self, a: Any, b: Any) -> Iterable[Dict[str, Any]]:
        lo, hi = (a, b) if a <= b else (b, a)
        return self.seq.range_search(lo, hi)

    def add(self, rec: Dict[str, Any]):
        self.seq.add(_coerce_to_schema(self.schema, rec))

    def remove(self, key: Any) -> int:
        return self.seq.remove(key)

    def scan(self, limit: int = 200) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        entries = self.seq.sparse.read()
        for pid in range(len(entries)):
            page = self.seq._read_page(pid)
            for r in page.records:
                rows.append(r)
                if len(rows) >= limit:
                    return rows
        return rows

class ExtHashAdapter(BaseAdapter):
    def __init__(self, base_name: str, schema: Schema, key_field: str):
        self.schema = schema; self.key = key_field
        self.eh = ExtendibleHashing(f"{base_name}_ext_hash", schema, key_field)

    def build_from_csv(self, csv_path: str):
        for r in _load_csv_rows(csv_path):
            self.add(r)

    def search(self, key: Any) -> List[Dict[str, Any]]:
        r = self.eh.search(key)
        return [r] if r else []

    def add(self, rec: Dict[str, Any]):
        self.eh.insert(_coerce_to_schema(self.schema, rec))

    def remove(self, key: Any) -> int:
        return 1 if self.eh.remove(key) else 0

    def range_search(self, a: Any, b: Any) -> Iterable[Dict[str, Any]]:
        raise NotImplementedError("Extendible Hashing no soporta range_search")

    def scan(self, limit: int = 200) -> List[Dict[str, Any]]:
        out = []
        for r in self.eh.iter_all():
            out.append(r)
            if len(out) >= limit:
                break
        return out

class BPTreeAdapter(BaseAdapter):
    def __init__(self, base_name: str, schema: Schema, key_field: str):
        self.schema = schema; self.key = key_field
        self.path = os.path.join(OUT_DIR, f"{base_name}_bptree.dat")
        self.bpt = BPlusClusteredFile(self.path, schema, key_field)

    def build_from_csv(self, csv_path: str):
        rows = _load_csv_rows(csv_path)
        for r in rows:
            self.add(r)

    def search(self, key: Any) -> List[Dict[str, Any]]:
        return self.bpt.search(key)

    def range_search(self, a: Any, b: Any) -> Iterable[Dict[str, Any]]:
        return self.bpt.range_search(a, b)

    def add(self, rec: Dict[str, Any]):
        self.bpt.insert(_coerce_to_schema(self.schema, rec))

    def remove(self, key: Any) -> int:
        return self.bpt.remove(key, only_first=True)

    def scan(self, limit: int = 200) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for r in self.bpt.iter_all():
            out.append(r)
            if len(out) >= limit: break
        return out

class RTreeIdxAdapter(BaseAdapter):
    def __init__(self, base_name: str, schema: Schema, key_field: str, x="x", y="y", z="z", label_field=None):
        self.schema = schema
        self.key = key_field
        self.x, self.y, self.z = x, y, z
        self.label = label_field or key_field
        self.rt = RTreeAdapter(os.path.join(OUT_DIR, f"{base_name}_rtree"))

    def build_from_csv(self, csv_path: str):
        rows = _load_csv_rows(csv_path)
        self.rt.build_from_csv(rows, self.x, self.y, self.z, label_field=self.label, schema=self.schema)

    def search(self, key: Any) -> List[Dict[str, Any]]:
        return self.rt.search_by_label(key)

    def add(self, rec: Dict[str, Any]):
        self.rt.add(rec, self.x, self.y, self.z, label_field=self.label, schema=self.schema)

    def remove(self, key: Any) -> int:
        return self.rt.remove_by_label(key)

    def range_search(self, a: Any, b: Any) -> Iterable[Dict[str, Any]]:
        return []

    def spatial_range(self, point: Tuple[float, ...], radius: float) -> List[Dict[str, Any]]:
        return self.rt.range(point, radius)

    def knn(self, point: Tuple[float, ...], k: int) -> List[Dict[str, Any]]:
        return self.rt.knn(point, k)


@dataclass
class TableHandle:
    name: str
    schema: Schema
    key: Optional[str]
    adapter: BaseAdapter
    idx_type: IndexType

class Engine:
    def __init__(self):
        self.parser = ParserSQL()
        self.catalog = Catalog()
        self.tables: Dict[str, TableHandle] = {}

    def execute(self, sql: str) -> Dict[str, Any]:
        st = self.parser.parse(sql)
        self.parser.validate_statement(st)

        if isinstance(st, CreateTableStatement):
            return self._exec_create(st)
        if isinstance(st, SelectStatement):
            return self._exec_select(st)
        if isinstance(st, InsertStatement):
            return self._exec_insert(st)
        if isinstance(st, DeleteStatement):
            return self._exec_delete(st)
        return {"status": "error", "message": "Sentencia no soportada"}

    def _exec_create(self, st: CreateTableStatement) -> Dict[str, Any]:
        if not st.from_file:
            schema = _schema_from_columns(st.columns)
            key_col = next((c.name for c in st.columns if c.is_key), None)
            idx_type = next((c.index_type for c in st.columns if c.index_type), IndexType.SEQUENTIAL)
            adapter = self._make_adapter(st.table_name, schema, key_col or st.columns[0].name, idx_type)

            self.catalog.register_table(
                st.table_name,
                {"columns": [f.name for f in schema.fields], "key": key_col},
                os.path.join(OUT_DIR, f"{st.table_name}.dat")
            )
            if key_col:
                self.catalog.register_index(
                    st.table_name, key_col, idx_type, os.path.join(OUT_DIR, f"{st.table_name}.dat")
                )

            self.tables[st.table_name] = TableHandle(st.table_name, schema, key_col, adapter, idx_type)
            return {"status": "ok", "message": f"Tabla '{st.table_name}' creada con índice {idx_type.value}"}

        (idx_type, key_col) = st.using_index

        if st.table_name not in self.tables:
            return {"status": "error", "message": "Debe crear primero la tabla con columnas (CREATE TABLE ...)"}

        th = self.tables[st.table_name]

        if key_col not in [f.name for f in th.schema.fields]:
            return {"status": "error",
                    "message": f"La columna índice '{key_col}' no existe en la tabla '{st.table_name}'"}

        if th.idx_type != idx_type or th.key != key_col:
            th.adapter = self._make_adapter(st.table_name, th.schema, key_col, idx_type)
            th.idx_type = idx_type
            th.key = key_col

        th.adapter.build_from_csv(st.from_file)

        self.catalog.register_index(
            st.table_name, key_col, idx_type, os.path.join(OUT_DIR, f"{st.table_name}.dat")
        )
        return {"status": "ok", "message": f"Datos cargados desde CSV con índice {idx_type.value}"}

    def _make_adapter(self, table: str, schema: Schema, key_col: str, idx_type: IndexType) -> BaseAdapter:
        base = table.lower()

        idx_name = getattr(idx_type, "name", None) or str(idx_type)
        idx_name = idx_name.upper()

        if idx_name == "SEQUENTIAL":
            return SequentialAdapter(base, schema, key_col)

        if idx_name == "ISAM":
            storage_schema = _ensure_isam_storage_schema(schema)
            return ISAMAdapter(base, storage_schema, key_col)

        if idx_name in ("EXTENDIBLE_HASH", "EXTHASH"):
            return ExtHashAdapter(base, schema, key_col)

        if idx_name in ("BPTREE", "BPTREE_CLUSTERED"):
            return BPTreeAdapter(base, schema, key_col)

        if idx_name == "RTREE":
            return RTreeIdxAdapter(base, schema, key_col, x="x", y="y", z="z", label_field=key_col)

        raise ValueError(f"Índice no soportado: {idx_type}")

    def _exec_select(self, st: SelectStatement) -> Dict[str, Any]:
        th = self._need(st.table_name)

        if not st.where_clause and not st.spatial_query:
            if hasattr(th.adapter, "scan"):
                rows = th.adapter.scan(limit=200)
                return {"status": "ok", "rows": rows}
            return {"status": "ok", "rows": []}

        if st.spatial_query:
            if not isinstance(th.adapter, RTreeIdxAdapter):
                return {"status": "error", "message": "La consulta espacial requiere índice RTree"}
            sq = st.spatial_query
            if sq["type"] == "spatial_range":
                # point puede ser de 2 o 3 dimensiones; radius puede ser None → 0
                pt_vals = [float(v) for v in sq["point"]]
                rad = float(sq["radio"] or 0)
                rows = th.adapter.spatial_range(tuple(pt_vals), rad)
                return {"status": "ok", "rows": rows}
            else:
                pt_vals = [float(v) for v in sq["point"]]
                k = int(sq["k"])
                rows = th.adapter.knn(tuple(pt_vals), k)
                return {"status": "ok", "rows": rows}


        if not st.where_clause:
            return {"status": "ok", "rows": []}

        wc = st.where_clause
        if wc["type"] == "equality":
            rows = th.adapter.search(wc["value"])
            return {"status": "ok", "rows": rows}
        elif wc["type"] == "range":
            if hasattr(th.adapter, "range_search"):
                try:
                    rows = list(th.adapter.range_search(wc["start"], wc["end"]))
                    return {"status": "ok", "rows": rows}
                except NotImplementedError:
                    return {"status": "error", "message": "El índice no soporta range_search"}
        return {"status": "error", "message": "WHERE no soportado"}

    def _exec_insert(self, st: InsertStatement) -> Dict[str, Any]:
        th = self._need(st.table_name)
        user_cols = [f for f in th.schema.fields if f.name != th.schema.deleted_name]
        if len(st.values) != len(user_cols):
            return {"status": "error",
                    "message": f"INSERT esperaba {len(user_cols)} valores (sin incluir '{th.schema.deleted_name}')"}

        row = {f.name: v for f, v in zip(user_cols, st.values)}
        if any(f.name == th.schema.deleted_name for f in th.schema.fields):
            row[th.schema.deleted_name] = 0

        th.adapter.add(row)
        return {"status": "ok", "rows_affected": 1}

    def _exec_delete(self, st: DeleteStatement) -> Dict[str, Any]:
        th = self._need(st.table_name)
        wc = st.where_clause
        if wc["type"] != "equality":
            return {"status": "error", "message": "DELETE sólo soporta igualdad por ahora"}
        n = th.adapter.remove(wc["value"])
        return {"status": "ok", "rows_affected": int(n)}

    def _need(self, table: str) -> TableHandle:
        if table not in self.tables:
            raise ValueError(f"Tabla no registrada: {table}")
        return self.tables[table]


# Pequeña prueba CLI / REPL interactivo
if __name__ == "__main__":
    import sys

    def _read_stmt(prompt: str, require_create: bool = False) -> str:
        """
        Lee varias líneas hasta:
          - balance de paréntesis cerrado (útil para CREATE TABLE (...))
          - o aparición de ';' fuera de comillas
        """
        lines = []
        depth = 0
        while True:
            line = input(prompt if not lines else "... ").rstrip()
            if not lines and require_create:
                head = line.strip().upper()
                if not head.startswith("CREATE TABLE"):
                    print(" Primero debes ejecutar un CREATE TABLE (con columnas y tamaños), sin FROM FILE.")
                    lines.clear()
                    depth = 0
                    continue

            lines.append(line)
            depth += line.count("(") - line.count(")")

            trimmed = line.strip()
            if trimmed.endswith(";"):
                break
            if depth <= 0 and (require_create and trimmed.endswith(")")):
                break

        sql = "\n".join(lines)
        if sql.strip().endswith(";"):
            sql = sql.rstrip().rstrip(";")
        return sql

    eng = Engine()
    print("\n=== Mini-DB Interactivo ===")
    print("Reglas:")
    print("  - Primero DEBES crear la tabla con tamaños fijos: CREATE TABLE ...")
    print("  - Recién después puedes cargar un CSV con: CREATE TABLE ... FROM FILE ... USING INDEX ...")
    print("  - Índices soportados: SEQUENTIAL (default), ISAM, EXTHASH, BPTREE, RTREE")
    print("  - Comandos especiales: :help  :tables  :exit\n")

    # 1) Forzar que el usuario empiece con CREATE TABLE
    table_name = None
    while True:
        try:
            sql = _read_stmt("SQL (debe ser CREATE TABLE ...): ", require_create=True)
        except (EOFError, KeyboardInterrupt):
            print("\nSaliendo.")
            sys.exit(0)

        if sql.lower() in (":exit", "exit", "quit"):
            print("Saliendo.")
            sys.exit(0)
        if sql.lower() in (":help", "help"):
            print("Ejemplo CREATE:\n"
                  "  CREATE TABLE Inventario3D(\n"
                  "    BoxID INT KEY INDEX RTREE,\n"
                  "    x FLOAT, y FLOAT, z FLOAT,\n"
                  "    Producto VARCHAR[40]\n"
                  "  )\n")
            continue

        try:
            st = eng.parser.parse(sql)
            eng.parser.validate_statement(st)
            if not isinstance(st, CreateTableStatement) or st.from_file:
                print("Primero un CREATE TABLE (con columnas/tamaños), sin FROM FILE.")
                continue
            res = eng.execute(sql)
            print(res)
            table_name = st.table_name
            break
        except Exception as e:
            print("ERROR:", e)

    # REPL libre
    print("\n=== REPL SQL ===")
    print("Escribe SQL; usa :tables para ver tablas/index; :help para ayuda; :exit para salir.")
    while True:
        try:
            line = _read_stmt("SQL> ")
        except (EOFError, KeyboardInterrupt):
            print("\nSaliendo.")
            break

        if not line:
            continue
        cmd = line.lower()

        if cmd in (":exit", "exit", "quit"):
            print("Saliendo.")
            break
        if cmd in (":help", "help"):
            print("Ejemplos:\n"
                  "  SELECT * FROM MiTabla WHERE id = 123;\n"
                  "  SELECT * FROM MiTabla WHERE id BETWEEN 10 AND 20;\n"
                  "  INSERT INTO MiTabla VALUES (...);\n"
                  "  DELETE FROM MiTabla WHERE id = 1;\n"
                  "Comandos:\n"
                  "  :tables  -> listar tablas registradas\n"
                  "  :exit    -> salir\n")
            continue
        if cmd == ":tables":
            if not eng.tables:
                print("(sin tablas)")
            else:
                for name, th in eng.tables.items():
                    print(f"- {name}: key={th.key}, idx={th.idx_type.name}, cols={[f.name for f in th.schema.fields]}")
            continue

        try:
            print(eng.execute(line))
        except Exception as e:
            print("ERROR:", e)