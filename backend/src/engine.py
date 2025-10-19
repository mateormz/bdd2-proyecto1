from typing import Any, Dict, List, Optional, Tuple
import os
import csv

from parser_sql import (
    parse_sql, CreateTableStatement, SelectStatement, InsertStatement, DeleteStatement,
    IndexType, DataType, Column
)
from catalog import Catalog
from core.schema import Schema, Field, Kind
from index.bptree import ClusteredIndexFile
from index.isam import ISAMFile
from index.ext_hash import ExtendibleHashing
from index.rtree_adapter import RTreeAdapter


class Engine:
    def __init__(self, catalog: Catalog):
        self.catalog = catalog
        self.rtree = RTreeAdapter()

    def execute(self, sql: str) -> Dict[str, Any]:
        stmt = parse_sql(sql)
        if isinstance(stmt, SelectStatement):
            return self._exec_select(stmt)
        elif isinstance(stmt, InsertStatement):
            return self._exec_insert(stmt)
        elif isinstance(stmt, DeleteStatement):
            return self._exec_delete(stmt)
        elif isinstance(stmt, CreateTableStatement):
            return self._exec_create(stmt)
        else:
            raise ValueError("Statement no soportado")

    # ---------------- SELECT ----------------
    def _exec_select(self, stmt: SelectStatement) -> Dict[str, Any]:
        tinfo = self.catalog.get_table(stmt.table_name)
        if not tinfo:
            raise ValueError(f"Tabla {stmt.table_name} no registrada")
        schema: Schema = tinfo["schema"]
        data_path: str = tinfo["data_path"]
        indexes: Dict[str, Dict[str, Any]] = tinfo.get("indexes", {})

        if stmt.spatial_query:
            rows = self._run_spatial(stmt, data_path, schema, indexes)
            rows = self._project_columns(rows, stmt.columns)
            return {"rows": rows, "metrics": {}}

        plan = self._choose_plan(stmt, indexes)
        rows = self._run_plan(plan, stmt, data_path, schema, indexes)
        rows = self._project_columns(rows, stmt.columns)
        return {"rows": rows, "metrics": {}}

    def _project_columns(self, rows: List[Dict[str, Any]], cols: List[str]) -> List[Dict[str, Any]]:
        if cols == ["*"]:
            return rows
        return [{c: r.get(c) for c in cols} for r in rows]

    def _choose_plan(self, stmt: SelectStatement, indexes: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        wc = stmt.where_clause
        if not wc:
            return {"kind": "seq"}
        if wc["type"] == "equality":
            col = wc["column"]
            op = wc["operator"]
            if op not in ("=", "=="):
                return self._plan_range(col)
            if self._has_index(indexes, col, "exthash"):
                return {"kind": "exthash", "col": col}
            if self._has_index(indexes, col, "bptree"):
                return {"kind": "bptree", "col": col}
            if self._has_index(indexes, col, "isam"):
                return {"kind": "isam", "col": col}
            return {"kind": "seq", "col": col}
        if wc["type"] in ("range", "cmp", "like"):
            col = wc["column"]
            return self._plan_range(col)
        return {"kind": "seq"}

    def _plan_range(self, col: str) -> Dict[str, Any]:
        return {"kind": "range", "col": col}

    def _has_index(self, indexes, col, idx_kind_name: str) -> bool:
        return self._get_index_meta(indexes, col, idx_kind_name.lower()) is not None

    def _run_plan(self, plan, stmt, data_path, schema, indexes):
        wc = stmt.where_clause
        if plan["kind"] == "seq":
            raise NotImplementedError("Consulta requiere full scan, pero el lector secuencial aún no está implementado.")

        if plan["kind"] == "exthash":
            idx_meta = self._get_index_meta(indexes, plan["col"], "exthash")
            if not idx_meta:
                return []
            eh = ExtendibleHashing(idx_meta["path"], schema, plan["col"])
            key = wc["value"]
            rec = eh.search(key)
            return [rec] if rec else []

        if plan["kind"] == "bptree":
            idx_meta = self._get_index_meta(indexes, plan["col"], "bptree")
            if not idx_meta:
                return []
            bpt = ClusteredIndexFile.load(data_path, idx_meta["path"], schema)
            key = wc["value"]
            return bpt.search(key)

        if plan["kind"] == "isam":
            idx_meta = self._get_index_meta(indexes, plan["col"], "isam")
            if not idx_meta:
                return []
            isam = ISAMFile(idx_meta["path"], schema, plan["col"])
            key = int(wc["value"]) if self._is_int(schema, plan["col"]) else wc["value"]
            rec = isam.search(key)
            return [rec] if rec else []

        if plan["kind"] == "range":
            col = plan["col"]
            lo, hi = self._extract_range_bounds(wc, schema, col)
            if self._has_index(indexes, col, "bptree"):
                idx_meta = self._get_index_meta(indexes, col, "bptree")
                bpt = ClusteredIndexFile.load(data_path, idx_meta["path"], schema)
                return bpt.range_search(lo, hi)
            if self._has_index(indexes, col, "isam"):
                idx_meta = self._get_index_meta(indexes, col, "isam")
                isam = ISAMFile(idx_meta["path"], schema, col)
                lo_i = int(lo) if self._is_int(schema, col) else lo
                hi_i = int(hi) if self._is_int(schema, col) else hi
                return isam.rangeSearch(lo_i, hi_i)
            raise NotImplementedError("Rango sin índice (B+Tree/ISAM) requiere scan secuencial, aún no implementado.")

        raise ValueError("Plan desconocido")

    def _run_spatial(self, stmt: SelectStatement, data_path: str,
                     schema: Schema, indexes: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
        q = stmt.spatial_query
        if q["type"] == "spatial_range":
            point = q["point"]; radio = q["radio"]
            return self.rtree.rangeSearch(tuple(point), radio)
        if q["type"] == "spatial_knn":
            point = q["point"]; k = q["k"]
            return self.rtree.kNN(tuple(point), k)
        return []

    # ---------------- CREATE ----------------
    def _exec_create(self, stmt: CreateTableStatement) -> Dict[str, Any]:
        repo = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        out_dir = os.path.join(repo, "out")
        os.makedirs(out_dir, exist_ok=True)

        # CREATE TABLE ... FROM FILE "..." USING INDEX <TYPE>(col)
        if stmt.from_file:
            table = stmt.table_name
            tinfo = self.catalog.get_table(table)
            if not tinfo:
                raise ValueError("Schema no registrado para la tabla. Regístralo en el Catálogo antes del CREATE FROM FILE.")
            schema: Schema = tinfo["schema"]
            data_path = tinfo["data_path"] or os.path.join(out_dir, f"{table}.dat")

            idx_type, col = stmt.using_index
            if idx_type == IndexType.BTREE:
                idx_path = os.path.join(out_dir, f"{table}_bptree.idx")
                c = ClusteredIndexFile(data_path, idx_path, schema, col, self._infer_kind(schema, col))
                c.build_from_csv(stmt.from_file)
                self.catalog.register_table(table, schema, data_path)
                self.catalog.register_index(table, col, IndexType.BTREE, idx_path)
            elif idx_type == IndexType.ISAM:
                idx_path = os.path.join(out_dir, f"{table}_isam.dat")
                isam = ISAMFile(idx_path, schema, col)
                isam.build_from_csv(stmt.from_file)
                self.catalog.register_table(table, schema, data_path)
                self.catalog.register_index(table, col, IndexType.ISAM, idx_path)
            elif idx_type == IndexType.EXTENDIBLE_HASH:
                idx_path = os.path.join(out_dir, f"{table}_exthash.dat")
                eh = ExtendibleHashing(idx_path, schema, key_field=col)
                with open(stmt.from_file, newline='', encoding="utf-8") as f:
                    r = csv.DictReader(f)
                    for row in r:
                        if "deleted" in [fld.name for fld in schema.fields] and "deleted" not in row:
                            row["deleted"] = 0
                        eh.insert(row)
                self.catalog.register_table(table, schema, data_path)
                self.catalog.register_index(table, col, IndexType.EXTENDIBLE_HASH, idx_path)
            else:
                raise ValueError(f"Índice no soportado en CREATE FROM FILE: {idx_type}")
            return {"ok": True, "action": "create_from_file", "table": table}

        # CREATE TABLE ... (schema)
        table = stmt.table_name
        schema = self._build_schema_from_columns(stmt.columns)
        data_path = os.path.join(out_dir, f"{table}.dat")
        self.catalog.register_table(table, schema, data_path)

        # Inicializa índices vacíos si se declararon
        for col in stmt.columns:
            if col.index_type == IndexType.BTREE:
                idx_path = os.path.join(out_dir, f"{table}_{col.name}_bptree.idx")
                c = ClusteredIndexFile(data_path, idx_path, schema, col.name, self._infer_kind(schema, col.name))
                # crea pickle vacío
                if not os.path.exists(data_path):
                    open(data_path, "ab").close()
                c._rebuild_index()
                self.catalog.register_index(table, col.name, IndexType.BTREE, idx_path)
            elif col.index_type == IndexType.EXTENDIBLE_HASH:
                idx_path = os.path.join(out_dir, f"{table}_{col.name}_exthash.dat")
                ExtendibleHashing(idx_path, schema, key_field=col.name)
                self.catalog.register_index(table, col.name, IndexType.EXTENDIBLE_HASH, idx_path)
            elif col.index_type == IndexType.ISAM:
                # ISAM vacío no se inicializa aquí
                pass

        return {"ok": True, "action": "create_schema", "table": table}

    # ---------------- INSERT ----------------
    def _exec_insert(self, stmt: InsertStatement) -> Dict[str, Any]:
        tinfo = self.catalog.get_table(stmt.table_name)
        if not tinfo:
            raise ValueError(f"Tabla {stmt.table_name} no registrada")
        schema: Schema = tinfo["schema"]
        data_path: str = tinfo["data_path"]
        indexes = tinfo.get("indexes", {})

        row: Dict[str, Any] = {}
        for f, v in zip(schema.fields, stmt.values):
            row[f.name] = v
        if "deleted" in [fld.name for fld in schema.fields] and row.get("deleted") is None:
            row["deleted"] = 0

        # ExtHash (si existe)
        for colname in indexes.keys():
            meta = self._get_index_meta(indexes, colname, "exthash")
            if meta:
                eh = ExtendibleHashing(meta["path"], schema, key_field=colname)
                eh.insert(row)

        # B+Tree (clustered)
        for colname in indexes.keys():
            meta = self._get_index_meta(indexes, colname, "bptree")
            if meta:
                bpt = ClusteredIndexFile.load(data_path, meta["path"], schema)
                bpt.insert(row)

        # ISAM (si existe)
        for colname in indexes.keys():
            meta = self._get_index_meta(indexes, colname, "isam")
            if meta:
                isam = ISAMFile(meta["path"], schema, key_field=colname)
                isam.insert(row)

        return {"ok": True, "table": stmt.table_name}

    # ---------------- DELETE ----------------
    def _exec_delete(self, stmt: DeleteStatement) -> Dict[str, Any]:
        tinfo = self.catalog.get_table(stmt.table_name)
        if not tinfo:
            raise ValueError(f"Tabla {stmt.table_name} no registrada")
        schema: Schema = tinfo["schema"]
        data_path: str = tinfo["data_path"]
        indexes = tinfo.get("indexes", {})

        wc = stmt.where_clause
        if not wc or wc["type"] != "equality":
            raise NotImplementedError("DELETE soporta solo igualdad por clave/columna indexada")
        col = wc["column"]; key = wc["value"]
        removed = 0

        meta = self._get_index_meta(indexes, col, "exthash")
        if meta:
            eh = ExtendibleHashing(meta["path"], schema, key_field=col)
            if eh.remove(key):
                removed += 1

        meta = self._get_index_meta(indexes, col, "bptree")
        if meta:
            bpt = ClusteredIndexFile.load(data_path, meta["path"], schema)
            removed += bpt.remove(str(key))

        meta = self._get_index_meta(indexes, col, "isam")
        if meta:
            isam = ISAMFile(meta["path"], schema, key_field=col)
            ok = isam.delete(int(key) if self._is_int(schema, col) else key)
            if ok:
                removed += 1

        return {"ok": True, "removed": removed}

    # ---------------- helpers ----------------
    def _apply_filter(self, rows: List[Dict[str, Any]], wc: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not wc:
            return rows
        t = wc["type"]; c = wc["column"]
        if t == "equality":
            op = wc["operator"]; v = wc["value"]
            if op in ("=", "=="): return [r for r in rows if r.get(c) == v]
            if op == "!=":        return [r for r in rows if r.get(c) != v]
            if op == "<":         return [r for r in rows if r.get(c) < v]
            if op == "<=":        return [r for r in rows if r.get(c) <= v]
            if op == ">":         return [r for r in rows if r.get(c) > v]
            if op == ">=":        return [r for r in rows if r.get(c) >= v]
        if t == "range":
            a, b = wc["start"], wc["end"]
            return [r for r in rows if a <= r.get(c) <= b]
        return rows

    def _extract_range_bounds(self, wc: Dict[str, Any], schema: Schema, col: str) -> Tuple[Any, Any]:
        if wc["type"] == "range":
            return wc["start"], wc["end"]
        v = wc.get("value"); op = wc.get("operator")
        if op in ("<", "<="): return (self._min_of(schema, col), v)
        if op in (">", ">="): return (v, self._max_of(schema, col))
        raise ValueError("Rango no reconocible")

    def _infer_kind(self, schema: Schema, col: str):
        f = next(f for f in schema.fields if f.name == col)
        return f.kind

    def _is_int(self, schema: Schema, col: str) -> bool:
        f = next(f for f in schema.fields if f.name == col)
        return f.kind.name == "INT"

    def _min_of(self, schema: Schema, col: str):
        return "" if not self._is_int(schema, col) else -2**31

    def _max_of(self, schema: Schema, col: str):
        return "\uffff" if not self._is_int(schema, col) else 2**31-1

    def _get_index_meta(self, indexes, col, kind_name):
        bucket = indexes.get(col, {})
        if "type" in bucket and "path" in bucket:
            t = bucket["type"]
            name = getattr(t, "name", str(t)).upper()
            val = getattr(t, "value", name).upper()
            if kind_name == "bptree"  and ("BTREE" in (name, val)):                 return bucket
            if kind_name == "isam"    and ("ISAM"  in  name):                       return bucket
            if kind_name == "exthash" and (name=="EXTENDIBLE_HASH" or val=="EXTHASH"):
                return bucket
            return None
        for meta in bucket.values():
            t = meta["type"]
            name = getattr(t, "name", str(t)).upper()
            val  = getattr(t, "value", name).upper()
            if kind_name == "bptree"  and ("BTREE" in (name, val)):                 return meta
            if kind_name == "isam"    and ("ISAM"  in  name):                       return meta
            if kind_name == "exthash" and (name=="EXTENDIBLE_HASH" or val=="EXTHASH"):
                return meta
        return None

    def _build_schema_from_columns(self, cols: List[Column]) -> Schema:
        fields: List[Field] = []
        for c in cols:
            if c.data_type == DataType.INT:
                fields.append(Field(c.name, Kind.INT, fmt="i"))
            elif c.data_type == DataType.FLOAT:
                fields.append(Field(c.name, Kind.FLOAT, fmt="f"))
            elif c.data_type == DataType.VARCHAR:
                if not c.size:
                    raise ValueError(f"VARCHAR requiere tamaño en {c.name}")
                fields.append(Field(c.name, Kind.CHAR, size=c.size))
            elif c.data_type == DataType.DATE:
                fields.append(Field(c.name, Kind.DATE))
            elif c.data_type == DataType.ARRAY:
                raise NotImplementedError("ARRAY no soportado en Schema binario actual")
            else:
                raise ValueError(f"Tipo no soportado: {c.data_type}")
        return Schema(fields)