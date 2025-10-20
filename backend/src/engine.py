from typing import Any, Dict, List, Optional, Tuple
import os

from parser_sql import parse_sql, SelectStatement, InsertStatement, DeleteStatement
from catalog import Catalog
from core.schema import Schema
from index.bptree import ClusteredIndexFile
from index.isam import ISAMFile
from index.ext_hash import ExtendibleHashing
from index.rtree_adapter import RTreeAdapter
# from sequential

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
        else:
            raise ValueError("Statement no soportado")

    def _exec_select(self, stmt: SelectStatement) -> Dict[str, Any]:
        tinfo = self.catalog.get_table(stmt.table_name)
        if not tinfo:
            raise ValueError(f"Tabla {stmt.table_name} no registrada")

        schema: Schema = tinfo["schema"]
        data_path: str = tinfo["data_path"]
        indexes: Dict[str, Dict[str, Any]] = tinfo.get("indexes", {})

        # 1) Espacial
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
            if self._has_index(indexes, col, "ExtHash"):
                return {"kind": "exthash", "col": col}
            if self._has_index(indexes, col, "BTree"):
                return {"kind": "bptree", "col": col}
            if self._has_index(indexes, col, "ISAM"):
                return {"kind": "isam", "col": col}
            return {"kind": "seq", "col": col}

        if wc["type"] in ("range", "cmp", "like"):
            col = wc["column"]
            return self._plan_range(col)

        return {"kind": "seq"}

    def _plan_range(self, col: str) -> Dict[str, Any]:
        return {"kind": "range", "col": col}

    def _has_index(self, indexes, col, kind):
        bucket = indexes.get(col, {})
        # bucket ahora es dict: {"BTREE": {...}, "ISAM": {...}, "EXTENDIBLE_HASH": {...}}
        kind_u = kind.upper()
        for k, meta in bucket.items():
            name = getattr(meta["type"], "name", k).upper()
            val  = getattr(meta["type"], "value", name).upper()
            if kind_u in ("EXTHASH","EXTENDIBLE_HASH"):
                if name=="EXTENDIBLE_HASH" or val=="EXTHASH": return True
            if kind_u=="BTREE" and "BTREE" in (name, val): return True
            if kind_u=="ISAM"  and "ISAM"  in name:        return True
        return False

    # === Ejecutores de plan ===
    def _run_plan(self, plan, stmt, data_path, schema, indexes):
        wc = stmt.where_clause

        if plan["kind"] == "seq":
            raise NotImplementedError("Consulta requiere full scan, pero el lector secuencial aún no está implementado.")

        if plan["kind"] == "exthash":
            idx_meta = self._get_index_meta(indexes, plan["col"], "exthash")
            if not idx_meta:
                return []
            eh = ExtendibleHashing(os.path.basename(idx_meta["path"]), schema, plan["col"])
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

            if self._has_index(indexes, col, "BTree"):
                idx_meta = self._get_index_meta(indexes, col, "bptree")
                bpt = ClusteredIndexFile.load(data_path, idx_meta["path"], schema)
                return bpt.range_search(lo, hi)

            if self._has_index(indexes, col, "ISAM"):
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

    def _apply_filter(self, rows: List[Dict[str, Any]], wc: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not wc: return rows
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
        for meta in bucket.values():
            t = meta["type"]
            if kind_name == "bptree"  and (t.name == "BTREE" or t.value.upper() == "BTREE"):
                return meta
            if kind_name == "isam"    and t.name == "ISAM":
                return meta
            if kind_name == "exthash" and (t.name == "EXTENDIBLE_HASH" or t.value.upper() == "EXTHASH"):
                return meta
        return None

    def _exec_insert(self, stmt: InsertStatement) -> Dict[str, Any]:
        raise NotImplementedError("INSERT aún no implementado en Engine")

    def _exec_delete(self, stmt: DeleteStatement) -> Dict[str, Any]:
        raise NotImplementedError("DELETE aún no implementado en Engine")