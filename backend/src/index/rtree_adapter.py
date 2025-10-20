from __future__ import annotations
import os, math, pickle, struct, sys
from typing import Any, Dict, List, Optional, Sequence, Tuple
from rtree import index as rtree_index
from core.schema import Schema, Field, Kind
from io_counters import count_read, count_write

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

def _to_float(v: Any) -> float:
    try: return float(v)
    except Exception: return 0.0

def _euclid(p: Sequence[float], q: Sequence[float]) -> float:
    if len(p) == 3 and len(q) == 3:
        return math.sqrt((p[0]-q[0])**2+(p[1]-q[1])**2+(p[2]-q[2])**2)
    return math.sqrt((p[0]-q[0])**2+(p[1]-q[1])**2)

def _bbox_from_point(pt: Sequence[float]) -> Tuple[float, ...]:
    if len(pt) == 3:
        x, y, z = pt
        return (x, y, z, x, y, z)
    x, y = pt[:2]
    return (x, y, x, y)

class RTreeAdapter:
    def __init__(self, base_path: str):
        self.base_path = base_path
        os.makedirs(os.path.dirname(base_path), exist_ok=True)
        self.meta_path = f"{self.base_path}.pkl"
        self.idx: Optional[rtree_index.Index] = None
        self.meta: Dict[str, Any] = {"dimension": 2, "next_id": 1, "label_to_ids": {}, "id_to_payload": {}}
        if os.path.exists(self.meta_path):
            try:
                with open(self.meta_path, "rb") as f:
                    count_read(os.path.getsize(self.meta_path))
                    self.meta = pickle.load(f)
            except Exception:
                self._reset_meta()
        self._open_index(self.meta["dimension"])

    def _reset_meta(self, dimension: int = 2):
        self.meta = {"dimension": int(dimension), "next_id": 1, "label_to_ids": {}, "id_to_payload": {}}
        self._save_meta()

    def _save_meta(self):
        with open(self.meta_path, "wb") as f:
            pickle.dump(self.meta, f, protocol=pickle.HIGHEST_PROTOCOL)
            count_write(os.path.getsize(self.meta_path))

    def _delete_index_files_only(self):
        for ext in (".dat", ".idx"):
            path = f"{self.base_path}{ext}"
            if os.path.exists(path):
                try:
                    os.remove(path)
                    count_write()
                except Exception:
                    pass

    def _open_index(self, dimension: int):
        p = rtree_index.Property()
        p.storage = rtree_index.RT_Disk
        p.index_type = rtree_index.RT_RTree
        p.dimension = int(dimension)
        self.idx = rtree_index.Index(self.base_path, properties=p)

    def _rebuild_index(self, new_dimension: int):
        self._delete_index_files_only()
        self._open_index(new_dimension)
        old_payloads = list(self.meta["id_to_payload"].items())
        self.meta["dimension"] = int(new_dimension)
        for _id, payload in old_payloads:
            pt = tuple(payload["point"])
            self.idx.insert(int(_id), _bbox_from_point(pt))
            count_write()
        self._save_meta()

    def _ensure_dimension(self, want_dim: int):
        if int(self.meta["dimension"]) != int(want_dim):
            self._rebuild_index(want_dim)

    def build_from_csv(self, rows: List[Dict[str, Any]], x_field: str, y_field: str, z_field: Optional[str] = None, label_field: Optional[str] = None, schema: Optional[Schema] = None):
        dim = 3 if z_field else 2
        self._reset_meta(dim)
        self._delete_index_files_only()
        self._open_index(dim)
        for row in rows:
            self.add(row, x_field, y_field, z_field=z_field, label_field=label_field, schema=schema)

    def add(self, row: Dict[str, Any], x_field: str, y_field: str, z_field: Optional[str] = None, label_field: Optional[str] = None, schema: Optional[Schema] = None):
        if schema is not None:
            row = schema.coerce_row(row)
        x = _to_float(row.get(x_field))
        y = _to_float(row.get(y_field))
        if z_field:
            z = _to_float(row.get(z_field))
            pt = (x, y, z); want_dim = 3
        else:
            pt = (x, y); want_dim = 2
        self._ensure_dimension(want_dim)
        label_str = str(row.get(label_field, row.get("id", "")))
        _id = int(self.meta["next_id"])
        self.meta["next_id"] = _id + 1
        self.idx.insert(_id, _bbox_from_point(pt))
        count_write()
        self.meta["id_to_payload"][_id] = {"point": pt, "row": dict(row), "label": label_str}
        self.meta["label_to_ids"].setdefault(label_str, []).append(_id)
        self._save_meta()

    def remove_by_label(self, label: Any) -> int:
        label_str = str(label)
        ids = self.meta["label_to_ids"].get(label_str, [])
        removed = 0
        for _id in list(ids):
            payload = self.meta["id_to_payload"].get(_id)
            if not payload: continue
            pt = tuple(payload["point"])
            try:
                self.idx.delete(int(_id), _bbox_from_point(pt))
                removed += 1
                count_write()
            except Exception:
                pass
            self.meta["id_to_payload"].pop(_id, None)
            try: self.meta["label_to_ids"][label_str].remove(_id)
            except ValueError: pass
        if self.meta["label_to_ids"].get(label_str) == []:
            self.meta["label_to_ids"].pop(label_str, None)
        if removed > 0:
            self._save_meta()
        return removed

    def search_by_label(self, label: Any) -> List[Dict[str, Any]]:
        label_str = str(label)
        out: List[Dict[str, Any]] = []
        for _id in self.meta["label_to_ids"].get(label_str, []):
            payload = self.meta["id_to_payload"].get(_id)
            if payload:
                count_read()
                out.append(dict(payload["row"]))
        return out

    def range(self, point: Sequence[float], radius: float) -> List[Dict[str, Any]]:
        dim = int(self.meta["dimension"])
        if dim == 3:
            x, y, z = float(point[0]), float(point[1]), float(point[2])
            query_box = (x-radius, y-radius, z-radius, x+radius, y+radius, z+radius)
            qpt = (x, y, z)
        else:
            x, y = float(point[0]), float(point[1])
            query_box = (x-radius, y-radius, x+radius, y+radius)
            qpt = (x, y)
        results: List[Tuple[float, Dict[str, Any]]] = []
        for obj in self.idx.intersection(query_box, objects=True):
            count_read()
            _id = int(obj.id)
            payload = self.meta["id_to_payload"].get(_id)
            if not payload: continue
            ppt = tuple(payload["point"])
            d = _euclid(qpt, ppt)
            if d <= radius:
                row = dict(payload["row"])
                row["_distance"] = float(round(d, 6))
                results.append((d, row))
        results.sort(key=lambda t: t[0])
        return [r for _, r in results]

    def knn(self, point: Sequence[float], k: int) -> List[Dict[str, Any]]:
        dim = int(self.meta["dimension"])
        k = max(1, int(k))
        if dim == 3:
            x, y, z = float(point[0]), float(point[1]), float(point[2])
            query_box = (x, y, z, x, y, z)
            qpt = (x, y, z)
        else:
            x, y = float(point[0]), float(point[1])
            query_box = (x, y, x, y)
            qpt = (x, y)
        out: List[Tuple[float, Dict[str, Any]]] = []
        try:
            for _id in self.idx.nearest(query_box, num_results=k):
                count_read()
                _id = int(_id)
                payload = self.meta["id_to_payload"].get(_id)
                if not payload: continue
                ppt = tuple(payload["point"])
                d = _euclid(qpt, ppt)
                row = dict(payload["row"])
                row["_distance"] = float(round(d, 6))
                out.append((d, row))
        except Exception:
            return []
        out.sort(key=lambda t: t[0])
        return [r for _, r in out]

    def count(self) -> int:
        return len(self.meta["id_to_payload"])

    def dimension(self) -> int:
        return int(self.meta["dimension"])

    def labels(self) -> List[str]:
        return list(self.meta["label_to_ids"].keys())

    def close(self):
        self._save_meta()
        count_write(os.path.getsize(self.meta_path))