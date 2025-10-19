# B+ Tree Clustered File - Index

from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple, Callable
import os, csv, struct, bisect, pickle, math, re


PAGE_SIZE   = 4096
MAGIC       = b"BPTCFS1\0"
VERSION     = 1
ORDER_HINT  = 64
NUM_FMT     = "<"

def _fixs(s: str, L: int) -> bytes:
    return (str(s) if s is not None else "").encode("utf-8", errors="ignore")[:L].ljust(L, b" ")

def _unfixs(b: bytes) -> str:
    return b.decode("utf-8", errors="ignore").rstrip()

def _looks_int(x: str) -> bool:
    try:
        if x.strip() == "": return False
        f = float(x)
        return f.is_integer()
    except: return False

def _looks_float(x: str) -> bool:
    try:
        if x.strip() == "": return False
        float(x)
        return True
    except: return False

ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


@dataclass
class ColSpec:
    name: str
    kind: str
    width: int = 0

class Schema:
    def __init__(self, cols: List[ColSpec]):
        self.cols = cols
        fmt_parts = [NUM_FMT]
        for c in cols:
            if c.kind == "int":   fmt_parts.append("q")
            elif c.kind == "float": fmt_parts.append("d")
            else:                 fmt_parts.append(f"{c.width}s")
        self.struct_fmt = "".join(fmt_parts)
        self.record_size = struct.calcsize(self.struct_fmt)

    def pack_row(self, row: Dict[str, Any]) -> bytes:
        values: List[Any] = []
        for c in self.cols:
            v = row.get(c.name, "")
            if c.kind == "int":
                try: values.append(int(float(v)))
                except: values.append(0)
            elif c.kind == "float":
                try: values.append(float(v))
                except: values.append(0.0)
            else:
                values.append(_fixs("" if v is None else str(v), c.width))
        return struct.pack(self.struct_fmt, *values)

    def unpack_row(self, buf: bytes) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        vals = struct.unpack(self.struct_fmt, buf)
        vi = 0
        for c in self.cols:
            v = vals[vi]; vi += 1
            if c.kind == "int":
                out[c.name] = int(v)
            elif c.kind == "float":
                out[c.name] = float(v)
            else:
                out[c.name] = _unfixs(v)
        return out

def infer_schema(rows: List[Dict[str, str]]) -> Schema:
    if not rows:
        raise ValueError("CSV sin filas, no se puede inferir esquema.")
    headers = list(rows[0].keys())

    cols: List[ColSpec] = []
    for h in headers:
        vals = [r.get(h, "") for r in rows]
        non_empty = [v for v in vals if str(v).strip() != ""]
        col_kind = "str"
        if non_empty and all(_looks_int(v) for v in non_empty):
            col_kind = "int"
        elif non_empty and all(_looks_float(v) for v in non_empty):
            col_kind = "float"
        else:
            col_kind = "str"

        if col_kind == "str":
            max_len = max((len(str(v)) for v in vals), default=0)
            if all((v == "" or ISO_DATE_RE.match(str(v))) for v in non_empty) and max_len <= 10:
                L = 10
            else:
                candidates = [16, 24, 32, 40, 48, 64, 96, 128, 256]
                L = next((c for c in candidates if c >= max_len), 256)
            cols.append(ColSpec(h, "str", L))
        else:
            cols.append(ColSpec(h, col_kind, 0))

    return Schema(cols)

class IOStats:
    def __init__(self): self.reads=0; self.writes=0
    def reset(self): self.reads=0; self.writes=0
IO = IOStats()

class Disk:
    HEADER_FMT  = "<8sIQQQ"
    HEADER_SIZE = struct.calcsize(HEADER_FMT)

    def __init__(self, filename: str):
        create = not os.path.exists(filename) or os.path.getsize(filename) == 0
        self.f = open(filename, "r+b" if not create else "w+b")
        if create:
            self.root_pid = 0
            self.free_head = 0
            self.num_pages = 1
            self._write_header()
        else:
            self._read_header()

    def _write_header(self):
        self.f.seek(0)
        hdr = struct.pack(self.HEADER_FMT, MAGIC, VERSION, self.root_pid, self.free_head, self.num_pages)
        self.f.write(hdr + (b"\x00" * (PAGE_SIZE - len(hdr))))
        IO.writes += 1

    def _read_header(self):
        self.f.seek(0)
        raw = self.f.read(PAGE_SIZE); IO.reads += 1
        magic, version, self.root_pid, self.free_head, self.num_pages = struct.unpack(self.HEADER_FMT, raw[:self.HEADER_SIZE])
        if magic != MAGIC:
            raise ValueError("Archivo no es B+ Clustered válido")

    def alloc(self) -> int:
        if self.free_head != 0:
            pid = self.free_head
            self.f.seek(pid * PAGE_SIZE)
            raw = self.f.read(PAGE_SIZE); IO.reads += 1
            next_free = struct.unpack("<Q", raw[:8])[0]
            self.free_head = next_free
            self._write_header()
            return pid
        pid = self.num_pages
        self.write_raw(pid, b"\x00")
        self.num_pages += 1
        self._write_header()
        return pid

    def free(self, pid: int):
        buf = struct.pack("<Q", self.free_head)
        self.write_raw(pid, buf)
        self.free_head = pid
        self._write_header()

    def write_raw(self, pid: int, buf: bytes):
        if len(buf) > PAGE_SIZE: raise ValueError("Página excede PAGE_SIZE")
        self.f.seek(pid * PAGE_SIZE)
        self.f.write(buf + (b"\x00" * (PAGE_SIZE - len(buf))))
        IO.writes += 1

    def read_raw(self, pid: int) -> bytes:
        self.f.seek(pid * PAGE_SIZE)
        raw = self.f.read(PAGE_SIZE); IO.reads += 1
        return raw

    def set_root(self, pid: int):
        self.root_pid = pid
        self._write_header()

    def close(self):
        self.f.flush(); self.f.close()



LEAF_HDR_FMT  = "<ciii"
LEAF_HDR_SIZE = struct.calcsize(LEAF_HDR_FMT)

class LeafPage:
    def __init__(self, schema: Schema, block_factor: int,
                 recs: Optional[List[Dict[str, Any]]] = None,
                 prev_leaf: int = 0, next_leaf: int = 0):
        self.schema = schema
        self.block_factor = block_factor
        self.recs: List[Dict[str, Any]] = [] if recs is None else recs
        self.prev_leaf = prev_leaf
        self.next_leaf = next_leaf

    def pack(self) -> bytes:
        if len(self.recs) > self.block_factor:
            raise ValueError("Overflow en pack()")
        out = [struct.pack(LEAF_HDR_FMT, b"L", len(self.recs), self.prev_leaf, self.next_leaf)]
        for r in self.recs:
            out.append(self.schema.pack_row(r))
        if len(self.recs) < self.block_factor:
            out.append(b"\x00" * ((self.block_factor - len(self.recs)) * self.schema.record_size))
        return b"".join(out)

    @staticmethod
    def unpack(buf: bytes, schema: Schema, block_factor: int) -> "LeafPage":
        tag, count, prev_leaf, next_leaf = struct.unpack(LEAF_HDR_FMT, buf[:LEAF_HDR_SIZE])
        assert tag == b"L", "Página no es hoja"
        recs: List[Dict[str, Any]] = []
        off = LEAF_HDR_SIZE
        for _ in range(count):
            recs.append(schema.unpack_row(buf[off:off+schema.record_size]))
            off += schema.record_size
        return LeafPage(schema, block_factor, recs, prev_leaf, next_leaf)

@dataclass
class InternalNode:
    is_leaf: bool
    keys: List[Any]
    children: List[int]

def write_internal(dsk: Disk, pid: int, node: InternalNode):
    data = pickle.dumps({"is_leaf": False, "keys": node.keys, "children": node.children},
                        protocol=pickle.HIGHEST_PROTOCOL)
    dsk.write_raw(pid, data)

def read_internal(dsk: Disk, pid: int) -> InternalNode:
    raw = dsk.read_raw(pid)
    obj = pickle.loads(raw[:raw.rfind(b".")+1])
    return InternalNode(False, obj["keys"], obj["children"])

def is_leaf_page(raw: bytes) -> bool:
    return len(raw) >= 1 and raw[0:1] == b"L"

class BPlusClustered:
    def __init__(self, filename: str, order_hint: int,
                 key_fn: Callable[[Dict[str, Any]], Any],
                 schema: Schema):
        self.dsk = Disk(filename)
        self.order = order_hint
        self.key_fn = key_fn
        self.schema = schema
        self.block_factor = max(1, (PAGE_SIZE - LEAF_HDR_SIZE) // self.schema.record_size)
        if self.dsk.root_pid == 0:
            leaf_pid = self.dsk.alloc()
            self.dsk.write_raw(leaf_pid, LeafPage(self.schema, self.block_factor).pack())
            self.dsk.set_root(leaf_pid)

    def _load(self, pid: int):
        raw = self.dsk.read_raw(pid)
        if is_leaf_page(raw): return LeafPage.unpack(raw, self.schema, self.block_factor), True
        return read_internal(self.dsk, pid), False

    def _save(self, pid: int, node, is_leaf: bool):
        if is_leaf: self.dsk.write_raw(pid, node.pack())
        else:       write_internal(self.dsk, pid, node)

    def search(self, key: Any) -> List[Dict[str, Any]]:
        pid = self.dsk.root_pid
        node, is_leaf = self._load(pid)
        while not is_leaf:
            idx = bisect.bisect_left(node.keys, key)
            pid = node.children[idx]
            node, is_leaf = self._load(pid)
        keys = [self.key_fn(r) for r in node.recs]
        i = bisect.bisect_left(keys, key)
        out: List[Dict[str, Any]] = []
        while i < len(node.recs) and self.key_fn(node.recs[i]) == key:
            out.append(node.recs[i]); i += 1
        return out

    def range_search(self, lo: Any, hi: Any) -> Iterable[Dict[str, Any]]:
        pid = self.dsk.root_pid
        node, is_leaf = self._load(pid)
        while not is_leaf:
            idx = bisect.bisect_left(node.keys, lo)
            pid = node.children[idx]
            node, is_leaf = self._load(pid)
        while True:
            keys = [self.key_fn(r) for r in node.recs]
            i = bisect.bisect_left(keys, lo)
            while i < len(node.recs) and self.key_fn(node.recs[i]) <= hi:
                yield node.recs[i]; i += 1
            if node.next_leaf == 0: break
            nxt, leaf = self._load(node.next_leaf)
            if not leaf or (len(nxt.recs) and self.key_fn(nxt.recs[0]) > hi): break
            node = nxt

    def insert(self, rec: Dict[str, Any]):
        key = self.key_fn(rec)
        split = self._ins_rec(self.dsk.root_pid, key, rec)
        if split is not None:
            sep_key, right_pid = split
            old_root = self.dsk.root_pid
            new_root = InternalNode(False, [sep_key], [old_root, right_pid])
            new_pid = self.dsk.alloc()
            self._save(new_pid, new_root, False)
            self.dsk.set_root(new_pid)

    def _ins_rec(self, pid: int, key: Any, rec: Dict[str, Any]):
        node, is_leaf = self._load(pid)
        if is_leaf:
            pos = bisect.bisect_right([self.key_fn(r) for r in node.recs], key)
            node.recs.insert(pos, rec)
            if len(node.recs) <= self.block_factor:
                self._save(pid, node, True)
                return None
            # split hoja
            mid = len(node.recs) // 2
            right_recs = node.recs[mid:]
            node.recs   = node.recs[:mid]
            right = LeafPage(self.schema, self.block_factor, right_recs, prev_leaf=pid, next_leaf=node.next_leaf)
            right_pid = self.dsk.alloc()
            node.next_leaf = right_pid
            self._save(pid, node, True)
            self._save(right_pid, right, True)
            sep_key = self.key_fn(right.recs[0])
            return (sep_key, right_pid)
        else:
            idx = bisect.bisect_left(node.keys, key)
            child_pid = node.children[idx]
            split = self._ins_rec(child_pid, key, rec)
            if split is None: return None
            sep, right_pid = split
            node.keys.insert(idx, sep)
            node.children.insert(idx+1, right_pid)
            if len(node.keys) <= ORDER_HINT:
                self._save(pid, node, False)
                return None
            mid = len(node.keys) // 2
            sep_up = node.keys[mid]
            right = InternalNode(False, node.keys[mid+1:], node.children[mid+1:])
            node.keys = node.keys[:mid]
            node.children = node.children[:mid+1]
            right_pid2 = self.dsk.alloc()
            self._save(pid, node, False)
            self._save(right_pid2, right, False)
            return (sep_up, right_pid2)

    def remove(self, key: Any, only_first: bool = False) -> int:
        removed = self._rem_rec(self.dsk.root_pid, key, only_first)
        root, leaf = self._load(self.dsk.root_pid)
        if (not leaf) and len(root.children) == 1:
            self.dsk.set_root(root.children[0])
        return removed

    def _rem_rec(self, pid: int, key: Any, only_first: bool) -> int:
        node, is_leaf = self._load(pid)
        if is_leaf:
            keys = [self.key_fn(r) for r in node.recs]
            i = bisect.bisect_left(keys, key)
            cnt = 0
            while i < len(node.recs) and self.key_fn(node.recs[i]) == key:
                node.recs.pop(i); cnt += 1
                if only_first: break
            self._save(pid, node, True)
            return cnt
        idx = bisect.bisect_left(node.keys, key)
        child_pid = node.children[idx]
        removed = self._rem_rec(child_pid, key, only_first)
        if removed == 0: return 0
        self._save(pid, node, False)
        return removed

    def close(self):
        self.dsk.close()


def read_csv_all(csv_path: str) -> List[Dict[str, str]]:
    with open(csv_path, newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        return list(rdr)

def make_key_fn(key_field: str, schema: Schema) -> Callable[[Dict[str, Any]], Any]:
    col = next(c for c in schema.cols if c.name == key_field)
    if col.kind == "int":   return lambda r: int(r[key_field])
    if col.kind == "float": return lambda r: float(r[key_field])
    return lambda r: str(r[key_field])


if __name__ == "__main__":
    csv_path = input("Ruta del archivo CSV: ").strip()
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"No existe el archivo: {csv_path}")

    rows_raw = read_csv_all(csv_path)
    if not rows_raw:
        raise ValueError("El CSV no contiene filas.")
    schema = infer_schema(rows_raw)


    rows: List[Dict[str, Any]] = []
    for r in rows_raw:
        nr: Dict[str, Any] = {}
        for c in schema.cols:
            v = r.get(c.name, "")
            if c.kind == "int":
                try: nr[c.name] = int(float(v))
                except: nr[c.name] = 0
            elif c.kind == "float":
                try: nr[c.name] = float(v)
                except: nr[c.name] = 0.0
            else:
                nr[c.name] = str(v)
        rows.append(nr)


    print("Campos disponibles:", ", ".join(c.name for c in schema.cols))
    key_field = input("¿Qué campo del CSV será la clave del B+Tree? (exacto): ").strip()
    if key_field not in [c.name for c in schema.cols]:
        raise ValueError(f"Campo no válido: {key_field}")


    base = os.path.splitext(os.path.basename(csv_path))[0]
    idx_filename = f"{base}_{key_field}.bpt"
    if os.path.exists(idx_filename):
        os.remove(idx_filename)

    key_fn = make_key_fn(key_field, schema)
    bpt = BPlusClustered(idx_filename, ORDER_HINT, key_fn, schema)

    for rec in rows:
        bpt.insert(rec)


    if rows:
        mid_rec = rows[len(rows)//2]
        k = key_fn(mid_rec)
        print(f"search({k!r}) ->", len(bpt.search(k)))

        lo_rec = rows[len(rows)//3]
        hi_rec = rows[len(rows)//3*2]
        lo_key, hi_key = key_fn(lo_rec), key_fn(hi_rec)
        if lo_key > hi_key: lo_key, hi_key = hi_key, lo_key
        cnt = sum(1 for _ in bpt.range_search(lo_key, hi_key))
        print(f"range_search({lo_key!r}..{hi_key!r}) ->", cnt)

        rem = bpt.remove(k, only_first=True)
        print(f"remove({k!r}) ->", rem)

    print("Leaf BLOCK_FACTOR:", bpt.block_factor)
    print("I/O reads:", IO.reads, "writes:", IO.writes)
    bpt.close()