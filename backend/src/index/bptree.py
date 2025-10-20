# B+ Tree Clustered File - Index
from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple, Callable
import os, struct, bisect, pickle, sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from core.schema import Schema, Kind
from io_counters import count_read, count_write  

PAGE_SIZE   = 4096
MAGIC       = b"BPTCFS1\0"
VERSION     = 1
ORDER_HINT  = 64

def _key_norm(v: Any, kind: Kind) -> Any:
    if v is None:
        if kind == Kind.INT:   return 0
        if kind == Kind.FLOAT: return 0.0
        if kind == Kind.DATE:  return ""
        return ""
    if kind == Kind.INT:
        return int(v)
    if kind == Kind.FLOAT:
        return float(v)
    if kind == Kind.DATE:
        return str(v)[:10]
    return str(v)

class Disk:
    HEADER_FMT  = "<8sIQQQ"
    HEADER_SIZE = struct.calcsize(HEADER_FMT)

    def __init__(self, filename: str):
        self.filename = filename
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
        hdr = struct.pack(self.HEADER_FMT, MAGIC, VERSION,
                          self.root_pid, self.free_head, self.num_pages)
        data = hdr + (b"\x00" * (PAGE_SIZE - len(hdr)))
        self.f.write(data)
        count_write(len(data))  

    def _read_header(self):
        self.f.seek(0)
        raw = self.f.read(PAGE_SIZE)
        count_read(len(raw))  
        magic, version, self.root_pid, self.free_head, self.num_pages = struct.unpack(
            self.HEADER_FMT, raw[:self.HEADER_SIZE]
        )
        if magic != MAGIC:
            raise ValueError("Archivo no es un B+ Tree Clustered válido.")

    def alloc(self) -> int:
        if self.free_head != 0:
            pid = self.free_head
            raw = self.read_raw(pid)
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
        if len(buf) > PAGE_SIZE:
            raise ValueError("Página excede PAGE_SIZE")
        self.f.seek(pid * PAGE_SIZE)
        data = buf + (b"\x00" * (PAGE_SIZE - len(buf)))
        self.f.write(data)
        count_write(len(data)) 

    def read_raw(self, pid: int) -> bytes:
        self.f.seek(pid * PAGE_SIZE)
        raw = self.f.read(PAGE_SIZE)
        count_read(len(raw))  
        return raw

    def set_root(self, pid: int):
        self.root_pid = pid
        self._write_header()

    def close(self):
        self.f.flush()
        self.f.close()

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
            raise ValueError("Overflow en LeafPage.pack()")
        out = [struct.pack(LEAF_HDR_FMT, b"L", len(self.recs),
                           self.prev_leaf, self.next_leaf)]
        for r in self.recs:
            out.append(self.schema.pack(r))
        if len(self.recs) < self.block_factor:
            out.append(b"\x00" * ((self.block_factor - len(self.recs)) * self.schema.size))
        return b"".join(out)

    @staticmethod
    def unpack(buf: bytes, schema: Schema, block_factor: int) -> "LeafPage":
        tag, count, prev_leaf, next_leaf = struct.unpack(
            LEAF_HDR_FMT, buf[:LEAF_HDR_SIZE]
        )
        if tag != b"L":
            raise ValueError("Página no es hoja")
        recs: List[Dict[str, Any]] = []
        off = LEAF_HDR_SIZE
        for _ in range(count):
            recs.append(schema.unpack(buf[off:off + schema.size]))
            off += schema.size
        return LeafPage(schema, block_factor, recs, prev_leaf, next_leaf)

@dataclass
class InternalNode:
    is_leaf: bool
    keys: List[Any]
    children: List[int]

def _write_internal(dsk: Disk, pid: int, node: InternalNode):
    data = pickle.dumps(
        {"is_leaf": False, "keys": node.keys, "children": node.children},
        protocol=pickle.HIGHEST_PROTOCOL
    )
    dsk.write_raw(pid, data)

def _read_internal(dsk: Disk, pid: int) -> InternalNode:
    raw = dsk.read_raw(pid)
    trimmed = raw.rstrip(b"\x00")
    obj = pickle.loads(trimmed) if trimmed else {"keys": [], "children": []}
    return InternalNode(False, list(obj.get("keys", [])), list(obj.get("children", [])))

def _is_leaf_page(raw: bytes) -> bool:
    return len(raw) >= 1 and raw[0:1] == b"L"

class BPlusClusteredFile:
    def __init__(self, filename: str, schema: Schema, key_field: str, order_hint: int = ORDER_HINT):
        self.dsk = Disk(filename)
        self.schema = schema
        self.key_field = key_field
        self.key_kind: Kind = next(f.kind for f in schema.fields if f.name == key_field)
        self.order = int(order_hint) if order_hint and order_hint > 3 else ORDER_HINT
        self.block_factor = max(1, (PAGE_SIZE - LEAF_HDR_SIZE) // self.schema.size)


        if self.dsk.root_pid == 0:
            leaf_pid = self.dsk.alloc()
            self.dsk.write_raw(leaf_pid, LeafPage(self.schema, self.block_factor).pack())
            self.dsk.set_root(leaf_pid)

    def _load(self, pid: int):
        raw = self.dsk.read_raw(pid)
        if _is_leaf_page(raw):
            return LeafPage.unpack(raw, self.schema, self.block_factor), True
        return _read_internal(self.dsk, pid), False

    def _save(self, pid: int, node, is_leaf: bool):
        if is_leaf:
            self.dsk.write_raw(pid, node.pack())
        else:
            _write_internal(self.dsk, pid, node)

    def _key_of(self, rec: Dict[str, Any]) -> Any:
        return _key_norm(rec.get(self.key_field), self.key_kind)

    def insert(self, rec: Dict[str, Any]):
        rec = self.schema.coerce_row(rec)
        key = self._key_of(rec)
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
            pos = bisect.bisect_right([self._key_of(r) for r in node.recs], key)
            node.recs.insert(pos, rec)
            if len(node.recs) <= self.block_factor:
                self._save(pid, node, True)
                return None
            mid = len(node.recs) // 2
            right_recs = node.recs[mid:]
            node.recs   = node.recs[:mid]
            right = LeafPage(self.schema, self.block_factor, right_recs,
                             prev_leaf=pid, next_leaf=node.next_leaf)
            right_pid = self.dsk.alloc()
            node.next_leaf = right_pid
            self._save(pid, node, True)
            self._save(right_pid, right, True)
            sep_key = self._key_of(right.recs[0])
            return (sep_key, right_pid)
        else:
            idx = bisect.bisect_left(node.keys, key)
            child_pid = node.children[idx]
            split = self._ins_rec(child_pid, key, rec)
            if split is None:
                return None
            sep, right_pid = split
            node.keys.insert(idx, sep)
            node.children.insert(idx + 1, right_pid)
            if len(node.keys) <= self.order:
                self._save(pid, node, False)
                return None
            mid = len(node.keys) // 2
            sep_up = node.keys[mid]
            right = InternalNode(False, node.keys[mid + 1:], node.children[mid + 1:])
            node.keys = node.keys[:mid]
            node.children = node.children[:mid + 1]
            right_pid2 = self.dsk.alloc()
            self._save(pid, node, False)
            self._save(right_pid2, right, False)
            return (sep_up, right_pid2)

    def search(self, key: Any) -> List[Dict[str, Any]]:
        k = _key_norm(key, self.key_kind)
        pid = self.dsk.root_pid
        node, is_leaf = self._load(pid)
        while not is_leaf:
            idx = bisect.bisect_left(node.keys, k)
            pid = node.children[idx]
            node, is_leaf = self._load(pid)
        keys = [self._key_of(r) for r in node.recs]
        i = bisect.bisect_left(keys, k)
        out: List[Dict[str, Any]] = []
        while i < len(node.recs) and self._key_of(node.recs[i]) == k:
            out.append(node.recs[i]); i += 1
        return out

    def range_search(self, a: Any, b: Any) -> Iterable[Dict[str, Any]]:
        lo, hi = _key_norm(a, self.key_kind), _key_norm(b, self.key_kind)
        if lo > hi:
            lo, hi = hi, lo
        pid = self.dsk.root_pid
        node, is_leaf = self._load(pid)
        while not is_leaf:
            idx = bisect.bisect_left(node.keys, lo)
            pid = node.children[idx]
            node, is_leaf = self._load(pid)
        while True:
            keys = [self._key_of(r) for r in node.recs]
            i = bisect.bisect_left(keys, lo)
            while i < len(node.recs) and self._key_of(node.recs[i]) <= hi:
                yield node.recs[i]; i += 1
            if node.next_leaf == 0:
                break
            nxt, leaf = self._load(node.next_leaf)
            if not leaf:
                break
            if len(nxt.recs) and self._key_of(nxt.recs[0]) > hi:
                break
            node = nxt

    def remove(self, key: Any, only_first: bool = False) -> int:
        k = _key_norm(key, self.key_kind)
        removed = self._rem_rec(self.dsk.root_pid, k, only_first)
        root, leaf = self._load(self.dsk.root_pid)
        if (not leaf) and len(root.children) == 1:
            self.dsk.set_root(root.children[0])
        return removed

    def _rem_rec(self, pid: int, k: Any, only_first: bool) -> int:
        node, is_leaf = self._load(pid)
        if is_leaf:
            keys = [self._key_of(r) for r in node.recs]
            i = bisect.bisect_left(keys, k)
            cnt = 0
            while i < len(node.recs) and self._key_of(node.recs[i]) == k:
                node.recs.pop(i); cnt += 1
                if only_first: break
            self._save(pid, node, True)
            return cnt
        idx = bisect.bisect_left(node.keys, k)
        child_pid = node.children[idx]
        removed = self._rem_rec(child_pid, k, only_first)
        if removed == 0:
            return 0
        self._save(pid, node, False)
        return removed

    def iter_all(self) -> Iterable[Dict[str, Any]]:
        pid = self.dsk.root_pid
        node, is_leaf = self._load(pid)
        while not is_leaf:
            pid = node.children[0]
            node, is_leaf = self._load(pid)
        while True:
            for r in node.recs:
                yield r
            if node.next_leaf == 0:
                break
            node, is_leaf = self._load(node.next_leaf)

    def close(self):
        self.dsk.close()

def build_from_rows(filename: str, schema: Schema, key_field: str,
                    rows: List[Dict[str, Any]], order_hint: int = ORDER_HINT) -> BPlusClusteredFile:
    if os.path.exists(filename):
        os.remove(filename)
    bpt = BPlusClusteredFile(filename, schema, key_field, order_hint)
    for r in rows:
        bpt.insert(r)
    return bpt