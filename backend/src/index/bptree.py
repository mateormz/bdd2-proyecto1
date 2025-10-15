# B+ Tree File Clustered

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, List, Optional, Tuple, Iterable, Dict
import os, io, struct, pickle, bisect


PAGE_SIZE = 4096
MAGIC     = b"BPTREE1\0"
VERSION   = 1
ORDER_HINT = 64

class IOStats:
    def __init__(self):
        self.reads = 0
        self.writes = 0

    def reset(self):
        self.reads = 0
        self.writes = 0

IO = IOStats()








class DiskManager:
    HEADER_FMT = "<8sIQQQ"
    HEADER_SIZE = struct.calcsize(HEADER_FMT)

    def __init__(self, filename: str):
        self.filename = filename
        create = not os.path.exists(filename) or os.path.getsize(filename) == 0
        self.f = open(filename, "r+b" if not create else "w+b")
        if create:
            self._init_file()
        else:
            self._read_header()

    def _init_file(self):
        self.root_pid = 0
        self.free_head = 0
        self.num_pages = 1
        self._write_header()
        self.f.flush()

    def _write_header(self):
        self.f.seek(0)
        header = struct.pack(self.HEADER_FMT, MAGIC, VERSION, self.root_pid, self.free_head, self.num_pages)
        pad = PAGE_SIZE - len(header)
        self.f.write(header + (b"\x00" * pad))
        IO.writes += 1

    def _read_header(self):
        self.f.seek(0)
        raw = self.f.read(PAGE_SIZE)
        IO.reads += 1
        magic, version, self.root_pid, self.free_head, self.num_pages = struct.unpack(self.HEADER_FMT, raw[:self.HEADER_SIZE])
        if magic != MAGIC:
            raise ValueError("Archivo no es un B+Tree válido")

    def alloc_page(self) -> int:
        if self.free_head != 0:
            pid = self.free_head
            next_free = self._read_page(pid)["__free_next__"]
            self.free_head = next_free
            self._write_header()
            return pid
        else:
            pid = self.num_pages
            self.write_page(pid, {"__empty__": True})
            self.num_pages += 1
            self._write_header()
            return pid

    def free_page(self, pid: int):
        rec = {"__free__": True, "__free_next__": self.free_head}
        self.write_page(pid, rec)
        self.free_head = pid
        self._write_header()

    def write_page(self, pid: int, obj: Any):
        buf = pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)
        if len(buf) > PAGE_SIZE:
            raise ValueError(f"Objeto excede PAGE_SIZE ({len(buf)} > {PAGE_SIZE})")
        self.f.seek(pid * PAGE_SIZE)
        pad = PAGE_SIZE - len(buf)
        self.f.write(buf + (b"\x00" * pad))
        IO.writes += 1

    def _read_page(self, pid: int) -> Any:
        self.f.seek(pid * PAGE_SIZE)
        raw = self.f.read(PAGE_SIZE)
        IO.reads += 1
        end = raw.rfind(b'.')
        if end == -1:
            return {}
        data = raw[:end+1]
        try:
            return pickle.loads(data)
        except Exception:
            return {}

    def read_page(self, pid: int) -> Any:
        return self._read_page(pid)

    def set_root(self, pid: int):
        self.root_pid = pid
        self._write_header()

    def close(self):
        self.f.flush()
        self.f.close()










@dataclass
class InternalNode:
    is_leaf: bool = False
    keys: List[Any] = field(default_factory=list)
    children: List[int] = field(default_factory=list)

@dataclass
class LeafNode:
    is_leaf: bool = True
    keys: List[Any] = field(default_factory=list)
    records: List[Dict[str, Any]] = field(default_factory=list)
    next_leaf: int = 0



class BPlusTreeFile:
    def __init__(self, filename: str, order_hint: int = ORDER_HINT):
        self.dm = DiskManager(filename)
        self.order_hint = order_hint

        if self.dm.root_pid == 0:
            root = LeafNode()
            root_pid = self.dm.alloc_page()
            self.dm.write_page(root_pid, root.__dict__)
            self.dm.set_root(root_pid)


    def _load_node(self, pid: int) -> Tuple[Any, bool]:
        obj = self.dm.read_page(pid)
        if obj.get("is_leaf", False):
            node = LeafNode(**{k: obj[k] for k in ["is_leaf", "keys", "records", "next_leaf"]})
            return node, True
        else:
            node = InternalNode(**{k: obj[k] for k in ["is_leaf", "keys", "children"]})
            return node, False

    def _save_node(self, pid: int, node: Any):
        if isinstance(node, LeafNode):
            self.dm.write_page(pid, {
                "is_leaf": True,
                "keys": node.keys,
                "records": node.records,
                "next_leaf": node.next_leaf
            })
        else:
            self.dm.write_page(pid, {
                "is_leaf": False,
                "keys": node.keys,
                "children": node.children
            })

    def _max_keys(self, is_leaf: bool) -> int:
        return self.order_hint

    def _min_keys(self, is_leaf: bool) -> int:
        mk = max(1, (self._max_keys(is_leaf) + 1)//2 - 1) if is_leaf else max(1, (self._max_keys(is_leaf)+1)//2 - 1)
        return mk


    def search(self, key: Any) -> List[Dict[str, Any]]:
        pid = self.dm.root_pid
        node, is_leaf = self._load_node(pid)
        path = [pid]
        while not is_leaf:
            idx = bisect.bisect_left(node.keys, key)
            pid = node.children[idx]
            node, is_leaf = self._load_node(pid)
            path.append(pid)
        i = bisect.bisect_left(node.keys, key)
        res = []
        while i < len(node.keys) and node.keys[i] == key:
            res.append(node.records[i])
            i += 1
        return res

    def range_search(self, lo: Any, hi: Any) -> Iterable[Dict[str, Any]]:
        pid = self.dm.root_pid
        node, is_leaf = self._load_node(pid)
        while not is_leaf:
            idx = bisect.bisect_left(node.keys, lo)
            pid = node.children[idx]
            node, is_leaf = self._load_node(pid)


        while True:
            i = bisect.bisect_left(node.keys, lo)
            while i < len(node.keys) and node.keys[i] <= hi:
                yield node.records[i]
                i += 1
            if node.next_leaf == 0:
                break

            nxt, _ = self._load_node(node.next_leaf)
            if len(nxt.keys) == 0 or nxt.keys[0] > hi:
                break
            node = nxt


    def insert(self, key: Any, record: Dict[str, Any]):
        root_pid = self.dm.root_pid
        new_child = self._insert_recursive(root_pid, key, record)
        if new_child is not None:
            sep_key, right_pid = new_child
            old_root_pid = root_pid
            new_root = InternalNode(is_leaf=False, keys=[sep_key], children=[old_root_pid, right_pid])
            new_root_pid = self.dm.alloc_page()
            self._save_node(new_root_pid, new_root)
            self.dm.set_root(new_root_pid)

    def _insert_recursive(self, pid: int, key: Any, record: Dict[str, Any]):
        node, is_leaf = self._load_node(pid)
        if is_leaf:
            pos = bisect.bisect_right(node.keys, key)
            node.keys.insert(pos, key)
            node.records.insert(pos, record)
            if len(node.keys) > self._max_keys(True):
                return self._split_leaf(pid, node)
            else:
                self._save_node(pid, node)
                return None
        else:
            idx = bisect.bisect_left(node.keys, key)
            child_pid = node.children[idx]
            split_info = self._insert_recursive(child_pid, key, record)
            if split_info is None:
                return None
            sep_key, right_pid = split_info
            node.keys.insert(idx, sep_key)
            node.children.insert(idx+1, right_pid)
            if len(node.keys) > self._max_keys(False):
                return self._split_internal(pid, node)
            else:
                self._save_node(pid, node)
                return None

    def _split_leaf(self, pid: int, node: LeafNode):
        mid = len(node.keys) // 2
        right = LeafNode(
            is_leaf=True,
            keys=node.keys[mid:],
            records=node.records[mid:],
            next_leaf=node.next_leaf
        )
        node.keys = node.keys[:mid]
        node.records = node.records[:mid]
        right_pid = self.dm.alloc_page()
        node.next_leaf = right_pid
        self._save_node(pid, node)
        self._save_node(right_pid, right)
        sep_key = right.keys[0]
        return (sep_key, right_pid)

    def _split_internal(self, pid: int, node: InternalNode):
        mid = len(node.keys)//2
        sep_key = node.keys[mid]
        right = InternalNode(
            is_leaf=False,
            keys=node.keys[mid+1:],
            children=node.children[mid+1:]
        )
        node.keys = node.keys[:mid]
        node.children = node.children[:mid+1]
        right_pid = self.dm.alloc_page()
        self._save_node(pid, node)
        self._save_node(right_pid, right)
        return (sep_key, right_pid)












if __name__ == "__main__":

    if os.path.exists("inventario.bpt"):
        os.remove("inventario.bpt")

    bpt = BPlusTreeFile("inventario.bpt", order_hint=64)

    for pid in [10, 5, 20, 15, 8, 7, 30, 25, 40, 1, 2, 3, 4, 6, 9, 11, 12]:
        bpt.insert(pid, {
            "id": pid,
            "nombre": f"Producto {pid}",
            "categoria": "general",
            "precio": pid * 1.5
        })

    print("search(15) ->", bpt.search(15))
    print("range [5,12]:")
    for r in bpt.range_search(5, 12):
        print(r["id"], r["nombre"])

    #print("remove(8) ->", bpt.remove(8))
    print("range [1,12] tras remove:")
    for r in bpt.range_search(1, 12):
        print(r["id"], r["nombre"])

    print("I/O reads:", IO.reads, "writes:", IO.writes)
    bpt.close()