# B+ Tree Clustered File - Index

from __future__ import annotations
import csv
import os
import pickle
import shutil
from dataclasses import dataclass, field
from typing import Any, Iterable, List, Optional, Tuple



import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from core.schema import Schema, Field, Kind
from io_counters import IOCounter, count_read, count_write
#from ..core.schema import Schema, Field, Kind
#from core.schema import Schema, Field, Kind



def cast_key(kind: Kind, raw: Any) -> Any:
    if kind == Kind.INT:
        return int(raw)
    if kind == Kind.FLOAT:
        return float(raw)
    return str(raw)



@dataclass
class BPTNode:
    is_leaf: bool
    keys: List[Any] = field(default_factory=list)
    values: Optional[List[List[int]]] = None
    children: Optional[List["BPTNode"]] = None
    next_leaf: Optional["BPTNode"] = None
    order: int = 256
    fanout: int = 256

    def find_child_index(self, key) -> int:
        lo, hi = 0, len(self.keys)
        while lo < hi:
            mid = (lo + hi) // 2
            if key < self.keys[mid]:
                hi = mid
            else:
                lo = mid + 1
        return lo

    def find_leaf_pos(self, key) -> Tuple[bool, int]:
        lo, hi = 0, len(self.keys)
        while lo < hi:
            mid = (lo + hi) // 2
            if self.keys[mid] == key:
                return True, mid
            if key < self.keys[mid]:
                hi = mid
            else:
                lo = mid + 1
        return False, lo


class BPlusTree:
    def __init__(self, key_kind: Kind, leaf_capacity: int = 256, internal_fanout: int = 256):
        self.key_kind = key_kind
        self.leaf_capacity = leaf_capacity
        self.internal_fanout = internal_fanout
        self.root: Optional[BPTNode] = None
        self.io_counter = IOCounter()

    def bulk_load(self, sorted_items: Iterable[Tuple[Any, int]]):
        leaves: List[BPTNode] = []
        leaf = BPTNode(is_leaf=True, order=self.leaf_capacity, fanout=self.internal_fanout,
                       keys=[], values=[])
        last_key = object()
        for key, off in sorted_items:
            if leaf.keys and len(leaf.keys) >= self.leaf_capacity and key != last_key:
                leaves.append(leaf)
                nxt = BPTNode(is_leaf=True, order=self.leaf_capacity, fanout=self.internal_fanout,
                              keys=[], values=[])
                leaf.next_leaf = nxt
                leaf = nxt
            if leaf.keys and key == leaf.keys[-1]:
                leaf.values[-1].append(off)
            else:
                leaf.keys.append(key)
                leaf.values.append([off])
            last_key = key
        if leaf.keys:
            leaves.append(leaf)
        if not leaves:
            self.root = BPTNode(is_leaf=True, order=self.leaf_capacity, fanout=self.internal_fanout,
                                keys=[], values=[])
            return

        level = leaves
        while len(level) > 1:
            nxt_level: List[BPTNode] = []
            for i in range(0, len(level), self.internal_fanout):
                chunk = level[i:i + self.internal_fanout]
                sep = [c.keys[0] for c in chunk[1:]]
                parent = BPTNode(is_leaf=False, keys=sep, children=chunk,
                                 order=self.leaf_capacity, fanout=self.internal_fanout)
                nxt_level.append(parent)
            level = nxt_level
        self.root = level[0]

    def _find_leaf(self, key) -> Optional[BPTNode]:
        node = self.root
        if not node:
            return None
        while not node.is_leaf:
            node = node.children[node.find_child_index(key)]
        return node

    def search(self, key) -> List[int]:
        node = self._find_leaf(key)
        if not node:
            return []
        found, pos = node.find_leaf_pos(key)
        if not found:
            return []
        return list(node.values[pos])

    def range_search(self, lo, hi) -> List[int]:
        node = self._find_leaf(lo)
        if not node:
            return []
        offs: List[int] = []
        while node:
            for k, vs in zip(node.keys, node.values):
                if k < lo:
                    continue
                if k > hi:
                    return offs
                offs.extend(vs)
            node = node.next_leaf
        return offs



class ClusteredIndexFile:
    def __init__(self, data_path: str, index_path: str, schema: Schema,
                 key_field: str, key_kind: Kind,
                 leaf_cap: int = 256, fanout: int = 256):
        self.data_path = data_path
        self.index_path = index_path
        self.schema = schema
        self.key_field = key_field
        self.key_kind = key_kind
        self.tree = BPlusTree(key_kind, leaf_capacity=leaf_cap, internal_fanout=fanout)
        self.io_counter = IOCounter()


    def _count_records(self) -> int:
        if not os.path.exists(self.data_path):
            return 0
        return os.path.getsize(self.data_path) // self.schema.size

    def _read_row_at(self, f, idx: int) -> dict:
        f.seek(idx * self.schema.size)
        return self.schema.unpack(f.read(self.schema.size))

    def _read_key_at(self, f, idx: int) -> Any:
        return cast_key(self.key_kind, self._read_row_at(f, idx)[self.key_field])

    def _bisect_left(self, f, n: int, key) -> int:
        lo, hi = 0, n
        while lo < hi:
            mid = (lo + hi) // 2
            if self._read_key_at(f, mid) < key:
                lo = mid + 1
            else:
                hi = mid
        return lo

    def _bisect_right(self, f, n: int, key) -> int:
        lo, hi = 0, n
        while lo < hi:
            mid = (lo + hi) // 2
            if key < self._read_key_at(f, mid):
                hi = mid
            else:
                lo = mid + 1
        return lo


    def build_from_csv(self, csv_path: str):
        rows: List[Tuple[Any, dict]] = []
        with open(csv_path, newline='', encoding='utf-8') as f:
            r = csv.DictReader(f)
            expected = [f.name for f in self.schema.fields]
            if r.fieldnames != expected:
                raise ValueError(f"Cabecera CSV incorrecta. Esperada {expected}, obtenida {r.fieldnames}")
            for row in r:
                k = cast_key(self.key_kind, row[self.key_field])
                rows.append((k, row))
        rows.sort(key=lambda kv: kv[0])

        os.makedirs(os.path.dirname(self.data_path), exist_ok=True)
        with open(self.data_path, 'wb') as binf:
            for _, row in rows:
                data = self.schema.pack(row)
                binf.write(data)
                self.io_counter.count_write(len(data))
                count_write(len(data))

        self._rebuild_index()

    def _rebuild_index(self):
        pairs: List[Tuple[Any, int]] = []
        if not os.path.exists(self.data_path):
            self.tree = BPlusTree(self.key_kind)
        else:
            with open(self.data_path, 'rb') as f:
                n = self._count_records()
                for i in range(n):
                    f.seek(i * self.schema.size)
                    buf = f.read(self.schema.size)
                    row = self.schema.unpack(buf)
                    key = cast_key(self.key_kind, row[self.key_field])
                    pairs.append((key, i * self.schema.size))
            self.tree = BPlusTree(self.key_kind, leaf_capacity=256, internal_fanout=256)
            self.tree.bulk_load(pairs)

        meta = {
            'key_field': self.key_field,
            'key_kind': self.key_kind.value,
            'schema_fmt': self.schema.fmt,
            'schema_size': self.schema.size,
        }
        with open(self.index_path, 'wb') as pf:
            pickle.dump({'meta': meta, 'tree': self.tree}, pf)


    def _rows_by_offsets(self, offsets: Iterable[int]) -> List[dict]:
        out = []
        with open(self.data_path, 'rb') as f:
            for off in offsets:
                f.seek(off)
                data = f.read(self.schema.size)
                self.io_counter.count_read(len(data))
                count_read(len(data))
                out.append(self.schema.unpack(data))
        return out

    def search(self, key_text: str) -> List[dict]:
        k = cast_key(self.key_kind, key_text)
        offs = self.tree.search(k)
        return self._rows_by_offsets(offs)

    def range_search(self, lo_text: str, hi_text: str) -> List[dict]:
        lo = cast_key(self.key_kind, lo_text)
        hi = cast_key(self.key_kind, hi_text)
        offs = self.tree.range_search(lo, hi)
        return self._rows_by_offsets(offs)


    def insert(self, row: dict):
        key = cast_key(self.key_kind, row[self.key_field])
        packed = self.schema.pack(row)
        n = self._count_records()
        os.makedirs(os.path.dirname(self.data_path), exist_ok=True)
        tmp = self.data_path + ".tmp"

        with open(self.data_path, 'rb') if os.path.exists(self.data_path) else open(self.data_path, 'wb') as src:
            with open(tmp, 'wb') as dst:
                if n == 0:
                    dst.write(packed)
                else:
                    with open(self.data_path, 'rb') as f:
                        pos = self._bisect_left(f, n, key)
                    if pos > 0:
                        with open(self.data_path, 'rb') as f:
                            f.seek(0)
                            dst.write(f.read(pos * self.schema.size))
                    dst.write(packed)
                    with open(self.data_path, 'rb') as f:
                        f.seek(pos * self.schema.size)
                        shutil.copyfileobj(f, dst)

        os.replace(tmp, self.data_path)
        self._rebuild_index()

    def remove(self, key_text: str) -> int:
        if not os.path.exists(self.data_path):
            return 0
        key = cast_key(self.key_kind, key_text)
        n = self._count_records()
        if n == 0:
            return 0

        with open(self.data_path, 'rb') as f:
            lo = self._bisect_left(f, n, key)
            hi = self._bisect_right(f, n, key)
        if lo == hi:
            return 0

        tmp = self.data_path + ".tmp"
        with open(self.data_path, 'rb') as src, open(tmp, 'wb') as dst:
            src.seek(0)
            dst.write(src.read(lo * self.schema.size))
            src.seek(hi * self.schema.size)
            shutil.copyfileobj(src, dst)

        os.replace(tmp, self.data_path)
        self._rebuild_index()
        return hi - lo


    @classmethod
    def load(cls, data_path: str, index_path: str, schema: Schema) -> "ClusteredIndexFile":
        with open(index_path, 'rb') as pf:
            payload = pickle.load(pf)
        meta = payload['meta']
        tree: BPlusTree = payload['tree']
        inst = cls(data_path, index_path, schema,
                   key_field=meta['key_field'],
                   key_kind=Kind(meta['key_kind']),
                   leaf_cap=tree.leaf_capacity,
                   fanout=tree.internal_fanout)
        inst.tree = tree
        return inst


def _prompt_row(fields: List[Field]) -> dict:
    row = {}
    print("Introduce valores para la nueva fila:")
    for f in fields:
        v = input(f"  {f.name} ({f.kind.value}) = ").strip()
        row[f.name] = v
    return row


def main():
    fields = [
        Field("id", Kind.INT, fmt="i"),
        Field("nombre", Kind.CHAR, size=20),
        Field("precio", Kind.FLOAT, fmt="f"),
        Field("fecha", Kind.DATE),
    ]
    schema = Schema(fields)

    print("=== B+ Tree Clustered Index File ===")
    csv_path = input("Ruta del CSV inicial (o vacío para saltar): ").strip()

    valid = [f.name for f in fields]
    print(f"Campos disponibles: {', '.join(valid)}")
    key_field = input("Campo llave del índice: ").strip()
    while key_field not in valid:
        key_field = input("Campo llave del índice: ").strip()
    key_kind = next(f.kind for f in fields if f.name == key_field)
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    out_dir = os.path.join(repo_root, "out")
    os.makedirs(out_dir, exist_ok=True)
    data_path = os.path.join(out_dir, "bptree_clustered.dat")
    index_path = os.path.join(out_dir, "bptree_clustered.idx")

    idx = ClusteredIndexFile(data_path, index_path, schema, key_field, key_kind,
                             leaf_cap=256, fanout=256)

    if csv_path:
        idx.build_from_csv(csv_path)
        print("Índice construido desde CSV.")

    print("\nComandos:")
    print("  point <key>")
    print("  range <lo> <hi>")
    print("  insert        (solicita valores)")
    print("  remove <key>  (elimina todas las filas con esa clave)")
    print("  quit")
    while True:
        try:
            cmd = input("> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break
        if not cmd:
            continue
        parts = cmd.split()
        op = parts[0].lower()

        if op == "quit":
            break

        elif op == "point" and len(parts) == 2:
            rows = idx.search(parts[1])
            print(f"{len(rows)} resultado(s)")
            for r in rows[:10]:
                print(r)
            if len(rows) > 10:
                print("...")

        elif op == "range" and len(parts) == 3:
            rows = idx.range_search(parts[1], parts[2])
            print(f"{len(rows)} resultado(s)")
            for r in rows[:10]:
                print(r)
            if len(rows) > 10:
                print("...")

        elif op == "insert":
            row = _prompt_row(fields)
            idx.insert(row)
            print("Insert OK.")

        elif op == "remove" and len(parts) == 2:
            removed = idx.remove(parts[1])
            print(f"Eliminados: {removed}")

        else:
            print("Uso: point <key> | range <lo> <hi> | insert | remove <key> | quit")


if __name__ == "__main__":
    main()