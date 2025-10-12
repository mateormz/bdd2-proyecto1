from __future__ import annotations
from typing import Optional, List, Tuple, Dict, Any, Protocol
import csv
import helpers
import data_index_file
from backend.src.AVL.codec import RecordCodec


class AVLFile:
    """AVL index (disk) + append-only data file with a generic record codec."""

    def __init__(self, data_path: str, index_path: str, *, codec: RecordCodec, create: bool = False):
        self.codec = codec
        self.data = data_index_file.DataFile(data_path, codec.record_size(), create=create)
        self.index = data_index_file.IndexFile(index_path, create=create)

    # lifecycle
    def close(self): self.data.close(); self.index.close()
    def __enter__(self): return self
    def __exit__(self, exc_type, exc, tb): self.close()

    # required ops (assignment naming)
    def add(self, row: Dict[str, Any]) -> None:
        """Append row and index by key (duplicates allowed)."""
        packed = self.codec.pack(row)
        data_off = self.data.append(packed)
        key = self.codec.key_of(row)
        new_root = helpers.insert(self.index, self.index.root_off, key, data_off)
        self.index._write_header(root_off=new_root, count=self.index.count + 1)

    def search(self, key: int) -> List[Dict[str, Any]]:
        offs: List[int] = []
        helpers.search_collect(self.index, self.index.root_off, key, offs)
        return [ self.codec.unpack(self.data.read_at(off)) for off in offs ]

    def rangeSearch(self, begin_key: int, end_key: int) -> List[Dict[str, Any]]:
        if begin_key > end_key: begin_key, end_key = end_key, begin_key
        offs: List[int] = []
        helpers.range_collect(self.index, self.index.root_off, begin_key, end_key, offs)
        return [ self.codec.unpack(self.data.read_at(off)) for off in offs ]

    def remove(self, key: int) -> int:
        """Remove ALL rows matching key. Returns count removed."""
        removed_total = 0
        while True:
            new_root, removed = helpers.delete_once(self.index, self.index.root_off, key)
            if not removed: break
            self.index.write_header(root_off=new_root, count=max(0, self.index.count - 1))
            removed_total += 1
        return removed_total

    # maintenance
    def compact_data(self) -> int:
        """Rewrite data file keeping only indexed records; update node offsets."""
        mapping: List[Tuple[int, bytes]] = []
        def _gather(off: int):
            if not off: return
            k, h, l, r, v = self.index.read_node(off)
            _gather(l); mapping.append((k, self.data.read_at(v))); _gather(r)
        _gather(self.index.root_off)

        self.data.rewrite_all(mapping)

        new_offsets: Dict[int, int] = {}
        pos = self.data.header_size
        for k, _ in mapping:
            new_offsets.setdefault(k, pos)  # for duplicates we still set by encounter; ok since value_off for each node will be updated independently below
            pos += self.codec.record_size()

        def _rewrite(off: int):
            if not off: return
            k, h, l, r, v = self.index.read_node(off)
            # Advance by one record size for *each* encounter of k in in-order
            # To assign distinct offsets for duplicates deterministically, we step through mapping again.
            # Simpler: recompute by scanning data file sequentially.
            _rewrite(l)
            nonlocal_pos = _rewrite.pos
            self.index.write_node(off, k, h, l, r, nonlocal_pos)
            _rewrite.pos += self.codec.record_size()
            _rewrite(r)
        _rewrite.pos = self.data.header_size
        _rewrite(self.index.root_off)
        return len(mapping)

    # instrumentation
    def reset_io(self) -> None:
        self.data.reads = self.data.writes = 0
        self.index.reads = self.index.writes = 0

    def io_stats(self) -> Dict[str, int]:
        return {
            "data_reads": self.data.reads,
            "data_writes": self.data.writes,
            "index_reads": self.index.reads,
            "index_writes": self.index.writes,
            "index_nodes": self.index.count,
        }


# ----------------------------- Convenience -----------------------------------

def bulk_load_csv(avl: AVLFile, csv_path: str, *, key_field: str, limit: Optional[int]=None) -> int:
    """Load rows from CSV into the AVLFile (first row as header)."""
    n = 0
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Ensure key exists
            if key_field not in row: raise KeyError(f"CSV missing key field '{key_field}'")
            avl.add(row)
            n += 1
            if limit and n >= limit: break
    return n