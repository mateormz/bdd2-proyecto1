from __future__ import annotations
import os, math, bisect, pickle, struct
from typing import Any, Dict, List, Iterable, Callable, Tuple
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.core.schema import Schema, Field, Kind

PAGE_SIZE = 4096
PAGE_HEADER_FMT = "<ii"
PAGE_HEADER_SIZE = struct.calcsize(PAGE_HEADER_FMT)



def _key_norm(v: Any, kind: Kind) -> Any:
    if v is None:
        if kind == Kind.INT: return -2**31
        if kind == Kind.FLOAT: return float("-inf")
        if kind == Kind.DATE: return "0001-01-01"
        return ""
    if kind == Kind.INT:
        return int(v)
    if kind == Kind.FLOAT:
        return float(v)
    if kind == Kind.DATE:
        s = str(v).strip()[:10]
        return s if len(s) == 10 else "0001-01-01"
    return str(v)



class Page:
    def __init__(self, schema: Schema, records: List[Dict[str, Any]] = None, next_page: int = -1):
        self.schema = schema
        self.records = [] if records is None else records
        self.next_page = next_page

    def pack(self, block_factor: int) -> bytes:
        buf = [struct.pack(PAGE_HEADER_FMT, len(self.records), self.next_page)]
        buf += [self.schema.pack(r) for r in self.records]
        pad = block_factor - len(self.records)
        if pad > 0:
            buf.append(b"\x00" * (pad * self.schema.size))
        return b"".join(buf)

    @staticmethod
    def unpack(buf: bytes, schema: Schema) -> "Page":
        if len(buf) < PAGE_HEADER_SIZE:
            return Page(schema, [], -1)
        count, next_page = struct.unpack(PAGE_HEADER_FMT, buf[:PAGE_HEADER_SIZE])
        recs = []
        off = PAGE_HEADER_SIZE
        for _ in range(count):
            rec = schema.unpack(buf[off:off + schema.size])
            recs.append(rec)
            off += schema.size
        return Page(schema, recs, next_page)



class SparseIndex:
    def __init__(self, path: str):
        self.path = path

    def write(self, entries: List[Tuple[Any, int]]):
        with open(self.path, "wb") as f:
            pickle.dump(entries, f, protocol=pickle.HIGHEST_PROTOCOL)

    def read(self) -> List[Tuple[Any, int]]:
        if not os.path.exists(self.path):
            return []
        with open(self.path, "rb") as f:
            return pickle.load(f)

    @staticmethod
    def locate(entries: List[Tuple[Any, int]], key: Any) -> int:
        keys = [k for k, _ in entries]
        pos = bisect.bisect_right(keys, key) - 1
        return max(0, pos)




class SequentialOrderedFile:
    def __init__(self, base_filename: str, schema: Schema, key_field: str):
        self.base = os.path.splitext(base_filename)[0]
        self.data_path = self.base + ".dat"
        self.index_path = self.base + ".sidx"
        self.schema = schema
        self.key_field = key_field
        self.key_kind = next(f.kind for f in schema.fields if f.name == key_field)
        self.sparse = SparseIndex(self.index_path)
        self.BLOCK_FACTOR = max(1, (PAGE_SIZE - PAGE_HEADER_SIZE) // schema.size)
        self.aux: List[Dict[str, Any]] = []
        self.K = 16

        if not os.path.exists(self.data_path):
            open(self.data_path, "wb").close()

    def _page_bytes(self) -> int:
        return PAGE_HEADER_SIZE + self.BLOCK_FACTOR * self.schema.size

    def bulk_load(self, rows: List[Dict[str, Any]]):
        rows_sorted = sorted(rows, key=lambda r: _key_norm(r[self.key_field], self.key_kind))
        entries: List[Tuple[Any, int]] = []
        with open(self.data_path, "wb") as f:
            pid = 0
            for i in range(0, len(rows_sorted), self.BLOCK_FACTOR):
                chunk = rows_sorted[i:i + self.BLOCK_FACTOR]
                page = Page(self.schema, chunk, -1)
                buf = page.pack(self.BLOCK_FACTOR)
                pad = self._page_bytes() - len(buf)
                f.write(buf + (b"\x00" * pad))
                entries.append((_key_norm(chunk[0][self.key_field], self.key_kind), pid))
                pid += 1
        self.sparse.write(entries)

    def _read_page(self, pid: int) -> Page:
        with open(self.data_path, "rb") as f:
            f.seek(pid * self._page_bytes())
            buf = f.read(self._page_bytes())
        return Page.unpack(buf, self.schema)

    def search(self, key: Any) -> List[Dict[str, Any]]:
        key_n = _key_norm(key, self.key_kind)
        entries = self.sparse.read()
        if not entries:
            return []
        pid = SparseIndex.locate(entries, key_n)
        out = []
        while pid < len(entries):
            page = self._read_page(pid)
            for r in page.records:
                k = _key_norm(r[self.key_field], self.key_kind)
                if k == key_n:
                    out.append(r)
                elif k > key_n:
                    return out
            pid += 1
        return out

    def range_search(self, lo: Any, hi: Any) -> Iterable[Dict[str, Any]]:
        lo_n = _key_norm(lo, self.key_kind)
        hi_n = _key_norm(hi, self.key_kind)
        entries = self.sparse.read()
        if not entries:
            return []
        pid = SparseIndex.locate(entries, lo_n)
        while pid < len(entries):
            page = self._read_page(pid)
            for r in page.records:
                k = _key_norm(r[self.key_field], self.key_kind)
                if lo_n <= k <= hi_n:
                    yield r
                elif k > hi_n:
                    return
            pid += 1

    def add(self, rec: Dict[str, Any]):
        self.aux.append(rec)
        if len(self.aux) >= self.K:
            self._rebuild()

    def _rebuild(self):
        entries = self.sparse.read()
        all_records: List[Dict[str, Any]] = []
        for pid in range(len(entries)):
            page = self._read_page(pid)
            all_records.extend(page.records)
        all_records.extend(self.aux)
        self.bulk_load(all_records)
        self.aux.clear()

    def remove(self, key: Any) -> int:
        key_n = _key_norm(key, self.key_kind)
        entries = self.sparse.read()
        if not entries:
            return 0
        removed = 0
        with open(self.data_path, "r+b") as f:
            for pid in range(len(entries)):
                off = pid * self._page_bytes()
                f.seek(off)
                buf = f.read(self._page_bytes())
                page = Page.unpack(buf, self.schema)
                new_recs = []
                for r in page.records:
                    k = _key_norm(r[self.key_field], self.key_kind)
                    if k == key_n:
                        removed += 1
                        continue
                    new_recs.append(r)
                page.records = new_recs
                new_buf = page.pack(self.BLOCK_FACTOR)
                pad = self._page_bytes() - len(new_buf)
                f.seek(off)
                f.write(new_buf + (b"\x00" * pad))
        new_entries: List[Tuple[Any, int]] = []
        for pid in range(len(entries)):
            page = self._read_page(pid)
            if page.records:
                new_entries.append((_key_norm(page.records[0][self.key_field], self.key_kind), pid))
        self.sparse.write(new_entries)
        return removed