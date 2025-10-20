from __future__ import annotations
import os
import struct
from typing import Any, Dict, List, Optional
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.core.schema import Schema, Field, Kind

DEFAULT_D = 8
BLOCK_FACTOR = 32
MAX_CHAINING = 3


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
        s = str(v).strip()[:10]
        return s
    return str(v)

class Bucket:
    __slots__ = ("records", "next_bucket", "schema")

    def __init__(self, records: Optional[List[Dict[str, Any]]] = None,
                 next_bucket: int = -1, schema: Optional[Schema] = None):
        self.records: List[Dict[str, Any]] = [] if records is None else records
        self.next_bucket: int = next_bucket
        self.schema = schema

    @property
    def record_size(self) -> int:
        return self.schema.size if self.schema else 0

    @property
    def byte_size(self) -> int:
        return BLOCK_FACTOR * self.record_size + 4

    def pack(self) -> bytes:
        assert self.schema is not None
        data = bytearray()
        for r in self.records[:BLOCK_FACTOR]:
            data += self.schema.pack(r)
        missing = BLOCK_FACTOR - len(self.records)
        if missing > 0:
            data += b"\x00" * (missing * self.record_size)
        data += struct.pack("<i", self.next_bucket)
        if len(data) < self.byte_size:
            data += b"\x00" * (self.byte_size - len(data))
        return bytes(data)

    @staticmethod
    def unpack(buf: bytes, schema: Schema) -> "Bucket":
        rec_size = schema.size
        want = BLOCK_FACTOR * rec_size + 4
        if len(buf) < want:
            buf = buf.ljust(want, b"\x00")
        recs: List[Dict[str, Any]] = []
        off = 0
        for _ in range(BLOCK_FACTOR):
            chunk = buf[off:off + rec_size]
            off += rec_size
            if chunk != b"\x00" * rec_size:
                rec = schema.unpack(chunk)
                if any(str(v).strip() for v in rec.values() if v is not None):
                    recs.append(rec)
        (nxt,) = struct.unpack("<i", buf[BLOCK_FACTOR * rec_size: BLOCK_FACTOR * rec_size + 4])
        return Bucket(recs, nxt, schema)


def _stable_hash_str(s: str) -> int:
    h = 0xCBF29CE484222325
    for b in s.encode("utf-8"):
        h ^= b
        h = (h * 0x100000001B3) & 0xFFFFFFFFFFFFFFFF
    return h

def _stable_hash_float(x: float) -> int:
    if x == 0.0:
        x = 0.0
    bits = struct.unpack("<Q", struct.pack("<d", float(x)))[0]
    bits ^= (bits >> 33)
    bits *= 0xff51afd7ed558ccd
    bits &= 0xFFFFFFFFFFFFFFFF
    bits ^= (bits >> 33)
    bits *= 0xc4ceb9fe1a85ec53
    bits &= 0xFFFFFFFFFFFFFFFF
    bits ^= (bits >> 33)
    return bits


class ExtendibleHashing:
    def __init__(self, base_name: str, schema: Schema, key_field: str,
                 hash_function=None, initial_depth: int = DEFAULT_D):
        ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
        OUT_DIR = os.path.join(ROOT, "out")
        os.makedirs(OUT_DIR, exist_ok=True)

        if not base_name.endswith(".dat"):
            base_name += ".dat"
        self.filename = os.path.join(OUT_DIR, base_name)

        self.schema = schema
        self.key_field = key_field
        self.key_kind: Kind = next(f.kind for f in schema.fields if f.name == key_field)

        self.hash_function = hash_function
        self._D = initial_depth

        if not os.path.exists(self.filename) or os.path.getsize(self.filename) == 0:
            self._init_file(initial_depth)
        else:
            d_on_disk = self._read_global_depth()
            self._D = d_on_disk if d_on_disk > 0 else initial_depth
            if self._bucket_count_on_disk() < (1 << self._D):
                self._init_file(self._D)

    def _bucket_size(self) -> int:
        return BLOCK_FACTOR * self.schema.size + 4

    def _read_global_depth(self) -> int:
        try:
            with open(self.filename, "rb") as f:
                b = f.read(4)
                if len(b) == 4:
                    return struct.unpack("<i", b)[0]
        except Exception:
            pass
        return DEFAULT_D

    def _write_global_depth(self, D: int):
        with open(self.filename, "r+b") as f:
            f.seek(0)
            f.write(struct.pack("<i", D))

    def _bucket_offset(self, index: int) -> int:
        return 4 + index * self._bucket_size()

    def _bucket_count_on_disk(self) -> int:
        size = os.path.getsize(self.filename)
        if size < 4:
            return 0
        return (size - 4) // self._bucket_size()

    def _init_file(self, D: int):
        self._D = D
        with open(self.filename, "wb") as f:
            f.write(struct.pack("<i", D))
            empty = Bucket([], -1, self.schema).pack()
            for _ in range(1 << D):
                f.write(empty)

    def _hash(self, key: Any) -> int:
        k = _key_norm(key, self.key_kind)
        if self.hash_function:
            h = self.hash_function(k)
        else:
            if isinstance(k, int):
                h = k
            elif isinstance(k, float):
                h = _stable_hash_float(k)
            else:
                h = _stable_hash_str(str(k))
        return h & ((1 << self._D) - 1)
    def _read_bucket(self, index: int) -> Bucket:
        with open(self.filename, "rb") as f:
            off = self._bucket_offset(index)
            f.seek(off)
            buf = f.read(self._bucket_size())
        return Bucket.unpack(buf, self.schema)

    def _write_bucket(self, index: int, bucket: Bucket):
        with open(self.filename, "r+b") as f:
            off = self._bucket_offset(index)
            f.seek(off)
            f.write(bucket.pack())

    def _append_bucket(self, bucket: Bucket) -> int:
        with open(self.filename, "r+b") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            header = 4
            sz = self._bucket_size()
            index = (size - header) // sz
            f.write(bucket.pack())
            return index
    def insert(self, record: Dict[str, Any]):
        rec = self.schema.coerce_row(record)
        key = rec.get(self.key_field)
        if self.search(key) is not None:
            return

        idx = self._hash(key)
        b = self._read_bucket(idx)

        if len(b.records) < BLOCK_FACTOR:
            b.records.append(rec)
            self._write_bucket(idx, b)
            return
        chain_len = 0
        prev_index = idx
        prev_bucket = b

        while prev_bucket.next_bucket != -1:
            chain_len += 1
            if chain_len > MAX_CHAINING:
                break
            prev_index = prev_bucket.next_bucket
            prev_bucket = self._read_bucket(prev_index)
            if len(prev_bucket.records) < BLOCK_FACTOR:
                prev_bucket.records.append(rec)
                self._write_bucket(prev_index, prev_bucket)
                return
        if chain_len < MAX_CHAINING:
            new_index = self._append_bucket(Bucket([rec], -1, self.schema))
            last = self._read_bucket(prev_index)
            last.next_bucket = new_index
            self._write_bucket(prev_index, last)
            return

        self._rehash_and_insert(rec)

    def _rehash_and_insert(self, last_record: Dict[str, Any]):
        all_recs: List[Dict[str, Any]] = []
        sz = self._bucket_size()

        with open(self.filename, "rb") as f:
            f.seek(0, os.SEEK_END)
            file_size = f.tell()

        with open(self.filename, "rb") as f:
            f.seek(4)
            pos = 4
            while pos + sz <= file_size:
                buf = f.read(sz)
                pos += sz
                b = Bucket.unpack(buf, self.schema)
                all_recs.extend(b.records)
        all_recs.append(self.schema.coerce_row(last_record))
        self._D += 1
        with open(self.filename, "wb") as f:
            f.write(struct.pack("<i", self._D))
            empty = Bucket([], -1, self.schema).pack()
            for _ in range(1 << self._D):
                f.write(empty)

        for r in all_recs:
            self._simple_insert_after_rehash(r)

    def _simple_insert_after_rehash(self, record: Dict[str, Any]):
        key = record.get(self.key_field)
        idx = self._hash(key)
        b = self._read_bucket(idx)

        if len(b.records) < BLOCK_FACTOR:
            b.records.append(record)
            self._write_bucket(idx, b)
            return
        prev_index = idx
        prev_bucket = b
        while prev_bucket.next_bucket != -1:
            prev_index = prev_bucket.next_bucket
            prev_bucket = self._read_bucket(prev_index)
            if len(prev_bucket.records) < BLOCK_FACTOR:
                prev_bucket.records.append(record)
                self._write_bucket(prev_index, prev_bucket)
                return

        new_index = self._append_bucket(Bucket([record], -1, self.schema))
        last = self._read_bucket(prev_index)
        last.next_bucket = new_index
        self._write_bucket(prev_index, last)

    def search(self, key: Any) -> Optional[Dict[str, Any]]:
        idx = self._hash(_key_norm(key, self.key_kind))
        b = self._read_bucket(idx)

        k_norm = _key_norm(key, self.key_kind)
        cur = b
        while True:
            for rec in cur.records:
                if _key_norm(rec.get(self.key_field), self.key_kind) == k_norm:
                    return rec
            if cur.next_bucket == -1:
                break
            cur = self._read_bucket(cur.next_bucket)
        return None

    def remove(self, key: Any) -> bool:
        idx = self._hash(_key_norm(key, self.key_kind))
        b = self._read_bucket(idx)

        k_norm = _key_norm(key, self.key_kind)
        for i, rec in enumerate(b.records):
            if _key_norm(rec.get(self.key_field), self.key_kind) == k_norm:
                del b.records[i]
                self._write_bucket(idx, b)
                return True

        prev_index = idx
        prev_bucket = b
        while prev_bucket.next_bucket != -1:
            nxt_index = prev_bucket.next_bucket
            cur = self._read_bucket(nxt_index)
            for i, rec in enumerate(cur.records):
                if _key_norm(rec.get(self.key_field), self.key_kind) == k_norm:
                    del cur.records[i]
                    self._write_bucket(nxt_index, cur)
                    return True
            prev_index = nxt_index
            prev_bucket = cur

        return False

    def build_from_rows(self, rows: List[Dict[str, Any]]):
        for r in rows:
            self.insert(r)

    def iter_all(self):
        sz = self._bucket_size()
        with open(self.filename, "rb") as f:
            f.seek(4)
            while True:
                buf = f.read(sz)
                if not buf or len(buf) < sz:
                    break
                b = Bucket.unpack(buf, self.schema)
                for r in b.records:
                    yield r