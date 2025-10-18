# Data header: magic 'AVLDAT01', version, record_size
from __future__ import annotations
from typing import Optional, List, Tuple, Dict, Any, Protocol
import io
import os
import struct

_DATA_HDR = struct.Struct('<8sIII')  # magic(8), version(u32), record_size(u32), reserved(u32)
_DATA_MAGIC = b'AVLDAT01'
_DATA_VERSION = 1

# Index header: magic 'AVLIDX01', version, node_size, root_off(u64), count(u32)
_INDEX_HDR = struct.Struct('<8sIIQI')
_INDEX_MAGIC = b'AVLIDX01'
_INDEX_VERSION = 1

# Index node layout:
#   key(i32), height(i32), left_off(u64), right_off(u64), value_off(u64)
_NODE = struct.Struct('<iiQQQ')  # 32 bytes
_NODE_SIZE = _NODE.size  # 32


class _DataFile:
    def __init__(self, path: str, record_size: int, create: bool):
        self.path = path
        self.record_size = record_size
        mode = 'r+b'
        if create or not os.path.exists(path):
            mode = 'w+b'
        self.f = open(path, mode)
        self.reads = 0
        self.writes = 0
        self._ensure_header()

    def _ensure_header(self):
        self.f.seek(0, io.SEEK_END)
        size = self.f.tell()
        if size == 0:
            hdr = _DATA_HDR.pack(_DATA_MAGIC, _DATA_VERSION, self.record_size, 0)
            self.f.seek(0); self.f.write(hdr); self.f.flush()
            self.writes += 1
        else:
            self.f.seek(0)
            hdr = self.f.read(_DATA_HDR.size); self.reads += 1
            magic, ver, rs, _ = _DATA_HDR.unpack(hdr)
            if magic != _DATA_MAGIC:
                raise ValueError("Bad data file magic")
            if ver != _DATA_VERSION:
                raise ValueError("Unsupported data file version")
            if rs != self.record_size:
                raise ValueError(f"Record size mismatch: file={rs}, expected={self.record_size}")

    @property
    def header_size(self) -> int:
        return _DATA_HDR.size

    def append(self, rec_bytes: bytes) -> int:
        if len(rec_bytes) != self.record_size:
            raise ValueError("Bad record size")
        self.f.seek(0, io.SEEK_END)
        off = self.f.tell()
        self.f.write(rec_bytes); self.f.flush()
        self.writes += 1
        return off

    def read_at(self, off: int) -> bytes:
        self.f.seek(off)
        b = self.f.read(self.record_size); self.reads += 1
        return b

    def rewrite_all(self, records: List[Tuple[int, bytes]]):
        self.f.seek(0)
        hdr = _DATA_HDR.pack(_DATA_MAGIC, _DATA_VERSION, self.record_size, 0)
        self.f.write(hdr); self.writes += 1
        for _, b in records:
            self.f.write(b); self.writes += 1
        self.f.flush()

    def close(self):
        self.f.close()


class IndexFile:
    def __init__(self, path: str, create: bool):
        self.path = path
        mode = 'r+b'
        if create or not os.path.exists(path):
            mode = 'w+b'
        self.f = open(path, mode)
        self.reads = 0
        self.writes = 0
        self._ensure_header()

    def _ensure_header(self):
        self.f.seek(0, io.SEEK_END)
        size = self.f.tell()
        if size == 0:
            hdr = _INDEX_HDR.pack(_INDEX_MAGIC, _INDEX_VERSION, _NODE_SIZE, 0, 0)
            self.f.seek(0); self.f.write(hdr); self.f.flush()
            self.writes += 1
        else:
            self.f.seek(0)
            hdr = self.f.read(_INDEX_HDR.size); self.reads += 1
            magic, ver, node_size, root_off, count = _INDEX_HDR.unpack(hdr)
            if magic != _INDEX_MAGIC:
                raise ValueError("Bad index file magic")
            if ver != _INDEX_VERSION:
                raise ValueError("Unsupported index file version")
            if node_size != _NODE_SIZE:
                raise ValueError(f"Node size mismatch: file={node_size}, expected={_NODE_SIZE}")
        self._read_header_cache()

    def _read_header_cache(self):
        self.f.seek(0)
        hdr = self.f.read(_INDEX_HDR.size); self.reads += 1
        magic, ver, node_size, root_off, count = _INDEX_HDR.unpack(hdr)
        self.root_off = root_off
        self.count = count

    def _write_header(self, *, root_off: Optional[int] = None, count: Optional[int] = None):
        if root_off is None: root_off = self.root_off
        if count    is None: count    = self.count
        hdr = _INDEX_HDR.pack(_INDEX_MAGIC, _INDEX_VERSION, _NODE_SIZE, root_off or 0, count or 0)
        self.f.seek(0); self.f.write(hdr); self.f.flush(); self.writes += 1
        self.root_off = root_off or 0
        self.count = count or 0

    @property
    def header_size(self) -> int:
        return _INDEX_HDR.size

    # ---- Node IO ----
    def _alloc_node(self, key: int, value_off: int, height: int = 1,
                    left_off: int = 0, right_off: int = 0) -> int:
        self.f.seek(0, io.SEEK_END)
        off = self.f.tell()
        self.f.write(_NODE.pack(key, height, left_off, right_off, value_off))
        self.f.flush(); self.writes += 1
        return off

    def read_node(self, off: int) -> Tuple[int, int, int, int, int]:
        self.f.seek(off)
        b = self.f.read(_NODE.size); self.reads += 1
        return _NODE.unpack(b)

    def write_node(self, off: int, key: int, height: int, left_off: int, right_off: int, value_off: int) -> None:
        self.f.seek(off)
        self.f.write(_NODE.pack(key, height, left_off, right_off, value_off))
        self.f.flush(); self.writes += 1

    def close(self):
        self.f.close()