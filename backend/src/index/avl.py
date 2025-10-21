from __future__ import annotations
import os, io, struct, csv
from typing import Dict, Any, List, Tuple, Optional
from io_counters import count_read, count_write

def _sfix(text: str, n: int) -> bytes:
    b = str(text).encode('utf-8', errors='ignore')[:n]
    return b + b'\x00' * (n - len(b))

def _sunfix(b: bytes) -> str:
    return b.rstrip(b'\x00').decode('utf-8', errors='ignore')

class EmployeeCodec:
    _STRUCT = struct.Struct('<i30si20s20s20sf10s')
    def record_size(self) -> int:
        return self._STRUCT.size
    def pack(self, row: Dict[str, Any]) -> bytes:
        return self._STRUCT.pack(
            int(row["employee_id"]),
            _sfix(row.get("name", ""), 30),
            int(row.get("age", 0)),
            _sfix(row.get("department", ""), 20),
            _sfix(row.get("position", ""), 20),
            _sfix(row.get("city", ""), 20),
            float(row.get("salary", 0.0)),
            _sfix(row.get("phone", ""), 10)
        )
    def unpack(self, b: bytes) -> Dict[str, Any]:
        emp_id, name, age, dept, pos, city, sal, phone = self._STRUCT.unpack(b)
        return {
            "employee_id": emp_id,
            "name": _sunfix(name),
            "age": age,
            "department": _sunfix(dept),
            "position": _sunfix(pos),
            "city": _sunfix(city),
            "salary": sal,
            "phone": _sunfix(phone)
        }
    def key_of(self, row: Dict[str, Any]) -> int:
        return int(row["employee_id"])

_DATA_HDR = struct.Struct('<8sIII')
_DATA_MAGIC = b'AVLDAT01'
_DATA_VERSION = 1

_INDEX_HDR = struct.Struct('<8sIIQI')
_INDEX_MAGIC = b'AVLIDX01'
_INDEX_VERSION = 1
_NODE = struct.Struct('<iiQQQ')
_NODE_SIZE = _NODE.size

class DataFile:
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
        if self.f.tell() == 0:
            hdr = _DATA_HDR.pack(_DATA_MAGIC, _DATA_VERSION, self.record_size, 0)
            self.f.seek(0); self.f.write(hdr); self.f.flush()
            self.writes += 1
            count_write()
        else:
            self.f.seek(0)
            hdr = self.f.read(_DATA_HDR.size)
            self.reads += 1
            count_read()
            magic, ver, rs, _ = _DATA_HDR.unpack(hdr)
            if magic != _DATA_MAGIC or ver != _DATA_VERSION or rs != self.record_size:
                raise ValueError("Encabezado inválido en archivo de datos")
    @property
    def header_size(self): return _DATA_HDR.size
    def append(self, rec_bytes: bytes) -> int:
        self.f.seek(0, io.SEEK_END)
        off = self.f.tell()
        self.f.write(rec_bytes); self.f.flush()
        self.writes += 1
        count_write()
        return off
    def read_at(self, off: int) -> bytes:
        self.f.seek(off)
        b = self.f.read(self.record_size)
        self.reads += 1
        count_read()
        return b
    def rewrite_all(self, records: List[Tuple[int, bytes]]):
        self.f.seek(0)
        hdr = _DATA_HDR.pack(_DATA_MAGIC, _DATA_VERSION, self.record_size, 0)
        self.f.write(hdr)
        self.writes += 1
        count_write()
        for _, b in records:
            self.f.write(b)
            self.writes += 1
            count_write()
        self.f.flush()
    def close(self): self.f.close()

class IndexFile:
    def __init__(self, path: str, create: bool):
        self.path = path
        mode = 'r+b'
        if create or not os.path.exists(path):
            mode = 'w+b'
        self.f = open(path, mode)
        self.reads = 0; self.writes = 0
        self._ensure_header()
    def _ensure_header(self):
        self.f.seek(0, io.SEEK_END)
        if self.f.tell() == 0:
            hdr = _INDEX_HDR.pack(_INDEX_MAGIC, _INDEX_VERSION, _NODE_SIZE, 0, 0)
            self.f.seek(0); self.f.write(hdr); self.f.flush()
            self.writes += 1
            count_write()
        self._read_header()
    def _read_header(self):
        self.f.seek(0)
        hdr = self.f.read(_INDEX_HDR.size)
        self.reads += 1
        count_read()
        magic, ver, node_size, root_off, count = _INDEX_HDR.unpack(hdr)
        self.root_off, self.count = root_off, count
    def _write_header(self, root_off=None, count=None):
        if root_off is None: root_off = self.root_off
        if count is None: count = self.count
        hdr = _INDEX_HDR.pack(_INDEX_MAGIC, _INDEX_VERSION, _NODE_SIZE, root_off or 0, count or 0)
        self.f.seek(0); self.f.write(hdr); self.f.flush()
        self.writes += 1
        count_write()
        self.root_off, self.count = root_off or 0, count or 0
    def _alloc_node(self, key: int, value_off: int, height=1, left_off=0, right_off=0) -> int:
        self.f.seek(0, io.SEEK_END)
        off = self.f.tell()
        self.f.write(_NODE.pack(key, height, left_off, right_off, value_off)); self.f.flush()
        self.writes += 1
        count_write()
        return off
    def read_node(self, off: int) -> Tuple[int, int, int, int, int]:
        self.f.seek(off)
        b = self.f.read(_NODE.size)
        self.reads += 1
        count_read()
        return _NODE.unpack(b)
    def write_node(self, off: int, k: int, h: int, l: int, r: int, v: int):
        self.f.seek(off)
        self.f.write(_NODE.pack(k, h, l, r, v)); self.f.flush()
        self.writes += 1
        count_write()
    def close(self): self.f.close()

def _height(idx: IndexFile, off: int) -> int:
    if not off: return 0
    _, h, _, _, _ = idx.read_node(off)
    return h

def _update_height(idx: IndexFile, off: int) -> int:
    if not off: return 0
    k, _, l, r, v = idx.read_node(off)
    h = 1 + max(_height(idx, l), _height(idx, r))
    idx.write_node(off, k, h, l, r, v)
    return h

def _balance_factor(idx: IndexFile, off: int) -> int:
    if not off: return 0
    _, _, l, r, _ = idx.read_node(off)
    return _height(idx, l) - _height(idx, r)

def _rotate_right(idx: IndexFile, y_off: int) -> int:
    k_y, h_y, x_off, r_y, v_y = idx.read_node(y_off)
    k_x, h_x, a_off, b_off, v_x = idx.read_node(x_off)
    idx.write_node(y_off, k_y, h_y, b_off, r_y, v_y)
    idx.write_node(x_off, k_x, h_x, a_off, y_off, v_x)
    _update_height(idx, y_off); _update_height(idx, x_off)
    return x_off

def _rotate_left(idx: IndexFile, x_off: int) -> int:
    k_x, h_x, l_x, y_off, v_x = idx.read_node(x_off)
    k_y, h_y, b_off, c_off, v_y = idx.read_node(y_off)
    idx.write_node(x_off, k_x, h_x, l_x, b_off, v_x)
    idx.write_node(y_off, k_y, h_y, x_off, c_off, v_y)
    _update_height(idx, x_off); _update_height(idx, y_off)
    return y_off

def _rebalance(idx: IndexFile, off: int) -> int:
    if not off: return 0
    _update_height(idx, off)
    bf = _balance_factor(idx, off)
    if bf > 1:
        k, h, l, r, v = idx.read_node(off)
        if _balance_factor(idx, l) < 0:
            new_l = _rotate_left(idx, l)
            k2, h2, _, r2, v2 = idx.read_node(off)
            idx.write_node(off, k2, h2, new_l, r2, v2)
        return _rotate_right(idx, off)
    if bf < -1:
        k, h, l, r, v = idx.read_node(off)
        if _balance_factor(idx, r) > 0:
            new_r = _rotate_right(idx, r)
            k2, h2, l2, _, v2 = idx.read_node(off)
            idx.write_node(off, k2, h2, l2, new_r, v2)
        return _rotate_left(idx, off)
    return off

def _insert(idx: IndexFile, off: int, key: int, value_off: int) -> int:
    if not off:
        return idx._alloc_node(key, value_off)
    k, h, l, r, v = idx.read_node(off)
    if key < k:
        new_l = _insert(idx, l, key, value_off)
        k, h, _, r, v = idx.read_node(off)
        idx.write_node(off, k, h, new_l, r, v)
    elif key > k:
        new_r = _insert(idx, r, key, value_off)
        k, h, l, _, v = idx.read_node(off)
        idx.write_node(off, k, h, l, new_r, v)
    else:
        new_r = _insert(idx, r, key, value_off)
        k, h, l, _, v = idx.read_node(off)
        idx.write_node(off, k, h, l, new_r, v)
    return _rebalance(idx, off)

def _search(idx: IndexFile, off: int, key: int, out: List[int]):
    if not off: return
    k, h, l, r, v = idx.read_node(off)
    if key < k: _search(idx, l, key, out)
    elif key > k: _search(idx, r, key, out)
    else:
        out.append(v)
        _search(idx, l, key, out)
        _search(idx, r, key, out)

def _range(idx: IndexFile, off: int, lo: int, hi: int, out: List[int]):
    if not off: return
    k, h, l, r, v = idx.read_node(off)
    if lo < k: _range(idx, l, lo, hi, out)
    if lo <= k <= hi: out.append(v)
    if k < hi: _range(idx, r, lo, hi, out)

class AVLFile:
    def __init__(self, data_path: str, index_path: str, create: bool = False):
        self.codec = EmployeeCodec()
        self.data = DataFile(data_path, self.codec.record_size(), create)
        self.index = IndexFile(index_path, create)
    def add(self, row: Dict[str, Any]) -> None:
        packed = self.codec.pack(row)
        data_off = self.data.append(packed)
        key = self.codec.key_of(row)
        new_root = _insert(self.index, self.index.root_off, key, data_off)
        self.index._write_header(root_off=new_root, count=self.index.count + 1)
    def search(self, key: int) -> List[Dict[str, Any]]:
        offs: List[int] = []
        _search(self.index, self.index.root_off, key, offs)
        return [self.codec.unpack(self.data.read_at(off)) for off in offs]
    def rangeSearch(self, lo: int, hi: int) -> List[Dict[str, Any]]:
        offs: List[int] = []
        _range(self.index, self.index.root_off, lo, hi, offs)
        return [self.codec.unpack(self.data.read_at(off)) for off in offs]
    def io_stats(self) -> Dict[str, int]:
        return {
            "data_reads": self.data.reads,
            "data_writes": self.data.writes,
            "index_reads": self.index.reads,
            "index_writes": self.index.writes
        }
