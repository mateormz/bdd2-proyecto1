# schema.py
import struct
from dataclasses import dataclass
from enum import Enum

class Kind(Enum):
    INT='INT'; FLOAT='FLOAT'; CHAR='CHAR'; DATE='DATE'

@dataclass
class Field:
    name: str
    kind: Kind
    size: int = 0
    fmt: str = ''

class Schema:
    def __init__(self, fields, deleted_name='deleted'):
        self.fields = fields
        self.deleted_name = deleted_name
        parts = []
        for f in fields:
            if f.kind in (Kind.INT, Kind.FLOAT):
                parts.append(f.fmt)
            elif f.kind == Kind.CHAR:
                parts.append(f'{f.size}s')
            elif f.kind == Kind.DATE:
                parts.append('10s')
        self.fmt = '<' + ''.join(parts)
        self.size = struct.calcsize(self.fmt)
        self.map = {f.name:i for i,f in enumerate(fields)}

    def pack(self, row: dict) -> bytes:
        vals = []
        for f in self.fields:
            v = row.get(f.name)
            if f.kind == Kind.INT:   vals.append(int(v or 0))
            elif f.kind == Kind.FLOAT: vals.append(float(v or 0.0))
            elif f.kind == Kind.CHAR:
                vals.append((v or '').encode()[:f.size].ljust(f.size, b'\x00'))
            elif f.kind == Kind.DATE:
                vals.append((v or '').encode()[:10].ljust(10, b'\x00'))
        return struct.pack(self.fmt, *vals)

    def unpack(self, data: bytes) -> dict:
        tup = struct.unpack(self.fmt, data)
        row = {}
        for f, v in zip(self.fields, tup):
            if f.kind in (Kind.CHAR, Kind.DATE):
                row[f.name] = v.decode('utf-8','ignore').rstrip('\x00').rstrip()
            else:
                row[f.name] = v
        return row