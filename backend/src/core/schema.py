# backend/src/core/schema.py
import struct
from dataclasses import dataclass
from enum import Enum
from typing import List, Dict, Any

class Kind(Enum):
    INT = 'INT'
    FLOAT = 'FLOAT'
    CHAR = 'CHAR'
    DATE = 'DATE'

@dataclass
class Field:
    name: str
    kind: Kind
    size: int = 0      # solo para CHAR
    fmt: str = ''      # obligatorio para INT/FLOAT (p.ej. "i", "q", "f", "d")

class Schema:
    def __init__(self, fields: List[Field], deleted_name: str = 'deleted'):
        self.fields = fields
        self.deleted_name = deleted_name

        # Validación básica
        for f in self.fields:
            if f.kind in (Kind.INT, Kind.FLOAT) and not f.fmt:
                raise ValueError(f"Field '{f.name}' ({f.kind}) requiere fmt de struct (p.ej. 'i','q','f','d').")
            if f.kind == Kind.CHAR and f.size <= 0:
                raise ValueError(f"Field '{f.name}' (CHAR) requiere size > 0.")

        parts = []
        for f in fields:
            if f.kind in (Kind.INT, Kind.FLOAT):
                parts.append(f.fmt)
            elif f.kind == Kind.CHAR:
                parts.append(f'{f.size}s')
            elif f.kind == Kind.DATE:
                parts.append('10s')  # YYYY-MM-DD
        self.fmt = '<' + ''.join(parts)  # little-endian
        self.size = struct.calcsize(self.fmt)
        self.map: Dict[str, int] = {f.name: i for i, f in enumerate(fields)}

    # Normaliza un dict a los tipos del schema
    def coerce_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for f in self.fields:
            v = row.get(f.name, None)
            if f.kind == Kind.INT:
                out[f.name] = int(v or 0)
            elif f.kind == Kind.FLOAT:
                out[f.name] = float(v or 0.0)
            elif f.kind == Kind.CHAR:
                out[f.name] = "" if v is None else str(v)
            elif f.kind == Kind.DATE:
                out[f.name] = "" if v is None else str(v)[:10]
        return out

    def pack(self, row: Dict[str, Any]) -> bytes:
        row = self.coerce_row(row)
        vals = []
        for f in self.fields:
            v = row.get(f.name)
            if f.kind == Kind.INT:
                vals.append(int(v))
            elif f.kind == Kind.FLOAT:
                vals.append(float(v))
            elif f.kind == Kind.CHAR:
                vals.append(v.encode('utf-8', 'ignore')[:f.size].ljust(f.size, b'\x00'))
            elif f.kind == Kind.DATE:
                vals.append(v.encode('utf-8', 'ignore')[:10].ljust(10, b'\x00'))
        return struct.pack(self.fmt, *vals)

    def unpack(self, data: bytes) -> Dict[str, Any]:
        tup = struct.unpack(self.fmt, data)
        row: Dict[str, Any] = {}
        for f, v in zip(self.fields, tup):
            if f.kind in (Kind.CHAR, Kind.DATE):
                row[f.name] = v.decode('utf-8', 'ignore').rstrip('\x00').rstrip()
            else:
                row[f.name] = v
        return row