from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Any, Protocol
import struct

# ----------------------------- Codec Layer -----------------------------------

class RecordCodec(Protocol):
    """Protocol for (de)serializing fixed-size rows to bytes, and extracting keys."""
    def record_size(self) -> int: ...
    def pack(self, row: Dict[str, Any]) -> bytes: ...
    def unpack(self, b: bytes) -> Dict[str, Any]: ...
    def key_of(self, row: Dict[str, Any]) -> int: ...

def _sfix(text: str, n: int) -> bytes:
    b = str(text).encode('utf-8', errors='ignore')[:n]
    return b + b'\x00' * (n - len(b))

def _sunfix(b: bytes) -> str:
    return b.rstrip(b'\x00').decode('utf-8', errors='ignore')


@dataclass
class EmployeeCodec(RecordCodec):
    """Example codec (112 bytes). """
    # <i 30s i 20s 20s 20s f 10s
    _STRUCT = struct.Struct('<i30si20s20s20sf10s')

    def record_size(self) -> int:
        return self._STRUCT.size  # 112

    def pack(self, row: Dict[str, Any]) -> bytes:
        return self._STRUCT.pack(
            int(row["employee_id"]),
            _sfix(row.get("name",""), 30),
            int(row.get("age",0)),
            _sfix(row.get("department",""), 20),
            _sfix(row.get("position",""), 20),
            _sfix(row.get("city",""), 20),
            float(row.get("salary",0.0)),
            _sfix(row.get("phone",""), 10),
        )

    def unpack(self, b: bytes) -> Dict[str, Any]:
        (emp_id, name, age, dept, pos, city, sal, phone) = self._STRUCT.unpack(b)
        return {
            "employee_id": emp_id,
            "name": _sunfix(name),
            "age": age,
            "department": _sunfix(dept),
            "position": _sunfix(pos),
            "city": _sunfix(city),
            "salary": sal,
            "phone": _sunfix(phone),
        }

    def key_of(self, row: Dict[str, Any]) -> int:
        return int(row["employee_id"])