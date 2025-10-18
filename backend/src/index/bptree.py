# B+ Tree Clustered File - Index

from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple
import os, csv, struct, bisect, pickle


PAGE_SIZE   = 4096
MAGIC       = b"BPTCFS1\0"
VERSION     = 1
ORDER_HINT  = 64
NUM_FMT     = "<"


L_NAME   = 32
L_GENDER = 8
L_DEPT   = 16
L_TITLE  = 16
L_EDU    = 12
L_LOC    = 16


RECORD_FORMAT = (
    NUM_FMT +
    "i" +
    f"{L_NAME}s" +
    "h" +
    f"{L_GENDER}s" +
    f"{L_DEPT}s" +
    f"{L_TITLE}s" +
    "i" +
    "h" +
    f"{L_EDU}s" +
    f"{L_LOC}s"
)
RECORD_SIZE = struct.calcsize(RECORD_FORMAT)

def _fixs(s: str, L: int) -> bytes:
    return (s or "")[:L].ljust(L).encode("utf-8", errors="ignore")

def _unfixs(b: bytes) -> str:
    return b.decode("utf-8", errors="ignore").rstrip()

@dataclass
class Record:
    employee_id: int
    name: str
    age: int
    gender: str
    department: str
    job_title: str
    salary: int
    experience_years: int
    education_level: str
    location: str

    def key(self) -> int:
        return int(self.employee_id)

    def pack(self) -> bytes:
        return struct.pack(
            RECORD_FORMAT,
            int(self.employee_id),
            _fixs(self.name, L_NAME),
            int(self.age),
            _fixs(self.gender, L_GENDER),
            _fixs(self.department, L_DEPT),
            _fixs(self.job_title, L_TITLE),
            int(self.salary),
            int(self.experience_years),
            _fixs(self.education_level, L_EDU),
            _fixs(self.location, L_LOC),
        )

    @staticmethod
    def unpack(buf: bytes) -> "Record":
        (employee_id,
         b_name,
         age,
         b_gender,
         b_dept,
         b_title,
         salary,
         experience_years,
         b_edu,
         b_loc) = struct.unpack(RECORD_FORMAT, buf)
        return Record(
            employee_id=employee_id,
            name=_unfixs(b_name),
            age=age,
            gender=_unfixs(b_gender),
            department=_unfixs(b_dept),
            job_title=_unfixs(b_title),
            salary=salary,
            experience_years=experience_years,
            education_level=_unfixs(b_edu),
            location=_unfixs(b_loc),
        )

class IOStats:
    def __init__(self): self.reads=0; self.writes=0
    def reset(self): self.reads=0; self.writes=0

IO = IOStats()

class Disk:
    HEADER_FMT  = "<8sIQQQ"
    HEADER_SIZE = struct.calcsize(HEADER_FMT)

    def __init__(self, filename: str):
        create = not os.path.exists(filename) or os.path.getsize(filename) == 0
        self.f = open(filename, "r+b" if not create else "w+b")
        if create:
            self.root_pid = 0
            self.free_head = 0
            self.num_pages = 1
            self._write_header()
        else:
            self._read_header()

    def _write_header(self):
        self.f.seek(0)
        hdr = struct.pack(self.HEADER_FMT, MAGIC, VERSION, self.root_pid, self.free_head, self.num_pages)
        self.f.write(hdr + (b"\x00" * (PAGE_SIZE - len(hdr))))
        IO.writes += 1

    def _read_header(self):
        self.f.seek(0)
        raw = self.f.read(PAGE_SIZE); IO.reads += 1
        magic, version, self.root_pid, self.free_head, self.num_pages = struct.unpack(self.HEADER_FMT, raw[:self.HEADER_SIZE])
        if magic != MAGIC:
            raise ValueError("Archivo no es B+ Clustered válido")

    def alloc(self) -> int:
        if self.free_head != 0:
            pid = self.free_head
            self.f.seek(pid * PAGE_SIZE)
            raw = self.f.read(PAGE_SIZE); IO.reads += 1
            next_free = struct.unpack("<Q", raw[:8])[0]
            self.free_head = next_free
            self._write_header()
            return pid
        pid = self.num_pages
        self.write_raw(pid, b"\x00")
        self.num_pages += 1
        self._write_header()
        return pid

    def free(self, pid: int):
        buf = struct.pack("<Q", self.free_head)
        self.write_raw(pid, buf)
        self.free_head = pid
        self._write_header()

    def write_raw(self, pid: int, buf: bytes):
        if len(buf) > PAGE_SIZE: raise ValueError("Página excede PAGE_SIZE")
        self.f.seek(pid * PAGE_SIZE)
        self.f.write(buf + (b"\x00" * (PAGE_SIZE - len(buf))))
        IO.writes += 1

    def read_raw(self, pid: int) -> bytes:
        self.f.seek(pid * PAGE_SIZE)
        raw = self.f.read(PAGE_SIZE); IO.reads += 1
        return raw

    def set_root(self, pid: int):
        self.root_pid = pid
        self._write_header()

    def close(self):
        self.f.flush(); self.f.close()


LEAF_HDR_FMT  = "<ciii"
LEAF_HDR_SIZE = struct.calcsize(LEAF_HDR_FMT)

BLOCK_FACTOR = max(1, (PAGE_SIZE - LEAF_HDR_SIZE) // RECORD_SIZE)

class LeafPage:
    def __init__(self, recs: Optional[List[Record]] = None, prev_leaf: int = 0, next_leaf: int = 0):
        self.recs: List[Record] = [] if recs is None else recs
        self.prev_leaf = prev_leaf
        self.next_leaf = next_leaf

    def keys(self) -> List[int]:
        return [r.employee_id for r in self.recs]

    def pack(self) -> bytes:
        if len(self.recs) > BLOCK_FACTOR:
            raise ValueError("Overflow en pack()")
        out = [struct.pack(LEAF_HDR_FMT, b"L", len(self.recs), self.prev_leaf, self.next_leaf)]
        out.extend(r.pack() for r in self.recs)
        if len(self.recs) < BLOCK_FACTOR:
            out.append(b"\x00" * ((BLOCK_FACTOR - len(self.recs)) * RECORD_SIZE))
        return b"".join(out)

    @staticmethod
    def unpack(buf: bytes) -> "LeafPage":
        tag, count, prev_leaf, next_leaf = struct.unpack(LEAF_HDR_FMT, buf[:LEAF_HDR_SIZE])
        assert tag == b"L", "Página no es hoja"
        recs: List[Record] = []
        off = LEAF_HDR_SIZE
        for _ in range(count):
            recs.append(Record.unpack(buf[off:off+RECORD_SIZE]))
            off += RECORD_SIZE
        return LeafPage(recs, prev_leaf, next_leaf)

@dataclass
class InternalNode:
    is_leaf: bool
    keys: List[int]
    children: List[int]

def write_internal(dsk: Disk, pid: int, node: InternalNode):
    data = pickle.dumps({"is_leaf": False, "keys": node.keys, "children": node.children}, protocol=pickle.HIGHEST_PROTOCOL)
    dsk.write_raw(pid, data)

def read_internal(dsk: Disk, pid: int) -> InternalNode:
    raw = dsk.read_raw(pid)
    obj = pickle.loads(raw[:raw.rfind(b".")+1])
    return InternalNode(False, obj["keys"], obj["children"])

def is_leaf_page(raw: bytes) -> bool:
    return len(raw) >= 1 and raw[0:1] == b"L"

class BPlusClustered:
    def __init__(self, filename: str, order_hint: int = ORDER_HINT):
        self.dsk = Disk(filename)
        self.order = order_hint
        if self.dsk.root_pid == 0:
            # crear primera hoja vacía
            leaf_pid = self.dsk.alloc()
            self.dsk.write_raw(leaf_pid, LeafPage().pack())
            self.dsk.set_root(leaf_pid)


    def _load(self, pid: int):
        raw = self.dsk.read_raw(pid)
        if is_leaf_page(raw): return LeafPage.unpack(raw), True
        return read_internal(self.dsk, pid), False

    def _save(self, pid: int, node, is_leaf: bool):
        if is_leaf: self.dsk.write_raw(pid, node.pack())
        else:       write_internal(self.dsk, pid, node)

    def search(self, key: int) -> List[Record]:
        pid = self.dsk.root_pid
        node, is_leaf = self._load(pid)
        while not is_leaf:
            idx = bisect.bisect_left(node.keys, key)
            pid = node.children[idx]
            node, is_leaf = self._load(pid)
        keys = [r.employee_id for r in node.recs]
        i = bisect.bisect_left(keys, key)
        out: List[Record] = []
        while i < len(node.recs) and node.recs[i].employee_id == key:
            out.append(node.recs[i]); i += 1
        return out

    def range_search(self, lo: int, hi: int) -> Iterable[Record]:
        pid = self.dsk.root_pid
        node, is_leaf = self._load(pid)
        while not is_leaf:
            idx = bisect.bisect_left(node.keys, lo)
            pid = node.children[idx]
            node, is_leaf = self._load(pid)
        while True:
            keys = [r.employee_id for r in node.recs]
            i = bisect.bisect_left(keys, lo)
            while i < len(node.recs) and node.recs[i].employee_id <= hi:
                yield node.recs[i]; i += 1
            if node.next_leaf == 0: break
            nxt, leaf = self._load(node.next_leaf)
            if not leaf or (len(nxt.recs) and nxt.recs[0].employee_id > hi): break
            node = nxt

    def insert(self, key: int, rec: Record):
        split = self._ins_rec(self.dsk.root_pid, key, rec)
        if split is not None:
            sep_key, right_pid = split
            old_root = self.dsk.root_pid
            new_root = InternalNode(False, [sep_key], [old_root, right_pid])
            new_pid = self.dsk.alloc()
            self._save(new_pid, new_root, False)
            self.dsk.set_root(new_pid)

    def _ins_rec(self, pid: int, key: int, rec: Record):
        node, is_leaf = self._load(pid)
        if is_leaf:
            pos = bisect.bisect_right([r.employee_id for r in node.recs], key)
            node.recs.insert(pos, rec)
            if len(node.recs) <= BLOCK_FACTOR:
                self._save(pid, node, True)
                return None

            mid = len(node.recs) // 2
            right_recs = node.recs[mid:]
            node.recs   = node.recs[:mid]
            right = LeafPage(right_recs, prev_leaf=pid, next_leaf=node.next_leaf)
            right_pid = self.dsk.alloc()
            node.next_leaf = right_pid
            self._save(pid, node, True)
            self._save(right_pid, right, True)
            sep_key = right.recs[0].employee_id
            return (sep_key, right_pid)
        else:
            idx = bisect.bisect_left(node.keys, key)
            child_pid = node.children[idx]
            split = self._ins_rec(child_pid, key, rec)
            if split is None: return None
            sep, right_pid = split
            node.keys.insert(idx, sep)
            node.children.insert(idx+1, right_pid)
            if len(node.keys) <= self.order:
                self._save(pid, node, False)
                return None

            mid = len(node.keys) // 2
            sep_up = node.keys[mid]
            right = InternalNode(False, node.keys[mid+1:], node.children[mid+1:])
            node.keys = node.keys[:mid]
            node.children = node.children[:mid+1]
            right_pid2 = self.dsk.alloc()
            self._save(pid, node, False)
            self._save(right_pid2, right, False)
            return (sep_up, right_pid2)


    def remove(self, key: int, only_first: bool = False) -> int:
        removed = self._rem_rec(self.dsk.root_pid, key, only_first)
        root, leaf = self._load(self.dsk.root_pid)
        if (not leaf) and len(root.children) == 1:
            self.dsk.set_root(root.children[0])
        return removed

    def _rem_rec(self, pid: int, key: int, only_first: bool) -> int:
        node, is_leaf = self._load(pid)
        if is_leaf:
            keys = [r.employee_id for r in node.recs]
            i = bisect.bisect_left(keys, key)
            cnt = 0
            while i < len(node.recs) and node.recs[i].employee_id == key:
                node.recs.pop(i); cnt += 1
                if only_first: break
            self._save(pid, node, True)
            return cnt
        idx = bisect.bisect_left(node.keys, key)
        child_pid = node.children[idx]
        removed = self._rem_rec(child_pid, key, only_first)
        if removed == 0: return 0
        self._save(pid, node, False)
        return removed

    def close(self):
        self.dsk.close()


def row_to_record(row: Dict[str, str]) -> Record:
    return Record(
        employee_id      = int(row["Employee_ID"]),
        name             = row["Name"],
        age              = int(row["Age"]),
        gender           = row["Gender"],
        department       = row["Department"],
        job_title        = row["Job_Title"],
        salary           = int(row["Salary"]),
        experience_years = int(row["Experience_Years"]),
        education_level  = row["Education_Level"],
        location         = row["Location"],
    )


if __name__ == "__main__":
    csv_path = "../../data/Employers_data.csv"
    if os.path.exists("employees.bpt"): os.remove("employees.bpt")
    bpt = BPlusClustered("employees.bpt", order_hint=64)


    rows: List[Dict[str, Any]] = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        for r in rdr:
            rows.append(r)

    recs = [row_to_record(r) for r in rows]
    for r in recs:
        bpt.insert(r.employee_id, r)

    if recs:
        k = recs[len(recs)//2].employee_id
        print("search(", k, ") ->", len(bpt.search(k)))

        lo = recs[len(recs)//3].employee_id
        hi = recs[len(recs)//3 * 2].employee_id
        lo, hi = min(lo, hi), max(lo, hi)
        cnt = sum(1 for _ in bpt.range_search(lo, hi))
        print(f"range_search({lo}..{hi}) ->", cnt)

        rem = bpt.remove(k, only_first=True)
        print("remove(", k, ") ->", rem)

    print("Leaf BLOCK_FACTOR:", BLOCK_FACTOR)
    print("I/O reads:", IO.reads, "writes:", IO.writes)
    bpt.close()