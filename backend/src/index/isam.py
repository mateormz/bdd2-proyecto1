import struct, os, csv
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.core.schema import Schema, Field, Kind
from src.io_counters import count_read, count_write

def _project_root() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

OUT_DIR = os.path.join(_project_root(), "out")
os.makedirs(OUT_DIR, exist_ok=True)

BLOCK_FACTOR = 50
INDEX_BLOCK_FACTOR = 20

class KeyCodec:
    def __init__(self, key_field: Field):
        self.kind = key_field.kind
        self.char_size = None
        if self.kind == Kind.INT:
            self.fmt = "<i"
            self.key_size = struct.calcsize(self.fmt)
        elif self.kind == Kind.FLOAT:
            self.fmt = "<d"
            self.key_size = struct.calcsize(self.fmt)
        elif self.kind == Kind.CHAR:
            self.fmt = None
            self.char_size = int(key_field.size)
            self.key_size = self.char_size
        elif self.kind == Kind.DATE:
            self.fmt = None
            self.key_size = 10  # 'YYYY-MM-DD'
        else:
            raise ValueError(f"Tipo de clave no soportado en ISAM: {self.kind}")

    def _date_to_int(self, s: str) -> int:
        s = (s or "")[:10]
        if len(s) < 10 or s[4] != '-' or s[7] != '-':
            return -10**9
        y = int(s[0:4]); m = int(s[5:7]); d = int(s[8:10])
        return y*10000 + m*100 + d

    def norm(self, v):
        if v is None:
            if self.kind == Kind.INT:   return -2 ** 31
            if self.kind == Kind.FLOAT: return float("-inf")
            if self.kind == Kind.DATE:  return "0001-01-01"
            return ""
        if self.kind == Kind.INT:   return int(v)
        if self.kind == Kind.FLOAT: return float(v)
        if self.kind == Kind.DATE:
            s = str(v).strip()[:10]
            if len(s) != 10 or s[4] != '-' or s[7] != '-':
                return "0001-01-01"
            return s
        return str(v)

    def cmp(self, a, b) -> int:
        a = self.norm(a)
        b = self.norm(b)
        if a < b: return -1
        if a > b: return 1
        return 0

    def pack_key(self, v) -> bytes:
        if self.kind == Kind.INT:   return struct.pack(self.fmt, self.norm(v))
        if self.kind == Kind.FLOAT: return struct.pack(self.fmt, self.norm(v))
        if self.kind == Kind.DATE:
            s = str(v)[:10]
            if len(s) != 10: s = "0001-01-01"
            return s.encode("utf-8")
        bs = str(v).encode("utf-8", "ignore")[:self.key_size]
        return bs.ljust(self.key_size, b"\x00")

    def unpack_key(self, raw: bytes):
        if self.kind in (Kind.INT, Kind.FLOAT):
            (val,) = struct.unpack(self.fmt, raw)
            return val
        s = raw.decode("utf-8", "ignore").rstrip("\x00").rstrip()
        return s


class DataPage:
    HEADER_FORMAT = '<ii'
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    def __init__(self, schema: Schema, records=None, next_overflow: int = -1):
        self.schema = schema
        self.records = list(records) if records is not None else []
        self.next_overflow = next_overflow
        self.SIZE_OF_PAGE = self.HEADER_SIZE + BLOCK_FACTOR * schema.size

    def pack(self) -> bytes:
        header = struct.pack(self.HEADER_FORMAT, len(self.records), self.next_overflow)
        body = b''.join(self.schema.pack(r) for r in self.records)
        missing = BLOCK_FACTOR - len(self.records)
        if missing > 0:
            body += b'\x00' * (missing * self.schema.size)
        return header + body

    def unpack(self, data: bytes):
        size, next_overflow = struct.unpack(self.HEADER_FORMAT, data[:self.HEADER_SIZE])
        records = []
        off = self.HEADER_SIZE
        for _ in range(size):
            records.append(self.schema.unpack(data[off: off + self.schema.size]))
            off += self.schema.size
        return DataPage(self.schema, records, next_overflow)


class IndexNode:
    INT_SIZE = struct.calcsize('<i')

    def __init__(self, codec: KeyCodec, keys=None, pointers=None):
        self.codec = codec
        self.keys = list(keys) if keys is not None else []
        self.pointers = list(pointers) if pointers is not None else []

    @property
    def HEADER_FORMAT(self): return '<i'
    @property
    def HEADER_SIZE(self):   return struct.calcsize(self.HEADER_FORMAT)
    @property
    def SIZE_OF_NODE(self):  return self.HEADER_SIZE + (INDEX_BLOCK_FACTOR * self.codec.key_size) + ((INDEX_BLOCK_FACTOR + 1) * self.INT_SIZE)

    def pack(self) -> bytes:
        data = struct.pack(self.HEADER_FORMAT, len(self.keys))
        for i in range(INDEX_BLOCK_FACTOR):
            if i < len(self.keys): data += self.codec.pack_key(self.keys[i])
            else:                  data += b'\x00' * self.codec.key_size
        for i in range(INDEX_BLOCK_FACTOR + 1):
            if i < len(self.pointers): data += struct.pack('<i', int(self.pointers[i]))
            else:                      data += struct.pack('<i', -1)
        return data

    @staticmethod
    def unpack(raw: bytes, codec: KeyCodec):
        node = IndexNode(codec)
        off = 0
        (size,) = struct.unpack_from('<i', raw, off); off += struct.calcsize('<i')
        keys = []
        for i in range(INDEX_BLOCK_FACTOR):
            key_raw = raw[off:off + codec.key_size]; off += codec.key_size
            if i < size: keys.append(codec.unpack_key(key_raw))
        pointers = []
        for i in range(INDEX_BLOCK_FACTOR + 1):
            (p,) = struct.unpack_from('<i', raw, off); off += IndexNode.INT_SIZE
            if i < (size + 1): pointers.append(p)
        node.keys = keys; node.pointers = pointers
        return node


class ISAMFile:

    def __init__(self, filename, schema: Schema, key_field: str):
        base = (filename if os.path.isabs(filename) else os.path.join(OUT_DIR, os.path.basename(filename)))
        base_no_ext, _ = os.path.splitext(base)
        self.filename      = base
        self.filename_idx1 = base_no_ext + '_idx1'
        self.filename_idx2 = base_no_ext + '_idx2'

        self.schema    = schema
        self.key_field = key_field

        f = next((f for f in schema.fields if f.name == key_field), None)
        if f is None: raise ValueError(f"Campo clave '{key_field}' no existe en el Schema.")
        self.codec = KeyCodec(f)

        self.page_size = DataPage.HEADER_SIZE + BLOCK_FACTOR * schema.size

    # ---------- LECTURAS (contadas) ----------
    def _read_root(self) -> IndexNode | None:
        if not os.path.exists(self.filename_idx2): return None
        with open(self.filename_idx2, 'rb') as f:
            size = IndexNode(self.codec).SIZE_OF_NODE
            raw = f.read(size)
            count_read(len(raw))
        return IndexNode.unpack(raw, self.codec)

    def _read_level1_node(self, node_idx: int) -> IndexNode:
        with open(self.filename_idx1, 'rb') as f:
            size = IndexNode(self.codec).SIZE_OF_NODE
            f.seek(node_idx * size)
            raw = f.read(size)
            count_read(len(raw))
        return IndexNode.unpack(raw, self.codec)

    def _read_data_page(self, page_idx: int) -> DataPage:
        with open(self.filename, 'rb') as f:
            f.seek(page_idx * self.page_size)
            data = f.read(self.page_size)
            count_read(len(data))
        return DataPage(self.schema).unpack(data)

    # ---------- ESCRITURAS (contadas) ----------
    def _write_data_page(self, page_idx: int, page: DataPage):
        with open(self.filename, 'r+b') as f:
            f.seek(page_idx * self.page_size)
            payload = page.pack()
            n = f.write(payload)
            count_write(n if isinstance(n, int) and n >= 0 else len(payload))

    def _append_overflow_page(self, page: DataPage) -> int:
        with open(self.filename, 'r+b') as f:
            f.seek(0, os.SEEK_END)
            pos = f.tell()
            new_idx = pos // self.page_size
            payload = page.pack()
            n = f.write(payload)
            count_write(n if isinstance(n, int) and n >= 0 else len(payload))
        return new_idx

    # ---------- CONSTRUCCIÓN ----------
    def build_from_csv(self, csv_path: str, delimiter: str = ','):
        rows = []
        with open(csv_path, 'r', newline='', encoding='utf-8') as fh:
            rdr = csv.DictReader(fh, delimiter=delimiter)
            for r in rdr:
                if not r:
                    continue
                try:
                    _ = self.codec.norm(r[self.key_field])
                    rec = {f.name: r.get(f.name, None) for f in self.schema.fields
                           if f.name != self.schema.deleted_name}
                    if self.schema.deleted_name:
                        rec[self.schema.deleted_name] = 0
                    rec = self.schema.coerce_row(rec)
                    rows.append(rec)
                except Exception:
                    continue

        rows.sort(key=lambda rec: self.codec.norm(rec[self.key_field]))

        # páginas de datos
        first_keys_level0 = []
        with open(self.filename, 'wb') as f:
            for start in range(0, len(rows), BLOCK_FACTOR):
                chunk = rows[start:start + BLOCK_FACTOR]
                page = DataPage(self.schema, chunk, next_overflow=-1)
                payload = page.pack()
                n = f.write(payload)
                count_write(n if isinstance(n, int) and n >= 0 else len(payload))
                first_keys_level0.append(self.codec.norm(chunk[0][self.key_field]))

        num_data_pages = len(first_keys_level0)

        # nivel 1
        first_keys_level1 = []
        level1_nodes = []
        with open(self.filename_idx1, 'wb') as f:
            i = 0
            while i < num_data_pages:
                end = min(i + INDEX_BLOCK_FACTOR + 1, num_data_pages)
                chunk_keys = first_keys_level0[i:end]
                node_keys = chunk_keys[1:] if len(chunk_keys) > 1 else []
                node_ptrs = list(range(i, end))
                node = IndexNode(self.codec, keys=node_keys, pointers=node_ptrs)
                payload = node.pack()
                n = f.write(payload)
                count_write(n if isinstance(n, int) and n >= 0 else len(payload))
                first_keys_level1.append(chunk_keys[0])
                level1_nodes.append(len(level1_nodes))
                i = end

        # root (nivel 2)
        root_keys = first_keys_level1[1:] if len(first_keys_level1) > 1 else []
        root_ptrs = level1_nodes
        root = IndexNode(self.codec, keys=root_keys, pointers=root_ptrs)
        with open(self.filename_idx2, 'wb') as f:
            payload = root.pack()
            n = f.write(payload)
            count_write(n if isinstance(n, int) and n >= 0 else len(payload))

    # ---------- NAVEGACIÓN / OPS ----------
    def _locate_data_page(self, key_value) -> int:
        k = self.codec.norm(key_value)
        root = self._read_root()
        if root is None: return -1

        node_idx = 0
        for i, sep in enumerate(root.keys):
            if self.codec.cmp(k, sep) < 0:
                node_idx = root.pointers[i]; break
        else:
            node_idx = root.pointers[len(root.keys)]

        node = self._read_level1_node(node_idx)
        page_idx = 0
        for i, sep in enumerate(node.keys):
            if self.codec.cmp(k, sep) < 0:
                page_idx = node.pointers[i]; break
        else:
            page_idx = node.pointers[len(node.keys)]
        return page_idx

    def search(self, key_value):
        if not os.path.exists(self.filename):
            return None
        page_idx = self._locate_data_page(key_value)
        if page_idx < 0:
            return None

        k = self.codec.norm(key_value)
        while page_idx != -1:
            page = self._read_data_page(page_idx)
            for rec in page.records:
                if rec.get(self.schema.deleted_name, 0) == 0 and self.codec.cmp(rec[self.key_field], k) == 0:
                    return rec
            page_idx = page.next_overflow
        return None

    def rangeSearch(self, begin_key, end_key):
        if not os.path.exists(self.filename): return []
        a = self.codec.norm(begin_key); b = self.codec.norm(end_key)
        if self.codec.cmp(a, b) > 0: return []

        start = self._locate_data_page(begin_key)
        if start < 0: return []

        with open(self.filename, 'rb') as f:
            f.seek(0, os.SEEK_END)
            total_pages = f.tell() // self.page_size

        results = []
        page_idx = start
        while 0 <= page_idx < total_pages:
            current = page_idx
            while current != -1:
                page = self._read_data_page(current)
                for rec in page.records:
                    if rec.get(self.schema.deleted_name, 0) == 1:
                        continue
                    krec = self.codec.norm(rec[self.key_field])
                    if self.codec.cmp(krec, a) < 0:
                        continue
                    if self.codec.cmp(krec, b) > 0:
                        return results
                    results.append(rec)
                current = page.next_overflow
            page_idx += 1
        return results

    def insert(self, record: dict):
        if self.schema.deleted_name and record.get(self.schema.deleted_name) is None:
            record[self.schema.deleted_name] = 0
        record = self.schema.coerce_row(record)

        k = self.codec.norm(record[self.key_field])
        page_idx = self._locate_data_page(record[self.key_field])
        if page_idx < 0: return

        current = page_idx
        last = page_idx
        while current != -1:
            page = self._read_data_page(current)

            for i, rec in enumerate(page.records):
                if self.codec.cmp(rec[self.key_field], k) == 0:
                    if rec.get(self.schema.deleted_name, 0) == 1:
                        page.records[i] = record
                        self._write_data_page(current, page)
                    return

            if len(page.records) < BLOCK_FACTOR:
                pos = 0
                for i, rec in enumerate(page.records):
                    if self.codec.cmp(k, rec[self.key_field]) < 0:
                        pos = i; break
                else:
                    pos = len(page.records)
                page.records.insert(pos, record)
                self._write_data_page(current, page)
                return

            last = current
            current = page.next_overflow

        new_page = DataPage(self.schema, [record], next_overflow=-1)
        new_idx = self._append_overflow_page(new_page)
        last_page = self._read_data_page(last)
        last_page.next_overflow = new_idx
        self._write_data_page(last, last_page)

    def delete(self, key_value) -> bool:
        if not os.path.exists(self.filename):
            return False
        page_idx = self._locate_data_page(key_value)
        if page_idx < 0:
            return False

        k = self.codec.norm(key_value)
        page_idx = self._locate_data_page(k)
        if page_idx < 0: return False

        while page_idx != -1:
            page = self._read_data_page(page_idx)
            for i, rec in enumerate(page.records):
                if self.codec.cmp(rec[self.key_field], k) == 0:
                    if rec.get(self.schema.deleted_name, 0) == 1:
                        return False
                    page.records[i][self.schema.deleted_name] = 1
                    self._write_data_page(page_idx, page)
                    return True
                if self.codec.cmp(rec[self.key_field], k) > 0:
                    return False
            page_idx = page.next_overflow
        return False

    # ---------- SCAN / DEBUG ----------
    def scanAll(self):
        if not os.path.exists(self.filename):
            print("(archivo vacío)"); return
        with open(self.filename, 'rb') as f:
            f.seek(0, os.SEEK_END)
            total_pages = f.tell() // self.page_size
            f.seek(0)
            for p in range(total_pages):
                raw = f.read(self.page_size)
                count_read(len(raw))
                page = DataPage(self.schema).unpack(raw)
                act = [r for r in page.records if r.get(self.schema.deleted_name, 0) == 0]
                print(f"Page {p:3d} (overflow={page.next_overflow:3d}, size={len(page.records)}, activos={len(act)})")
                for r in act:
                    print("  ", r)

    def scanIndex(self):
        print("=== NIVEL 2 (Root) ===")
        root = self._read_root()
        if root:
            print("keys:", root.keys)
            print("ptrs:", root.pointers)
        print("\n=== NIVEL 1 ===")
        with open(self.filename_idx1, 'rb') as f:
            f.seek(0, os.SEEK_END)
            node_size = IndexNode(self.codec).SIZE_OF_NODE
            n = f.tell() // node_size
            f.seek(0)
            for i in range(n):
                raw = f.read(node_size)
                count_read(len(raw))
                node = IndexNode.unpack(raw, self.codec)
                print(f"Node {i}: keys={node.keys}, ptrs={node.pointers}")