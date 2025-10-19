import struct, os, csv
from core.schema import Schema, Field, Kind

BLOCK_FACTOR = 50  # Factor de bloque para páginas de datos
INDEX_BLOCK_FACTOR = 20  # Factor de bloque para nodos de índice

class DataPage:
    """Nivel 0: Página de datos con overflow chaining"""
    HEADER_FORMAT = '<ii'
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
    
    def __init__(self, schema: Schema, records=None, next_overflow: int = -1):
        self.schema = schema
        self.records = list(records) if records is not None else []
        self.next_overflow = next_overflow
        self.SIZE_OF_PAGE = self.HEADER_SIZE + BLOCK_FACTOR * schema.size

    def pack(self) -> bytes:
        header = struct.pack(self.HEADER_FORMAT, len(self.records), self.next_overflow)
        records_data = b''.join(self.schema.pack(r) for r in self.records)
        missing = BLOCK_FACTOR - len(self.records)
        if missing > 0:
            records_data += b'\x00' * (missing * self.schema.size)
        return header + records_data

    def unpack(self, data: bytes):
        size, next_overflow = struct.unpack(self.HEADER_FORMAT, data[:self.HEADER_SIZE])
        records = []
        offset = self.HEADER_SIZE
        for _ in range(size):
            record_data = data[offset: offset + self.schema.size]
            records.append(self.schema.unpack(record_data))
            offset += self.schema.size
        return DataPage(self.schema, records, next_overflow)

class IndexNode:
    """Nodo de índice (niveles 1 y 2)"""
    HEADER_FORMAT = '<i'
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
    INT_SIZE = struct.calcsize('<i')
    SIZE_OF_NODE = HEADER_SIZE + (INDEX_BLOCK_FACTOR * INT_SIZE) + ((INDEX_BLOCK_FACTOR + 1) * INT_SIZE)

    def __init__(self, keys=None, pointers=None):
        self.keys = list(keys) if keys is not None else []
        self.pointers = list(pointers) if pointers is not None else []

    def pack(self) -> bytes:
        size = len(self.keys)
        data = struct.pack(IndexNode.HEADER_FORMAT, size)
        
        # Empaquetar keys (separator keys)
        for i in range(INDEX_BLOCK_FACTOR):
            if i < len(self.keys):
                data += struct.pack('<i', int(self.keys[i]))
            else:
                data += struct.pack('<i', 0)
        
        # Empaquetar pointers
        for i in range(INDEX_BLOCK_FACTOR + 1):
            if i < len(self.pointers):
                data += struct.pack('<i', int(self.pointers[i]))
            else:
                data += struct.pack('<i', -1)
        
        return data

    @staticmethod
    def unpack(raw: bytes):
        offset = 0
        (size,) = struct.unpack_from(IndexNode.HEADER_FORMAT, raw, offset)
        offset += IndexNode.HEADER_SIZE
        
        keys = []
        for i in range(INDEX_BLOCK_FACTOR):
            (k,) = struct.unpack_from('<i', raw, offset)
            offset += IndexNode.INT_SIZE
            if i < size:
                keys.append(k)
        
        pointers = []
        for i in range(INDEX_BLOCK_FACTOR + 1):
            (p,) = struct.unpack_from('<i', raw, offset)
            offset += IndexNode.INT_SIZE
            if i < (size + 1):
                pointers.append(p)
        
        return IndexNode(keys=keys, pointers=pointers)

class ISAMFile:
    """
    ISAM de 3 niveles:
    - Nivel 2 (root): índice superior
    - Nivel 1: índice intermedio
    - Nivel 0: páginas de datos
    """
    def __init__(self, filename, schema: Schema, key_field: str):
        self.filename = filename  # Nivel 0: datos
        self.filename_idx1 = filename + '_idx1'  # Nivel 1: índice intermedio
        self.filename_idx2 = filename + '_idx2'  # Nivel 2: índice root
        self.schema = schema
        self.key_field = key_field
        self.page_size = DataPage.HEADER_SIZE + BLOCK_FACTOR * schema.size

    def build_from_csv(self, csv_path: str, delimiter: str = ','):
        """Construye el ISAM de 3 niveles a partir de un CSV"""
        # 1. Leer y ordenar registros
        rows = []
        with open(csv_path, 'r', newline='', encoding='utf-8') as fh:
            reader = csv.reader(fh, delimiter=delimiter)
            for row in reader:
                if not row:
                    continue
                try:
                    record = {}
                    for i, field in enumerate(self.schema.fields):
                        if i < len(row):
                            record[field.name] = row[i]
                    # Validar key field
                    int(record[self.key_field])
                    rows.append(record)
                except (ValueError, KeyError):
                    continue

        rows.sort(key=lambda r: int(r[self.key_field]))

        # 2. NIVEL 0: Crear páginas de datos
        first_keys_level0 = []  # Primer key de cada página de datos
        
        with open(self.filename, 'wb') as f:
            for start in range(0, len(rows), BLOCK_FACTOR):
                chunk = rows[start:start + BLOCK_FACTOR]
                page = DataPage(self.schema, chunk, next_overflow=-1)
                f.write(page.pack())
                first_keys_level0.append(int(chunk[0][self.key_field]))

        num_data_pages = len(first_keys_level0)
        print(f"Nivel 0: {num_data_pages} páginas de datos creadas")

        # 3. NIVEL 1: Crear nodos de índice intermedio
        # Cada nodo de nivel 1 agrupa hasta (INDEX_BLOCK_FACTOR + 1) páginas de datos
        first_keys_level1 = []
        level1_nodes = []
        
        with open(self.filename_idx1, 'wb') as f:
            for start in range(0, num_data_pages, INDEX_BLOCK_FACTOR + 1):
                end = min(start + INDEX_BLOCK_FACTOR + 1, num_data_pages)
                chunk_keys = first_keys_level0[start:end]
                
                # Separator keys: todos excepto el primero
                node_keys = chunk_keys[1:] if len(chunk_keys) > 1 else []
                # Pointers: índices de las páginas de datos
                node_pointers = list(range(start, end))
                
                node = IndexNode(keys=node_keys, pointers=node_pointers)
                f.write(node.pack())
                
                # Guardar el primer key de este nodo para el nivel 2
                first_keys_level1.append(chunk_keys[0])
                level1_nodes.append(len(level1_nodes))

        num_level1_nodes = len(level1_nodes)
        print(f"Nivel 1: {num_level1_nodes} nodos de índice creados")

        # 4. NIVEL 2: Crear nodo raíz
        # El root agrupa todos los nodos de nivel 1
        root_keys = first_keys_level1[1:] if len(first_keys_level1) > 1 else []
        root_pointers = level1_nodes
        
        root = IndexNode(keys=root_keys, pointers=root_pointers)
        with open(self.filename_idx2, 'wb') as f:
            f.write(root.pack())
        
        print(f"Nivel 2: Nodo root creado con {len(root_keys)} keys")

    def _read_root(self) -> IndexNode:
        """Lee el nodo raíz (nivel 2)"""
        if not os.path.exists(self.filename_idx2):
            return None
        with open(self.filename_idx2, 'rb') as f:
            raw = f.read(IndexNode.SIZE_OF_NODE)
        return IndexNode.unpack(raw)

    def _read_level1_node(self, node_idx: int) -> IndexNode:
        """Lee un nodo de nivel 1"""
        with open(self.filename_idx1, 'rb') as f:
            f.seek(node_idx * IndexNode.SIZE_OF_NODE)
            raw = f.read(IndexNode.SIZE_OF_NODE)
        return IndexNode.unpack(raw)

    def _read_data_page(self, page_idx: int) -> DataPage:
        """Lee una página de datos (nivel 0)"""
        with open(self.filename, 'rb') as f:
            f.seek(page_idx * self.page_size)
            data = f.read(self.page_size)
        page = DataPage(self.schema)
        return page.unpack(data)

    def _write_data_page(self, page_idx: int, page: DataPage):
        """Escribe una página de datos"""
        with open(self.filename, 'r+b') as f:
            f.seek(page_idx * self.page_size)
            f.write(page.pack())

    def _append_overflow_page(self, page: DataPage) -> int:
        """Añade una overflow page al final del archivo de datos"""
        with open(self.filename, 'r+b') as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            new_index = size // self.page_size
            f.write(page.pack())
        return new_index

    def _locate_data_page(self, key_value: int) -> int:
        """Navega los 2 niveles de índice para encontrar la página de datos"""
        # NIVEL 2: Root
        root = self._read_root()
        if root is None:
            return -1

        # Buscar en el root qué nodo de nivel 1 visitar
        level1_node_idx = 0
        for i, key in enumerate(root.keys):
            if key_value < key:
                level1_node_idx = root.pointers[i]
                break
        else:
            level1_node_idx = root.pointers[len(root.keys)]

        # NIVEL 1: Nodo intermedio
        level1_node = self._read_level1_node(level1_node_idx)
        
        # Buscar en nivel 1 qué página de datos visitar
        data_page_idx = 0
        for i, key in enumerate(level1_node.keys):
            if key_value < key:
                data_page_idx = level1_node.pointers[i]
                break
        else:
            data_page_idx = level1_node.pointers[len(level1_node.keys)]

        return data_page_idx

    def search(self, key_value: int):
        """Busca un registro navegando los 3 niveles"""
        if not os.path.exists(self.filename):
            return None

        # Navegar índices (nivel 2 y 1) para llegar a la página de datos
        page_idx = self._locate_data_page(key_value)
        if page_idx < 0:
            return None

        # NIVEL 0: Buscar en TODA la cadena de overflow
        # No podemos asumir orden perfecto entre páginas de overflow
        while page_idx != -1:
            page = self._read_data_page(page_idx)
            
            for rec in page.records:
                if int(rec[self.key_field]) == key_value and rec.get(self.schema.deleted_name, 0) == 0:
                    return rec
            
            page_idx = page.next_overflow

        return None

    def rangeSearch(self, begin_key: int, end_key: int):
        """
        Retorna todos los registros entre begin_key y end_key (inclusivo)
        """
        if not os.path.exists(self.filename):
            return []
        
        if begin_key > end_key:
            return []
        
        results = []
        
        start_page_idx = self._locate_data_page(begin_key)
        if start_page_idx < 0:
            return []
        
        with open(self.filename, 'rb') as f:
            f.seek(0, os.SEEK_END)
            total_size = f.tell()
        total_pages = total_size // self.page_size
        
        visited = set()
        pages_to_visit = [start_page_idx]
        
        while pages_to_visit:
            page_idx = pages_to_visit.pop(0)
            
            if page_idx in visited or page_idx < 0 or page_idx >= total_pages:
                continue
            
            visited.add(page_idx)
            
            current_idx = page_idx
            found_in_chain = False
            
            while current_idx != -1 and current_idx < total_pages:
                page = self._read_data_page(current_idx)
                
                for rec in page.records:
                    if rec.get(self.schema.deleted_name, 0) == 0 and begin_key <= int(rec[self.key_field]) <= end_key:
                        results.append(rec)
                        found_in_chain = True

                    elif int(rec[self.key_field]) > end_key and current_idx == page_idx and not found_in_chain:
                        pass
                
                current_idx = page.next_overflow
            
            last_page = self._read_data_page(page_idx)
            if last_page.records:
                max_id_in_page = max(int(rec[self.key_field]) for rec in last_page.records if rec.get(self.schema.deleted_name, 0) == 0) if any(rec.get(self.schema.deleted_name, 0) == 0 for rec in last_page.records) else 0
                
                if max_id_in_page < end_key and page_idx + 1 < total_pages:
                    pages_to_visit.append(page_idx + 1)
        
        results.sort(key=lambda r: int(r[self.key_field]))
        return results

    def insert(self, record: dict):
        """Inserta un registro. Primero llena nodos, luego aplica chaining"""
        key_value = int(record[self.key_field])
        page_idx = self._locate_data_page(key_value)
        if page_idx < 0:
            return

        # Buscar en toda la cadena
        current_idx = page_idx
        last_idx = page_idx
        
        while current_idx != -1:
            page = self._read_data_page(current_idx)
            
            # Verificar si ya existe (incluso si está deleted)
            for i, rec in enumerate(page.records):
                if int(rec[self.key_field]) == key_value:
                    if rec.get(self.schema.deleted_name, 0) == 1:
                        # Reemplazar registro marcado como deleted
                        page.records[i] = record
                        self._write_data_page(current_idx, page)
                    return
            
            # Si hay espacio en esta página, insertar aquí ordenadamente
            if len(page.records) < BLOCK_FACTOR:
                pos = 0
                for i, rec in enumerate(page.records):
                    if key_value < int(rec[self.key_field]):
                        pos = i
                        break
                else:
                    pos = len(page.records)
                
                page.records.insert(pos, record)
                self._write_data_page(current_idx, page)
                return
            
            last_idx = current_idx
            current_idx = page.next_overflow
        
        # Todas las páginas están llenas, crear nueva overflow page
        new_overflow = DataPage(self.schema, [record], next_overflow=-1)
        new_idx = self._append_overflow_page(new_overflow)
        
        last_page = self._read_data_page(last_idx)
        last_page.next_overflow = new_idx
        self._write_data_page(last_idx, last_page)

    def delete(self, key_value: int) -> bool:
        """Marca un registro como deleted (soft delete)"""
        if not os.path.exists(self.filename):
            return False

        page_idx = self._locate_data_page(key_value)
        if page_idx < 0:
            return False

        while page_idx != -1:
            page = self._read_data_page(page_idx)
            
            for i, rec in enumerate(page.records):
                if int(rec[self.key_field]) == key_value:
                    if rec.get(self.schema.deleted_name, 0) == 1:
                        return False
                    page.records[i][self.schema.deleted_name] = 1
                    self._write_data_page(page_idx, page)
                    return True
                if int(rec[self.key_field]) > key_value:
                    return False
            
            page_idx = page.next_overflow

        return False

    def scanAll(self):
        """Muestra todas las páginas de datos"""
        if not os.path.exists(self.filename):
            print("(archivo vacío)")
            return
        
        with open(self.filename, 'rb') as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            n_pages = size // self.page_size
            
            f.seek(0, os.SEEK_SET)
            for pidx in range(n_pages):
                page_data = f.read(self.page_size)
                page = DataPage(self.schema)
                page = page.unpack(page_data)
                actives = [r for r in page.records if r.get(self.schema.deleted_name, 0) == 0]
                
                overflow_str = f"→{page.next_overflow}" if page.next_overflow != -1 else ""
                print(f"Page {pidx:3d} (overflow={page.next_overflow:3d}, size={len(page.records)}, activos={len(actives)}) {overflow_str}")
                
                for rec in actives:
                    print(f"  {rec}")

    def scanIndex(self):
        """Muestra la estructura completa del índice de 3 niveles"""
        print("=== NIVEL 2 (Root) ===")
        root = self._read_root()
        if root:
            print(f"keys: {root.keys}")
            print(f"ptrs: {root.pointers} (apuntan a nodos de nivel 1)")
        
        print("\n=== NIVEL 1 (Nodos intermedios) ===")
        with open(self.filename_idx1, 'rb') as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            n_nodes = size // IndexNode.SIZE_OF_NODE
            
            f.seek(0, os.SEEK_SET)
            for i in range(n_nodes):
                raw = f.read(IndexNode.SIZE_OF_NODE)
                node = IndexNode.unpack(raw)
                print(f"Node {i}: keys={node.keys}, ptrs={node.pointers} (apuntan a páginas de datos)")


if __name__ == "__main__":
    # Definir el schema según Employers_data.csv
    fields = [
        Field("Employee_ID",        Kind.INT,   fmt="i"),
        Field("Name",               Kind.CHAR,  size=40),
        Field("Age",                Kind.INT,   fmt="i"),
        Field("Gender",             Kind.CHAR,  size=10),
        Field("Department",         Kind.CHAR,  size=20),
        Field("Job_Title",          Kind.CHAR,  size=30),
        Field("Experience_Years",   Kind.INT,   fmt="i"),
        Field("Education_Level",    Kind.CHAR,  size=15),
        Field("Location",           Kind.CHAR,  size=20),
        Field("Salary",             Kind.INT,   fmt="i"),
        Field("deleted",            Kind.INT,   fmt="B"),
    ]
    schema = Schema(fields, deleted_name='deleted')

    # Limpiar archivos anteriores
    for fname in ['data.dat', 'data.dat_idx1', 'data.dat_idx2']:
        try:
            os.remove(fname)
        except FileNotFoundError:
            pass

    # Construcción del ISAM
    isam = ISAMFile('data.dat', schema, key_field='Employee_ID')
    print("=== CONSTRUCCIÓN DEL ISAM ===")
    isam.build_from_csv('data/Employers_data.csv', delimiter=',')


    # Ver índice
    print("\n=== ESTRUCTURA DE ÍNDICES ===")
    isam.scanIndex()

    
    '''
    # Búsquedas simples
    print("\n=== TEST: SEARCH ===")
    for qid in (1, 5000, 10000):
        rec = isam.search(qid)
        if rec:
            print("FOUND:", rec)
        else:
            print("NOT FOUND:", qid)

    

    # Range search pequeño
    print("\n=== TEST: RANGE SEARCH [250..255] ===")
    results = isam.rangeSearch(250, 255)
    for r in results:
        print(r)

    

    # Insertar nuevo empleado
    print("\n=== TEST: INSERT (id=10001) ===")
    new_emp = {
        "Employee_ID": 10001,
        "Name": "New Hire",
        "Age": 27,
        "Gender": "Female",
        "Department": "Engineering",
        "Job_Title": "Engineer",
        "Experience_Years": 3,
        "Education_Level": "Master",
        "Location": "Austin",
        "Salary": 105000,
        "deleted": 0
    }
    isam.insert(new_emp)
    isam.scanAll()
    isam.scanIndex()

    print("Inserted:", isam.search(10001))

    # Borrar uno existente
    print("\n=== TEST: DELETE (id=5000) ===")
    ok = isam.delete(5000)
    print("delete(5000):", ok)
    print("search(5000):", isam.search(5000))

    
    # Range después del delete
    print("\n=== TEST: RANGE [4995..5005] ===")
    results = isam.rangeSearch(4995, 5005)
    for r in results:
        print(r)

    '''

    # python -m src.index.isam