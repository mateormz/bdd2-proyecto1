import struct, os, csv

BLOCK_FACTOR = 3  # Factor de bloque para páginas de datos
INDEX_BLOCK_FACTOR = 3  # Factor de bloque para nodos de índice

class Record:
    FORMAT = '<i30sif10sB'
    SIZE_OF_RECORD = struct.calcsize(FORMAT)

    def __init__(self, sale_id: int, product_name: str, quantity: int, unit_price: float, sale_date: str, deleted: int = 0):
        self.sale_id = int(sale_id)
        self.product_name = (product_name or '')[:30]
        self.quantity = int(quantity)
        self.unit_price = float(unit_price)
        self.sale_date = (sale_date or '')[:10]
        self.deleted = 1 if deleted else 0

    def pack(self) -> bytes:
        return struct.pack(
            self.FORMAT,
            self.sale_id,
            self.product_name.ljust(30).encode(),
            self.quantity,
            self.unit_price,
            self.sale_date.ljust(10).encode(),
            self.deleted
        )

    @staticmethod
    def unpack(data: bytes):
        sale_id, name, qty, price, date, deleted = struct.unpack(Record.FORMAT, data)
        return Record(
            sale_id,
            name.decode().rstrip('\x00 '),
            qty,
            price,
            date.decode().rstrip('\x00 '),
            deleted
        )

class DataPage:
    """Nivel 0: Página de datos con overflow chaining"""
    HEADER_FORMAT = '<ii'
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
    SIZE_OF_PAGE = HEADER_SIZE + BLOCK_FACTOR * Record.SIZE_OF_RECORD

    def __init__(self, records=None, next_overflow: int = -1):
        self.records = list(records) if records is not None else []
        self.next_overflow = next_overflow

    def pack(self) -> bytes:
        header = struct.pack(self.HEADER_FORMAT, len(self.records), self.next_overflow)
        records_data = b''.join(r.pack() for r in self.records)
        missing = BLOCK_FACTOR - len(self.records)
        if missing > 0:
            records_data += b'\x00' * (missing * Record.SIZE_OF_RECORD)
        return header + records_data

    @staticmethod
    def unpack(data: bytes):
        size, next_overflow = struct.unpack(DataPage.HEADER_FORMAT, data[:DataPage.HEADER_SIZE])
        records = []
        offset = DataPage.HEADER_SIZE
        for _ in range(size):
            record_data = data[offset: offset + Record.SIZE_OF_RECORD]
            records.append(Record.unpack(record_data))
            offset += Record.SIZE_OF_RECORD
        return DataPage(records, next_overflow)

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
    def __init__(self, filename):
        self.filename = filename  # Nivel 0: datos
        self.filename_idx1 = filename + '_idx1'  # Nivel 1: índice intermedio
        self.filename_idx2 = filename + '_idx2'  # Nivel 2: índice root

    def build_from_csv(self, csv_path: str, delimiter: str = ';'):
        """Construye el ISAM de 3 niveles a partir de un CSV"""
        # 1. Leer y ordenar registros
        rows = []
        with open(csv_path, 'r', newline='', encoding='utf-8') as fh:
            reader = csv.reader(fh, delimiter=delimiter)
            for row in reader:
                if not row:
                    continue
                try:
                    sale_id = int(row[0])
                except ValueError:
                    continue
                name = row[1]
                qty = int(row[2])
                price = float(row[3])
                date = row[4]
                rows.append(Record(sale_id, name, qty, price, date, deleted=0))

        rows.sort(key=lambda r: r.sale_id)

        # 2. NIVEL 0: Crear páginas de datos
        first_keys_level0 = []  # Primer key de cada página de datos
        
        with open(self.filename, 'wb') as f:
            for start in range(0, len(rows), BLOCK_FACTOR):
                chunk = rows[start:start + BLOCK_FACTOR]
                page = DataPage(chunk, next_overflow=-1)
                f.write(page.pack())
                first_keys_level0.append(chunk[0].sale_id)

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
            f.seek(page_idx * DataPage.SIZE_OF_PAGE)
            data = f.read(DataPage.SIZE_OF_PAGE)
        return DataPage.unpack(data)

    def _write_data_page(self, page_idx: int, page: DataPage):
        """Escribe una página de datos"""
        with open(self.filename, 'r+b') as f:
            f.seek(page_idx * DataPage.SIZE_OF_PAGE)
            f.write(page.pack())

    def _append_overflow_page(self, page: DataPage) -> int:
        """Añade una overflow page al final del archivo de datos"""
        with open(self.filename, 'r+b') as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            new_index = size // DataPage.SIZE_OF_PAGE
            f.write(page.pack())
        return new_index

    def _locate_data_page(self, sale_id: int) -> int:
        """Navega los 2 niveles de índice para encontrar la página de datos"""
        # NIVEL 2: Root
        root = self._read_root()
        if root is None:
            return -1

        # Buscar en el root qué nodo de nivel 1 visitar
        level1_node_idx = 0
        for i, key in enumerate(root.keys):
            if sale_id < key:
                level1_node_idx = root.pointers[i]
                break
        else:
            level1_node_idx = root.pointers[len(root.keys)]

        # NIVEL 1: Nodo intermedio
        level1_node = self._read_level1_node(level1_node_idx)
        
        # Buscar en nivel 1 qué página de datos visitar
        data_page_idx = 0
        for i, key in enumerate(level1_node.keys):
            if sale_id < key:
                data_page_idx = level1_node.pointers[i]
                break
        else:
            data_page_idx = level1_node.pointers[len(level1_node.keys)]

        return data_page_idx

    def search(self, sale_id: int):
        """Busca un registro navegando los 3 niveles"""
        if not os.path.exists(self.filename):
            return None

        # Navegar índices (nivel 2 y 1) para llegar a la página de datos
        page_idx = self._locate_data_page(sale_id)
        if page_idx < 0:
            return None

        # NIVEL 0: Buscar en TODA la cadena de overflow
        # No podemos asumir orden perfecto entre páginas de overflow
        while page_idx != -1:
            page = self._read_data_page(page_idx)
            
            for rec in page.records:
                if rec.sale_id == sale_id and rec.deleted == 0:
                    return rec
            
            page_idx = page.next_overflow

        return None

    def insert(self, record: Record):
        """Inserta un registro. Primero llena nodos, luego aplica chaining"""
        page_idx = self._locate_data_page(record.sale_id)
        if page_idx < 0:
            return

        # Buscar en toda la cadena
        current_idx = page_idx
        last_idx = page_idx
        
        while current_idx != -1:
            page = self._read_data_page(current_idx)
            
            # Verificar si ya existe (incluso si está deleted)
            for i, rec in enumerate(page.records):
                if rec.sale_id == record.sale_id:
                    if rec.deleted == 1:
                        # Reemplazar registro marcado como deleted
                        page.records[i] = record
                        self._write_data_page(current_idx, page)
                    return
            
            # Si hay espacio en esta página, insertar aquí ordenadamente
            if len(page.records) < BLOCK_FACTOR:
                pos = 0
                for i, rec in enumerate(page.records):
                    if record.sale_id < rec.sale_id:
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
        new_overflow = DataPage([record], next_overflow=-1)
        new_idx = self._append_overflow_page(new_overflow)
        
        last_page = self._read_data_page(last_idx)
        last_page.next_overflow = new_idx
        self._write_data_page(last_idx, last_page)

    def delete(self, sale_id: int) -> bool:
        """Marca un registro como deleted (soft delete)"""
        if not os.path.exists(self.filename):
            return False

        page_idx = self._locate_data_page(sale_id)
        if page_idx < 0:
            return False

        while page_idx != -1:
            page = self._read_data_page(page_idx)
            
            for i, rec in enumerate(page.records):
                if rec.sale_id == sale_id:
                    if rec.deleted == 1:
                        return False
                    page.records[i].deleted = 1
                    self._write_data_page(page_idx, page)
                    return True
                if rec.sale_id > sale_id:
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
            n_pages = size // DataPage.SIZE_OF_PAGE
            
            f.seek(0, os.SEEK_SET)
            for pidx in range(n_pages):
                page_data = f.read(DataPage.SIZE_OF_PAGE)
                page = DataPage.unpack(page_data)
                actives = [r for r in page.records if r.deleted == 0]
                
                overflow_str = f"→{page.next_overflow}" if page.next_overflow != -1 else ""
                print(f"Page {pidx:3d} (overflow={page.next_overflow:3d}, size={len(page.records)}, activos={len(actives)}) {overflow_str}")
                
                for rec in actives:
                    print(f"  {rec.sale_id:4d} | {rec.product_name:30s} | {rec.quantity:3d} | {rec.unit_price:7.2f} | {rec.sale_date}")

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
    # Limpiar archivos anteriores
    for fname in ['data.dat', 'data.dat_idx1', 'data.dat_idx2']:
        try:
            os.remove(fname)
        except FileNotFoundError:
            pass

    isam = ISAMFile('data.dat')
    print("=== CONSTRUCCIÓN DEL ISAM ===")
    isam.build_from_csv('sales_dataset_unsorted(2).csv')
    
    print("\n=== ESTRUCTURA DE DATOS (Nivel 0) ===")
    isam.scanAll()
    
    print("\n=== ESTRUCTURA DE ÍNDICES ===")
    isam.scanIndex()

    

    # Búsqueda
    print("\n=== BÚSQUEDA ===")
    qid = 403
    rec = isam.search(qid)
    if rec:
        print(f"FOUND: {rec.sale_id} | {rec.product_name} | {rec.quantity} | {rec.unit_price:.2f}")
    else:
        print(f"NOT FOUND: {qid}")

    

    # Inserción (debe llenar nodos primero)
    print("\n=== INSERCIÓN 1 (llenar página) ===")
    isam.insert(Record(10001, "NEW ITEM A", 10, 99.99, "2025-01-01"))
    isam.scanAll()
    isam.scanIndex()

    # Inserción que fuerza overflow (chaining)
    print("\n=== INSERCIÓN 2 (debe crear overflow) ===")
    isam.insert(Record(406, "NEW ITEM B", 5, 50.00, "2025-01-02"))
    isam.scanAll()

    # Más inserciones
    print("\n=== INSERCIÓN 3 ===")
    isam.insert(Record(407, "NEW ITEM C", 15, 75.50, "2025-01-03"))
    isam.scanAll()

    # Delete
    print("\n=== DELETE ===")
    ok = isam.delete(405)
    print(f"delete(405): {ok}")
    isam.scanAll()

    # Verificar que el índice NO cambió
    print("\n=== ÍNDICE (debe permanecer estático) ===")
    isam.scanIndex()

    # Inserción (debe llenar nodos primero)
    print("\n=== INSERCIÓN 1 (llenar página) ===")
    isam.insert(Record(10005, "NEW ITEM A", 10, 99.99, "2025-01-01"))
    isam.scanAll()
    isam.scanIndex()

    # Inserción (debe llenar nodos primero)
    print("\n=== INSERCIÓN 1 (llenar página) ===")
    isam.insert(Record(10002, "NEW ITEM A", 10, 99.99, "2025-01-01"))
    isam.scanAll()
    isam.scanIndex()

    # Inserción (debe llenar nodos primero)
    print("\n=== INSERCIÓN 1 (llenar página) ===")
    isam.insert(Record(10004, "NEW ITEM A", 10, 99.99, "2025-01-01"))
    isam.scanAll()
    isam.scanIndex()

    # Búsqueda
    print("\n=== BÚSQUEDA ===")
    qid = 10004
    rec = isam.search(qid)
    if rec:
        print(f"FOUND: {rec.sale_id} | {rec.product_name} | {rec.quantity} | {rec.unit_price:.2f}")
    else:
        print(f"NOT FOUND: {qid}")