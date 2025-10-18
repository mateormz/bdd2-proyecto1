import struct
import os

D = 2
MAX_CHAINING = 1
BLOCK_FACTOR = 3

class Record:
    FORMAT = 'i20s'
    SIZE_OF_RECORD = struct.calcsize(FORMAT)
    
    def __init__(self, key, value):
        self.key = key
        self.value = value

    def pack(self):
        return struct.pack(Record.FORMAT, self.key, self.value.encode())

    @staticmethod
    def unpack(data):
        key, value = struct.unpack(Record.FORMAT, data)
        return Record(key, value.decode().rstrip('\x00'))
    
class Bucket:
    BUCKET_SIZE = (BLOCK_FACTOR * Record.SIZE_OF_RECORD) + 4

    def __init__(self, records=[], next_bucket=-1):
        self.records = records
        self.next_bucket = next_bucket

    def pack(self):
        records_data = b''

        for record in self.records:
            records_data += record.pack()
        
        i = len(self.records)
        while i < BLOCK_FACTOR:
            records_data += b'\x00' * Record.SIZE_OF_RECORD
            i += 1

        next_bucket_data = struct.pack('i', self.next_bucket)
        return records_data + next_bucket_data

    @staticmethod
    def unpack(data: bytes):
        if len(data) < Bucket.BUCKET_SIZE:
            data = data.ljust(Bucket.BUCKET_SIZE, b'\x00')
        
        records = []

        for i in range(BLOCK_FACTOR):
            offset = i * Record.SIZE_OF_RECORD
            record_data = data[offset:offset + Record.SIZE_OF_RECORD]
            
            if record_data != b'\x00' * Record.SIZE_OF_RECORD:
                record = Record.unpack(record_data)
                if record.key != 0 or record.value.strip():
                    records.append(record)
                
        records_section_size = BLOCK_FACTOR * Record.SIZE_OF_RECORD
        next_bucket_data = data[records_section_size:records_section_size + 4]
        next_bucket = struct.unpack('i', next_bucket_data)[0]
        
        return Bucket(records, next_bucket)

class ExtendibleHashing:
    def __init__(self, filename: str):
        self.filename = "../out/ext_hash.dat" + filename
        global D
        
        if not os.path.exists(filename):
            with open(filename, 'w+b') as f:
                pass
            D = 2
            self._init_buckets()
        else:
            D = self._read_global_depth()
            if D == 0:
                D = 2
                self._init_buckets()
        
    def _read_global_depth(self):
        try:
            with open(self.filename, 'rb') as f:
                depth_data = f.read(4)
                if len(depth_data) == 4:
                    return struct.unpack('i', depth_data)[0]
        except:
            pass
        return D

    def _init_buckets(self):
        with open(self.filename, 'w+b') as f:
            f.write(struct.pack('i', D))
            
            for i in range(2 ** D):
                f.write(Bucket([], -1).pack())
    
    def get_bucket_index(self, key: int):
        return key % (2 ** D)

    def read_bucket(self, bucket_index):
        with open(self.filename, 'rb') as f:
            offset = 4 + (bucket_index * Bucket.BUCKET_SIZE)
            f.seek(offset)
            data = f.read(Bucket.BUCKET_SIZE)
            
            if len(data) < Bucket.BUCKET_SIZE:
                return Bucket([], -1)
                
            return Bucket.unpack(data)
    
    def write_bucket(self, bucket_index, bucket):
        with open(self.filename, 'r+b') as f:
            offset = 4 + (bucket_index * Bucket.BUCKET_SIZE)
            
            f.seek(0, 2)
            if offset + Bucket.BUCKET_SIZE > f.tell():
                f.write(b'\x00' * (offset + Bucket.BUCKET_SIZE - f.tell()))
            
            f.seek(offset)
            f.write(bucket.pack())
    
    def _find_available_bucket_position(self):
        with open(self.filename, 'r+b') as f:
            f.seek(0, 2)
            return (f.tell() - 4) // Bucket.BUCKET_SIZE
    
    def get_chain_length(self, bucket_index):
        current_bucket = self.read_bucket(bucket_index)
        chain_length = 0
        
        while current_bucket.next_bucket != -1:
            chain_length += 1
            current_bucket = self.read_bucket(current_bucket.next_bucket)
        
        return chain_length
    
    def add_overflow_bucket(self, bucket_index, record):
        bucket = self.read_bucket(bucket_index)
        current_bucket, current_index = bucket, bucket_index
        
        while current_bucket.next_bucket != -1:
            current_index = current_bucket.next_bucket
            current_bucket = self.read_bucket(current_index)
        
        new_bucket_index = self._find_available_bucket_position()
        current_bucket.next_bucket = new_bucket_index
        self.write_bucket(current_index, current_bucket)
        
        with open(self.filename, 'r+b') as f:
            f.seek(0, 2)
            f.write(Bucket([record], -1).pack())
    
    def increment_global_depth(self):
        global D
        new_d = D + 1
        D = new_d
        with open(self.filename, 'r+b') as f:
            f.seek(0)
            f.write(struct.pack('i', D))
    
    def rebuild_with_complete_scan(self):
        all_records = []
        
        with open(self.filename, 'rb') as f:
            f.seek(0, 2)
            file_size = f.tell()
            f.seek(4)
            
            while f.tell() + Bucket.BUCKET_SIZE <= file_size:
                bucket_data = f.read(Bucket.BUCKET_SIZE)
                if len(bucket_data) == Bucket.BUCKET_SIZE:
                    bucket = Bucket.unpack(bucket_data)
                    for record in bucket.records:
                        if (record.key is not None and 
                            record.value is not None and 
                            str(record.value).strip() != ''):
                            all_records.append(record)
        
        self.increment_global_depth()
        new_d = self._read_global_depth()
        
        with open(self.filename, 'w+b') as f:
            f.write(struct.pack('i', new_d))
            
            for i in range(2 ** new_d):
                f.write(Bucket([], -1).pack())
        
        for record in all_records:
            self._simple_insert(record)
    
    def _simple_insert(self, record):
        bucket_index = self.get_bucket_index(record.key)
        bucket = self.read_bucket(bucket_index)
        
        if len(bucket.records) < BLOCK_FACTOR:
            bucket.records.append(record)
            self.write_bucket(bucket_index, bucket)
        else:
            self.add_overflow_bucket(bucket_index, record)

    def insert(self, record: Record):
        bucket_index = self.get_bucket_index(record.key)
        
        existing = self.search(record.key)
        if existing:
            return
        
        bucket = self.read_bucket(bucket_index)
        
        if len(bucket.records) < BLOCK_FACTOR:
            bucket.records.append(record)
            self.write_bucket(bucket_index, bucket)
            return
            
        if self.get_chain_length(bucket_index) < MAX_CHAINING:
            self.add_overflow_bucket(bucket_index, record)
            return
            
        self.rebuild_with_complete_scan()
        self._simple_insert(record)

    def search(self, key):
        bucket_index = self.get_bucket_index(key)
        bucket = self.read_bucket(bucket_index)
        
        for record in bucket.records:
            if record.key == key:
                return record
        
        current_bucket = bucket
        while current_bucket.next_bucket != -1:
            current_bucket = self.read_bucket(current_bucket.next_bucket)
            for record in current_bucket.records:
                if record.key == key:
                    return record
        
        return None
    
    def remove(self, key):
        bucket_index = self.get_bucket_index(key)
        bucket = self.read_bucket(bucket_index)
        
        for i, record in enumerate(bucket.records):
            if record.key == key:
                del bucket.records[i]
                self.write_bucket(bucket_index, bucket)
                return True
        
        current_bucket = bucket
        while current_bucket.next_bucket != -1:
            next_index = current_bucket.next_bucket
            current_bucket = self.read_bucket(next_index)
            for i, record in enumerate(current_bucket.records):
                if record.key == key:
                    del current_bucket.records[i]
                    self.write_bucket(next_index, current_bucket)
                    return True
        
        return False