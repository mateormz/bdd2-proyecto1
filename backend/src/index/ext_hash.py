import struct
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'core'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from schema import Schema, Field, Kind
from io_counters import IOCounter, count_read, count_write

D = 2
MAX_CHAINING = 1
BLOCK_FACTOR = 3
    
class Bucket:
    def __init__(self, records=[], next_bucket=-1, schema=None):
        self.records = records
        self.next_bucket = next_bucket
        self.schema = schema
        self.record_size = schema.size if schema else 0
        self.bucket_size = (BLOCK_FACTOR * self.record_size) + 4

    def pack(self):
        records_data = b''

        for record in self.records:
            records_data += self.schema.pack(record)
        
        remaining_slots = BLOCK_FACTOR - len(self.records)
        records_data += b'\x00' * (remaining_slots * self.record_size)    
        next_bucket_data = struct.pack('i', self.next_bucket)
        return records_data + next_bucket_data

    @staticmethod
    def unpack(data: bytes, schema):
        record_size = schema.size
        bucket_size = (BLOCK_FACTOR * record_size) + 4

        if len(data) < bucket_size:
            data = data.ljust(bucket_size, b'\x00')
        
        records = []

        for i in range(BLOCK_FACTOR):
            offset = i * record_size
            record_data = data[offset:offset + record_size]
            
            if record_data != b'\x00' * record_size:
                record_dict = schema.unpack(record_data)
                if any(str(v).strip() for v in record_dict.values() if v):
                    records.append(record_dict)
                
        records_section_size = BLOCK_FACTOR * record_size
        next_bucket_data = data[records_section_size:records_section_size + 4]
        next_bucket = struct.unpack('i', next_bucket_data)[0]
        
        return Bucket(records, next_bucket, schema)

class ExtendibleHashing:
    def __init__(self, filename: str, schema: Schema, key_field: str, hash_function=None):
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
        out_dir = os.path.join(base_dir, 'out')
        os.makedirs(out_dir, exist_ok=True)

        if not filename.endswith('.dat'):
            filename = filename + '.dat'
        self.filename = os.path.abspath(os.path.join(out_dir, filename))
        
        self.schema = schema
        self.key_field = key_field
        self.hash_function = hash_function or self._default_hash
        self.io_counter = IOCounter()
        global D
        
        if not os.path.exists(self.filename):
            with open(self.filename, 'w+b') as f:
                pass
            D = 2
            self._init_buckets()
        else:
            D = self._read_global_depth()
            if D == 0:
                D = 2
                self._init_buckets()
    
    def _default_hash(self, key):
        if isinstance(key, int) or isinstance(key, float):
            return key % (2 ** D)
        elif isinstance(key, str):
            return hash(key) % (2 ** D)
        else:
            return hash(str(key)) % (2 ** D)
        
    def _read_global_depth(self):
        try:
            with open(self.filename, 'rb') as f:
                depth_data = f.read(4)
                self.io_counter.count_read(len(depth_data))
                count_read(len(depth_data))
                if len(depth_data) == 4:
                    return struct.unpack('i', depth_data)[0]
        except:
            pass
        return D

    def _init_buckets(self):
        with open(self.filename, 'w+b') as f:
            depth_data = struct.pack('i', D)
            f.write(depth_data)
            self.io_counter.count_write(len(depth_data))
            count_write(len(depth_data))
            
            for i in range(2 ** D):
                bucket_data = Bucket([], -1, self.schema).pack()
                f.write(bucket_data)
                self.io_counter.count_write(len(bucket_data))
                count_write(len(bucket_data))
    
    def get_bucket_index(self, key):
        return self.hash_function(key)

    def read_bucket(self, bucket_index):
        with open(self.filename, 'rb') as f:
            bucket_size = self.schema.size * BLOCK_FACTOR + 4
            offset = 4 + (bucket_index * bucket_size)
            f.seek(offset)
            data = f.read(bucket_size)
            
            self.io_counter.count_read(len(data))
            count_read(len(data))
            
            if len(data) < bucket_size:
                return Bucket([], -1, self.schema)
                
            return Bucket.unpack(data, self.schema)
    
    def write_bucket(self, bucket_index, bucket):
        with open(self.filename, 'r+b') as f:
            bucket_size = self.schema.size * BLOCK_FACTOR + 4
            offset = 4 + (bucket_index * bucket_size)
            
            f.seek(0, 2)
            current_size = f.tell()
            if offset + bucket_size > current_size:
                padding = b'\x00' * (offset + bucket_size - current_size)
                f.write(padding)
                self.io_counter.count_write(len(padding))
                count_write(len(padding))
            
            f.seek(offset)
            bucket_data = bucket.pack()
            f.write(bucket_data)
            
            self.io_counter.count_write(len(bucket_data))
            count_write(len(bucket_data))

    def _find_available_bucket_position(self):
        with open(self.filename, 'r+b') as f:
            f.seek(0, 2)
            bucket_size = self.schema.size * BLOCK_FACTOR + 4
            return (f.tell() - 4) // bucket_size
    
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
        
        new_bucket = Bucket([record], -1, self.schema)
        self.write_bucket(new_bucket_index, new_bucket)
    
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
            
            bucket_size = self.schema.size * BLOCK_FACTOR + 4
            while f.tell() + bucket_size <= file_size:
                bucket_data = f.read(bucket_size)
                if len(bucket_data) == bucket_size:
                    bucket = Bucket.unpack(bucket_data, self.schema)
                    for record in bucket.records:
                        if record.get(self.key_field) is not None:
                            all_records.append(record)
        
        self.increment_global_depth()
        new_d = self._read_global_depth()
        
        with open(self.filename, 'w+b') as f:
            f.write(struct.pack('i', new_d))
            
            for i in range(2 ** new_d):
                f.write(Bucket([], -1, self.schema).pack())
        
        for record in all_records:
            self._simple_insert(record)
    
    def _simple_insert(self, record):
        key_value = record.get(self.key_field)
        bucket_index = self.get_bucket_index(key_value)
        bucket = self.read_bucket(bucket_index)
        
        if len(bucket.records) < BLOCK_FACTOR:
            bucket.records.append(record)
            self.write_bucket(bucket_index, bucket)
        else:
            self.add_overflow_bucket(bucket_index, record)

    def insert(self, record):
        key_value = record.get(self.key_field)
        bucket_index = self.get_bucket_index(key_value)
        
        existing = self.search(key_value)
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
            if record.get(self.key_field) == key:
                return record
        
        current_bucket = bucket
        while current_bucket.next_bucket != -1:
            current_bucket = self.read_bucket(current_bucket.next_bucket)
            for record in current_bucket.records:
                if record.get(self.key_field) == key:
                    return record
        
        return None
    
    def remove(self, key):
        bucket_index = self.get_bucket_index(key)
        bucket = self.read_bucket(bucket_index)
        
        for i, record in enumerate(bucket.records):
            if record.get(self.key_field) == key:
                del bucket.records[i]
                self.write_bucket(bucket_index, bucket)
                return True
        
        current_bucket = bucket
        while current_bucket.next_bucket != -1:
            next_index = current_bucket.next_bucket
            current_bucket = self.read_bucket(next_index)
            for i, record in enumerate(current_bucket.records):
                if record.get(self.key_field) == key:
                    del current_bucket.records[i]
                    self.write_bucket(next_index, current_bucket)
                    return True
        
        return False
    
    def reset_io_counters(self):
        self.io_counter.reset()
    
    def start_measurement(self):
        self.io_counter.start_timing()
    
    def stop_measurement(self):
        self.io_counter.stop_timing()
    
    def show_io_report(self, title="ExtendibleHashing Report"):
        self.io_counter.show_report(title)
    
    def get_io_stats(self):
        return {
            'reads': self.io_counter.reads,
            'writes': self.io_counter.writes,
            'read_bytes': self.io_counter.read_bytes,
            'write_bytes': self.io_counter.write_bytes,
            'total_time_ms': self.io_counter.total_time_ms
        }