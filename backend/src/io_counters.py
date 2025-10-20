import time

class IOCounter:
    def __init__(self):
        self.reads = 0
        self.writes = 0 
        self.read_bytes = 0
        self.write_bytes = 0
        self.start_time = None
        self.total_time_ms = 0.0
    
    def count_read(self, bytes_count=0):
        self.reads += 1
        self.read_bytes += bytes_count
    
    def count_write(self, bytes_count=0):
        self.writes += 1
        self.write_bytes += bytes_count
    
    def start_timing(self):
        self.start_time = time.time()
    
    def stop_timing(self):
        if self.start_time:
            self.total_time_ms = (time.time() - self.start_time) * 1000
            self.start_time = None
    
    def reset(self):
        self.reads = 0
        self.writes = 0
        self.read_bytes = 0
        self.write_bytes = 0
        self.total_time_ms = 0.0
        self.start_time = None
    
    def show_report(self, title="Reporte I/O"):
        total_ops = self.reads + self.writes
        print(f"\n{title}")
        print("="*40)
        print(f"Lecturas: {self.reads}")
        print(f"Escrituras: {self.writes}")
        print(f"Total operaciones: {total_ops}")
        print(f"Tiempo: {self.total_time_ms:.2f} ms")
        if total_ops > 0:
            print(f"Promedio: {self.total_time_ms/total_ops:.2f} ms/op")
        print("="*40)


_counter = IOCounter()

def count_read(bytes_count=0):
    _counter.count_read(bytes_count)

def count_write(bytes_count=0):
    _counter.count_write(bytes_count)

def start_timing():
    _counter.start_timing()

def stop_timing():
    _counter.stop_timing()

def reset_counters():
    _counter.reset()

def show_report(title="Reporte I/O Global"):
    _counter.show_report(title)

def get_counters():
    return {
        'reads': _counter.reads,
        'writes': _counter.writes,
        'read_bytes': _counter.read_bytes,
        'write_bytes': _counter.write_bytes,
        'total_time_ms': _counter.total_time_ms
    }

if __name__ == "__main__":
    # Reiniciar contadores antes del test
    reset_counters()
    start_timing()

    # Simular lecturas y escrituras
    count_read(128)   # leímos 128 bytes
    count_read(256)   # leímos 256 bytes
    count_write(64)   # escribimos 64 bytes
    count_write(512)  # escribimos 512 bytes

    # Simular una pequeña espera para ver el tiempo
    import time
    time.sleep(0.05)  # 50 ms

    stop_timing()

    # Mostrar el reporte
    show_report("Test de IOCounter")
    print("Diccionario de métricas:", get_counters())