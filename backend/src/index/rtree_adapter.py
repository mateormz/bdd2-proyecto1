import math
import pickle
import csv
import _csv
import os
import heapq
import time
from collections import deque
from rtree import index

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

_counter = IOCounter()

def count_read(bytes_count=0):
    _counter.count_read(bytes_count)

def count_write(bytes_count=0):
    _counter.count_write(bytes_count)

OUT_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'out', 'rtree_index')

class RTreeAdapter:
    def __init__(self, path=OUT_PATH):
        p = index.Property()
        p.storage = index.RT_Disk
        p.index_type = index.RT_RTree
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.path = path
        self.idx = index.Index(path, properties=p)

    def add(self, point, payload_id):
        x, y = point
        self.idx.insert(payload_id, (x, y, x, y))
        count_write()

    def remove(self, point, payload_id):
        x, y = point
        self.idx.delete(payload_id, (x, y, x, y))
        count_write()

    def rangeSearch(self, point, radio):
        x, y = point
        count_read()
        results = []
        candidates = self.idx.intersection((x - radio, y - radio, x + radio, y + radio), objects=True)
        for c in candidates:
            count_read()
            px, py = (c.bbox[0], c.bbox[1])
            dist = math.sqrt((px - x) ** 2 + (py - y) ** 2)
            if dist <= radio:
                results.append({"id": c.id, "x": px, "y": py, "dist": round(dist, 4)})
        results.sort(key=lambda r: r["dist"])
        return results

    def kNN(self, point, k):
        x, y = point
        count_read()
        res = list(self.idx.nearest((x, y, x, y), num_results=k))
        count_read(len(res))
        return res
