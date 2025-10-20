import math
import pickle
import csv
import _csv
import os
import heapq
from collections import deque
from rtree import index
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

    def remove(self, point, payload_id):
        x, y = point
        self.idx.delete(payload_id, (x, y, x, y))

    def rangeSearch(self, point, radio):
        x, y = point
        results = []
        candidates = self.idx.intersection((x - radio, y - radio, x + radio, y + radio), objects=True)
        for c in candidates:
            px, py = (c.bbox[0], c.bbox[1])
            dist = math.sqrt((px - x) ** 2 + (py - y) ** 2)
            if dist <= radio:
                results.append({"id": c.id, "x": px, "y": py, "dist": round(dist, 4)})
        results.sort(key=lambda r: r["dist"])
        return results

    def kNN(self, point, k):
        x, y = point
        return list(self.idx.nearest((x, y, x, y), num_results=k))

# PRUEBA
""""
if __name__ == "__main__":
    rt = RTreeAdapter()
    rt.add((1, 1), 1)
    rt.add((2, 2), 2)
    rt.add((10, 10), 3)

    print("Rango (2,2,r=2):", rt.rangeSearch((2, 2), 2))
    print("KNN (2,2,k=2):", rt.kNN((2, 2), 2))

    rt.remove((1, 1), 1)
    print("Después de eliminar:", rt.kNN((2, 2), 5))
"""
