import math
import pickle
import os
import heapq
from collections import deque
OUT_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'out', 'rtree_adapter.dat')
def bbox_of_point(p):
    return (tuple(p), tuple(p))
def bbox_area(b):
    mins, maxs = b
    a = 1.0
    for mi, ma in zip(mins, maxs):
        a *= (ma - mi)
    return a
def bbox_union(a, b):
    mins_a, maxs_a = a
    mins_b, maxs_b = b
    mins = tuple(min(x, y) for x, y in zip(mins_a, mins_b))
    maxs = tuple(max(x, y) for x, y in zip(maxs_a, maxs_b))
    return (mins, maxs)
def bbox_enlargement(base, add):
    return bbox_area(bbox_union(base, add)) - bbox_area(base)
def bbox_contains_point(b, p):
    mins, maxs = b
    return all(mi <= x <= ma for x, mi, ma in zip(p, mins, maxs))
def bbox_intersects_circle(b, center, r):
    mins, maxs = b
    sq = 0.0
    for c, mi, ma in zip(center, mins, maxs):
        if c < mi:
            d = mi - c
            sq += d * d
        elif c > ma:
            d = c - ma
            sq += d * d
    return sq <= r * r
def mindist_bbox_point(b, p):
    mins, maxs = b
    sq = 0.0
    for c, mi, ma in zip(p, mins, maxs):
        if c < mi:
            d = mi - c
            sq += d * d
        elif c > ma:
            d = c - ma
            sq += d * d
    return math.sqrt(sq)
class Node:
    def __init__(self, leaf=False):
        self.leaf = leaf
        self.entries = []  
        self.bbox = None
        self.parent = None

    def recompute_bbox(self):
        if not self.entries:
            self.bbox = None
            return
        b = self.entries[0][0]
        for e in self.entries[1:]:
            b = bbox_union(b, e[0])
        self.bbox = b

class RTree:
    def __init__(self, M=4, persistence=True, path=OUT_PATH):
        self.M = M
        self.m = max(1, M // 2)
        self.root = Node(leaf=True)
        self.persistence = persistence
        self.path = path
        if persistence:
            self._load()
    def _save(self):
        try:
            with open(self.path, 'wb') as f:
                pickle.dump(self, f)
        except Exception:
            pass
    def _load(self):
        if os.path.exists(self.path):
            try:
                with open(self.path, 'rb') as f:
                    obj = pickle.load(f)
                self.__dict__.update(obj.__dict__)
            except Exception:
                pass
    # INSERTAR HELPERS
    def _choose_leaf(self, n, entry_bbox):
        if n.leaf:
            return n
        best = None
        best_inc = None
        best_area = None
        for (b, child) in n.entries:
            inc = bbox_enlargement(b, entry_bbox)
            area = bbox_area(b)
            if best is None or inc < best_inc or (inc == best_inc and area < best_area):
                best = child
                best_inc = inc
                best_area = area
        return self._choose_leaf(best, entry_bbox)
    def _split_node(self, n):
        E = n.entries[:]
        n.entries = []
        n.recompute_bbox()
        best_pair = None
        best_waste = -1
        for i in range(len(E)):
            for j in range(i+1, len(E)):
                union_area = bbox_area(bbox_union(E[i][0], E[j][0]))
                waste = union_area - bbox_area(E[i][0]) - bbox_area(E[j][0])
                if waste > best_waste:
                    best_waste = waste
                    best_pair = (i, j)
        i, j = best_pair
        n1 = Node(leaf=n.leaf)
        n2 = Node(leaf=n.leaf)
        n1.entries.append(E[i]); n2.entries.append(E[j])
        for k, e in enumerate(E):
            if k in (i, j):
                continue
            b1 = bbox_union(n1.entries[0][0], e[0])
            b2 = bbox_union(n2.entries[0][0], e[0])
            inc1 = bbox_area(b1) - bbox_area(n1.entries[0][0])
            inc2 = bbox_area(b2) - bbox_area(n2.entries[0][0])
            if inc1 < inc2:
                n1.entries.append(e)
            elif inc2 < inc1:
                n2.entries.append(e)
            else:
                if bbox_area(n1.entries[0][0]) < bbox_area(n2.entries[0][0]):
                    n1.entries.append(e)
                else:
                    n2.entries.append(e)
            if len(n1.entries) + (len(E) - k - 1) < self.m:
                n1.entries.append(e)
            elif len(n2.entries) + (len(E) - k - 1) < self.m:
                n2.entries.append(e)
        n1.recompute_bbox(); n2.recompute_bbox()
        return n1, n2
    #REORDENAMIENTO
    def _adjust_tree(self, node, new_node=None):
        if node is self.root:
            if new_node:
                newr = Node(leaf=False)
                newr.entries = [(node.bbox, node), (new_node.bbox, new_node)]
                node.parent = newr; new_node.parent = newr
                newr.recompute_bbox()
                self.root = newr
            return
        parent = node.parent
        for idx, (b, child) in enumerate(parent.entries):
            if child is node:
                parent.entries[idx] = (node.bbox, child)
                break
        if new_node:
            parent.entries.append((new_node.bbox, new_node))
            new_node.parent = parent
        parent.recompute_bbox()
        if len(parent.entries) > self.M:
            p1, p2 = self._split_node(parent)
            if parent is self.root:
                newr = Node(leaf=False)
                newr.entries = [(p1.bbox, p1), (p2.bbox, p2)]
                p1.parent = newr; p2.parent = newr
                newr.recompute_bbox()
                self.root = newr
            else:
                grand = parent.parent
                for i, (b, ch) in enumerate(grand.entries):
                    if ch is parent:
                        grand.entries.pop(i)
                        break
                p1.parent = grand; p2.parent = grand
                grand.entries.append((p1.bbox, p1)); grand.entries.append((p2.bbox, p2))
                grand.recompute_bbox()
                if len(grand.entries) > self.M:
                    self._adjust_tree(grand, None)
        else:
            self._adjust_tree(parent, None)

    #AGREGAR PUNTO AL PLANO
    def add(self, point, payload=None):
        p = tuple(float(x) for x in point)
        eb = bbox_of_point(p)
        leaf = self._choose_leaf(self.root, eb)
        leaf.entries.append((eb, p, payload))
        leaf.recompute_bbox()
        if len(leaf.entries) > self.M:
            n1, n2 = self._split_node(leaf)
            if leaf is self.root:
                newr = Node(leaf=False)
                n1.parent = newr; n2.parent = newr
                newr.entries = [(n1.bbox, n1), (n2.bbox, n2)]
                newr.recompute_bbox()
                self.root = newr
            else:
                parent = leaf.parent
                for i, (b, ch) in enumerate(parent.entries):
                    if ch is leaf:
                        parent.entries.pop(i); break
                n1.parent = parent; n2.parent = parent
                parent.entries.append((n1.bbox, n1)); parent.entries.append((n2.bbox, n2))
                parent.recompute_bbox()
                if len(parent.entries) > self.M:
                    self._adjust_tree(parent, None)
        else:
            self._adjust_tree(leaf, None)
        if self.persistence:
            self._save()
    def _find_leaf(self, node, point):
        p = tuple(point)
        found = []
        if node.leaf:
            for e in node.entries:
                if e[1] == p:
                    found.append((node, e))
            return found
        for (b, child) in node.entries:
            if bbox_contains_point(b, p):
                found.extend(self._find_leaf(child, p))
        return found
    def remove(self, point):
        f = self._find_leaf(self.root, point)
        if not f:
            return 0
        removed = 0
        Q = []
        for (leaf, entry) in f:
            try:
                leaf.entries.remove(entry)
                removed += 1
            except ValueError:
                pass
            leaf.recompute_bbox()
            if leaf is not self.root and len(leaf.entries) < self.m:
                parent = leaf.parent
                for i, (b, ch) in enumerate(parent.entries):
                    if ch is leaf:
                        parent.entries.pop(i); break
                parent.recompute_bbox()
                for e in leaf.entries:
                    if leaf.leaf:
                        Q.append(e) 
                    else:
                        pass
        for e in Q:
            if len(e) == 3:
                _, pt, payload = e
                self.add(pt, payload)
        if not self.root.leaf and len(self.root.entries) == 1:
            self.root = self.root.entries[0][1]
            self.root.parent = None
        if self.persistence:
            self._save()
        return removed
    #BUSQUEDA POR RANGO
    def rangeSearch(self, point, radio):
        c = tuple(float(x) for x in point)
        r = float(radio)
        res = []
        q = deque([self.root])
        while q:
            n = q.popleft()
            if n.bbox is None:
                continue
            if not bbox_intersects_circle(n.bbox, c, r):
                continue
            if n.leaf:
                for (b, pt, payload) in n.entries:
                    dx = sum((a - b) ** 2 for a, b in zip(pt, c))
                    if dx <= r * r:
                        res.append((pt, payload))
            else:
                for (b, child) in n.entries:
                    if bbox_intersects_circle(b, c, r):
                        q.append(child)
        return res
    # KNN
    def kNN(self, point, k):
        c = tuple(float(x) for x in point)
        k = int(k)
        out = []
        heap = []
        heapq.heappush(heap, (mindist_bbox_point(self.root.bbox, c) if self.root.bbox else 0.0, self.root, False))
        while heap and len(out) < k:
            pr, item, is_entry = heapq.heappop(heap)
            if isinstance(item, Node):
                n = item
                if n.leaf:
                    for (b, pt, payload) in n.entries:
                        d = math.dist(pt, c)
                        heapq.heappush(heap, (d, (pt, payload), True))
                else:
                    for (b, child) in n.entries:
                        d = mindist_bbox_point(b, c)
                        heapq.heappush(heap, (d, child, False))
            else:
                if is_entry:
                    pt, payload = item
                    d = math.dist(pt, c)
                    out.append((d, (pt, payload)))
        out.sort(key=lambda x: x[0])
        return out[:k]
    def rangeSearch_kNN(self, point, k):
        return self.kNN(point, k)
    def all_points(self):
        res = []
        q = deque([self.root])
        while q:
            n = q.popleft()
            if n.leaf:
                for e in n.entries:
                    res.append(e)
            else:
                for _, ch in n.entries:
                    q.append(ch)
        return res
# PRUEBA
if __name__ == "__main__":
    r = RTree(M=4, persistence=False)
    pts = [(1,1), (2,2), (3,3), (10,10), (11,10), (10,11)]
    for p in pts:
        r.add(p)
    print("all:", [e[1] for e in r.all_points()])
    print("range (2,2) r=2:", r.rangeSearch((2,2), 2))
    print("kNN (0,0) k=3:", r.kNN((0,0), 3))
    r.remove((2,2))
    print("after rm:", [e[1] for e in r.all_points()])
