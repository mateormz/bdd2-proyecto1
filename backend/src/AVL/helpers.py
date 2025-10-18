from __future__ import annotations
from typing import Optional, List, Tuple, Dict, Any, Protocol
from backend.src.AVL import data_index_file


def _height(idx: data_index_file.IndexFile, off: int) -> int:
    if not off: return 0
    _, h, _, _, _ = idx.read_node(off)
    return h

def _update_height(idx: data_index_file.IndexFile, off: int) -> int:
    if not off: return 0
    k, _, l, r, v = idx.read_node(off)
    h = 1 + max(_height(idx, l), _height(idx, r))
    idx.write_node(off, k, h, l, r, v)
    return h

def _balance_factor(idx: data_index_file.IndexFile, off: int) -> int:
    if not off: return 0
    _, _, l, r, _ = idx.read_node(off)
    return _height(idx, l) - _height(idx, r)

def _rotate_right(idx: data_index_file.IndexFile, y_off: int) -> int:
    k_y, h_y, x_off, r_y, v_y = idx.read_node(y_off)
    k_x, h_x, a_off, b_off, v_x = idx.read_node(x_off)
    # y.left = b
    idx.write_node(y_off, k_y, h_y, b_off, r_y, v_y)
    # x.right = y
    idx.write_node(x_off, k_x, h_x, a_off, y_off, v_x)
    _update_height(idx, y_off); _update_height(idx, x_off)
    return x_off

def _rotate_left(idx: data_index_file.IndexFile, x_off: int) -> int:
    k_x, h_x, l_x, y_off, v_x = idx.read_node(x_off)
    k_y, h_y, b_off, c_off, v_y = idx.read_node(y_off)
    # x.right = b
    idx.write_node(x_off, k_x, h_x, l_x, b_off, v_x)
    # y.left = x
    idx.write_node(y_off, k_y, h_y, x_off, c_off, v_y)
    _update_height(idx, x_off); _update_height(idx, y_off)
    return y_off

def _rebalance(idx: data_index_file.IndexFile, off: int) -> int:
    if not off: return 0
    _update_height(idx, off)
    bf = _balance_factor(idx, off)
    if bf > 1:
        k, h, l, r, v = idx.read_node(off)
        if _balance_factor(idx, l) < 0:  # LR
            new_l = _rotate_left(idx, l)
            k2, h2, _, r2, v2 = idx.read_node(off)
            idx.write_node(off, k2, h2, new_l, r2, v2)
        return _rotate_right(idx, off)
    if bf < -1:
        k, h, l, r, v = idx.read_node(off)
        if _balance_factor(idx, r) > 0:  # RL
            new_r = _rotate_right(idx, r)
            k2, h2, l2, _, v2 = idx.read_node(off)
            idx.write_node(off, k2, h2, l2, new_r, v2)
        return _rotate_left(idx, off)
    return off

def _insert(idx: data_index_file.IndexFile, off: int, key: int, value_off: int) -> int:
    if not off:
        return idx._alloc_node(key, value_off, height=1, left_off=0, right_off=0)
    k, h, l, r, v = idx.read_node(off)
    if key < k:
        new_l = _insert(idx, l, key, value_off)
        k, h, _, r, v = idx.read_node(off)
        idx.write_node(off, k, h, new_l, r, v)
    elif key > k:
        new_r = _insert(idx, r, key, value_off)
        k, h, l, _, v = idx.read_node(off)
        idx.write_node(off, k, h, l, new_r, v)
    else:
        # Duplicate policy: insert equal keys into the RIGHT subtree
        new_r = _insert(idx, r, key, value_off)
        k, h, l, _, v = idx.read_node(off)
        idx.write_node(off, k, h, l, new_r, v)
    return _rebalance(idx, off)

def _min_node(idx: _insert, off: int) -> int:
    cur = off
    while True:
        k, h, l, r, v = idx.read_node(cur)
        if not l: return cur
        cur = l

def _delete_once(idx: _insert, off: int, key: int) -> Tuple[int, bool]:
    """Delete a single node with the given key. Return (new_root, removed?)."""
    if not off: return 0, False
    k, h, l, r, v = idx.read_node(off)
    removed = False
    if key < k:
        new_l, removed = _delete_once(idx, l, key)
        idx.write_node(off, k, h, new_l, r, v)
    elif key > k:
        new_r, removed = _delete_once(idx, r, key)
        idx.write_node(off, k, h, l, new_r, v)
    else:
        removed = True
        if not l or not r:
            return (l or r), True
        succ_off = _min_node(idx, r)
        k2, h2, l2, r2, v2 = idx.read_node(succ_off)
        idx.write_node(off, k2, h, l, r, v2)
        new_r, _ = _delete_once(idx, r, k2)
        k3, h3, l3, _, v3 = idx.read_node(off)
        idx.write_node(off, k3, h3, l3, new_r, v3)
    return _rebalance(idx, off), removed

def _search_collect(idx: _insert, off: int, key: int, out: List[int]):
    """Collect *all* value_offs matching key, visiting both sides when equal."""
    if not off: return
    k, h, l, r, v = idx.read_node(off)
    if key < k:
        _search_collect(idx, l, key, out)
    elif key > k:
        _search_collect(idx, r, key, out)
    else:
        # equal: collect here and explore both children for more equals
        out.append(v)
        _search_collect(idx, l, key, out)
        _search_collect(idx, r, key, out)

def _range_collect(idx: _insert, off: int, lo: int, hi: int, out: List[int]):
    if not off: return
    k, h, l, r, v = idx.read_node(off)
    if lo < k: _range_collect(idx, l, lo, hi, out)
    if lo <= k <= hi: out.append(v)
    if k < hi: _range_collect(idx, r, lo, hi, out)