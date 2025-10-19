import os
from catalog import Catalog
from core.schema import Schema, Field, Kind
from index.bptree import ClusteredIndexFile
from index.isam import ISAMFile

from index.ext_hash import ExtendibleHashing
from parser_sql import IndexType

# Schema
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
]
schema = Schema(fields, deleted_name="deleted")

repo = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
data_csv = os.path.join(repo, "backend", "data", "Employers_data.csv")
out_dir  = os.path.join(repo, "backend", "out")
os.makedirs(out_dir, exist_ok=True)

bpt_data  = os.path.join(out_dir, "bptree.dat")
bpt_index = os.path.join(out_dir, "bptree.idx")
isam_base = os.path.join(out_dir, "isam.dat")
exthash   = "ext_hash.dat"

# B+tree
print("[build] B+Tree clustered by Employee_ID")
bpt = ClusteredIndexFile(bpt_data, bpt_index, schema, key_field="Employee_ID", key_kind=Kind.INT)
bpt.build_from_csv(data_csv)

# ISAM
print("[build] ISAM by Employee_ID")
isam = ISAMFile(isam_base, schema, key_field="Employee_ID")
isam.build_from_csv(data_csv)

# ExtHash
print("[build] ExtHash by Employee_ID")
eh = ExtendibleHashing(exthash, schema, key_field="Employee_ID")

import csv
with open(data_csv, newline='', encoding="utf-8") as f:
    r = csv.DictReader(f)
    for i, row in enumerate(r):
        row["deleted"] = 0
        eh.insert(row)
        if i >= 2000:
            break

# Registra "empleados"
from catalog import Catalog
cat = Catalog()
cat.register_table(
    "empleados",
    schema=schema,
    data_path=bpt_data,
)
cat.register_index("empleados", "Employee_ID", IndexType.BTREE, bpt_index)
cat.register_index("empleados", "Employee_ID", IndexType.ISAM,  isam_base)
cat.register_index("empleados", "Employee_ID", IndexType.EXTENDIBLE_HASH, os.path.join(out_dir, exthash))

import pickle, os
cat_path = os.path.join(out_dir, "catalog.pickle")
with open(cat_path, "wb") as fh:
    pickle.dump(cat, fh)
print(f"[ok] Índices construidos y catálogo guardado en: {cat_path}")