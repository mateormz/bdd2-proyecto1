# backend/src/src_tests/test_engine.py
import os, pickle, glob
from pathlib import Path
from engine import Engine

HERE = Path(__file__).resolve()          # .../backend/src/src_tests/test_engine.py
SRC_DIR = HERE.parent.parent             # .../backend/src
BACKEND_DIR = SRC_DIR.parent             # .../backend

# Buscar catalog.pickle en backend/out o src/out
out_candidates = [BACKEND_DIR / "out", SRC_DIR / "out"]
catalog_path = None
for d in out_candidates:
    p = d / "catalog.pickle"
    if p.exists():
        catalog_path = p
        break
if not catalog_path:
    raise FileNotFoundError(
        f"No encontré catalog.pickle en: {out_candidates}. "
        "Primero construye índices (ej: python backend/src/test_build_index.py)."
    )

OUT = catalog_path.parent  # backend/out

def cleanup(prefixes):
    pats = []
    for p in prefixes:
        pats += [f"{p}*.dat", f"{p}*.idx", f"{p}*.idx1", f"{p}*.idx2"]
    for pat in pats:
        for fp in glob.glob(str(OUT / pat)):
            try: os.remove(fp)
            except FileNotFoundError: pass

# limpiar artefactos de pruebas previas
cleanup(["test_bpt", "test_eh", "test_isam"])

with open(catalog_path, "rb") as fh:
    catalog = pickle.load(fh)

engine = Engine(catalog)

def run(q: str):
    print("\n===", q)
    res = engine.execute(q)
    if "rows" in res:
        rows = res["rows"]
        print("rows:", len(rows))
        for r in rows[:5]:
            print(r)
        if len(rows) > 5:
            print("...")
    else:
        print({k: v for k, v in res.items() if k != "metrics"})

# --- B+TREE ---
run("""CREATE TABLE test_bpt (
    id INT KEY INDEX BTREE,
    nombre VARCHAR[40],
    salario INT
)""")
run("INSERT INTO test_bpt VALUES (1, 'Ana', 5000)")
run("INSERT INTO test_bpt VALUES (2, 'Bruno', 7000)")
run("INSERT INTO test_bpt VALUES (3, 'Carla', 6500)")
run("INSERT INTO test_bpt VALUES (4, 'Diego', 8000)")
run("INSERT INTO test_bpt VALUES (5, 'Eva', 9000)")
run("SELECT * FROM test_bpt WHERE id = 1")
run("SELECT * FROM test_bpt WHERE id = 5")
run("SELECT nombre, salario FROM test_bpt WHERE id = 3")
run("SELECT id, nombre FROM test_bpt WHERE id BETWEEN 2 AND 4")
run("DELETE FROM test_bpt WHERE id = 3")
run("SELECT * FROM test_bpt WHERE id = 3")
run("SELECT id, nombre FROM test_bpt WHERE id BETWEEN 1 AND 5")

# --- EXTHASH ---
run("""CREATE TABLE test_eh (
    id INT KEY INDEX EXTHASH,
    nombre VARCHAR[40],
    salario INT
)""")
run("INSERT INTO test_eh VALUES (10, 'Lara', 4000)")
run("INSERT INTO test_eh VALUES (11, 'Mauro', 4200)")
run("INSERT INTO test_eh VALUES (12, 'Nora', 4400)")
run("SELECT * FROM test_eh WHERE id = 10")
run("SELECT * FROM test_eh WHERE id = 12")
run("DELETE FROM test_eh WHERE id = 11")
run("SELECT * FROM test_eh WHERE id = 11")

# --- ISAM (normal, sin tocar 'deleted' manualmente) ---
# CSV temporal de prueba (sin header)
test_csv = (OUT / "test_isam.csv")
with open(test_csv, "w", encoding="utf-8") as f:
    f.write("100,Pepe,3000\n")
    f.write("101,Rita,3500\n")
    f.write("102,Sara,3800\n")
    f.write("103,Toni,4200\n")
    f.write("104,Uri,4500\n")

from core.schema import Schema, Field, Kind
base_isam_schema = Schema([
    Field("id", Kind.INT, fmt="i"),
    Field("nombre", Kind.CHAR, size=40),
    Field("salario", Kind.INT, fmt="i"),
])
catalog.register_table("test_isam", base_isam_schema, str(OUT / "test_isam.dat"))

run(f'CREATE TABLE test_isam FROM FILE "{test_csv}" USING INDEX ISAM(id)')
run("SELECT * FROM test_isam WHERE id = 100")
run("SELECT id, nombre FROM test_isam WHERE id BETWEEN 101 AND 103")
run("INSERT INTO test_isam VALUES (105, 'Vero', 4700)")
run("SELECT * FROM test_isam WHERE id = 105")
run("DELETE FROM test_isam WHERE id = 101")
run("SELECT * FROM test_isam WHERE id = 101")

# Guardar catálogo actualizado
with open(catalog_path, "wb") as fh:
    pickle.dump(catalog, fh)

print("\n[done]")