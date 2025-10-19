import os, pickle
from engine import Engine

repo = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
out_dir = os.path.join(repo, "out")

with open(os.path.join(out_dir, "catalog.pickle"), "rb") as fh:
    catalog = pickle.load(fh)

engine = Engine(catalog)

def run(q):
    print("\n===", q)
    res = engine.execute(q)
    if "rows" in res:
        rows = res["rows"]
        print("rows:", len(rows))
        for r in rows[:5]: print(r)
        if len(rows) > 5: print("...")
    else:
        print({k: v for k, v in res.items() if k != "metrics"})

# 1) Crear tabla vacía con índice BTREE en id
run("""CREATE TABLE test_ops (
    id INT KEY INDEX BTREE,
    nombre VARCHAR[40],
    salario INT
)""")

# 2) Inserciones
run("INSERT INTO test_ops VALUES (1, 'Ana', 5000)")
run("INSERT INTO test_ops VALUES (2, 'Bruno', 7000)")
run("INSERT INTO test_ops VALUES (3, 'Carla', 6500)")
run("INSERT INTO test_ops VALUES (4, 'Diego', 8000)")
run("INSERT INTO test_ops VALUES (5, 'Eva', 9000)")

# 3) Búsquedas puntuales (usa BTREE)
run("SELECT * FROM test_ops WHERE id = 1")
run("SELECT * FROM test_ops WHERE id = 5")
run("SELECT nombre, salario FROM test_ops WHERE id = 3")

# 4) Rango (usa BTREE.range_search)
run("SELECT id, nombre FROM test_ops WHERE id BETWEEN 2 AND 4")

# 5) Delete y verificación
run("DELETE FROM test_ops WHERE id = 3")
run("SELECT * FROM test_ops WHERE id = 3")
run("SELECT id, nombre FROM test_ops WHERE id BETWEEN 1 AND 5")

# 6) Reinsertar y verificar
run("INSERT INTO test_ops VALUES (3, 'Carla2', 6600)")
run("SELECT * FROM test_ops WHERE id = 3")

# (opcional) persistir el catálogo actualizado
with open(os.path.join(out_dir, "catalog.pickle"), "wb") as fh:
    pickle.dump(catalog, fh)
print("\n[done]")