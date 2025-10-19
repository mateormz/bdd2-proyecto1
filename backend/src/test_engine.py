import os, pickle
from engine import Engine
from parser_sql import parse_sql

repo = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
out_dir = os.path.join(repo, "out")
with open(os.path.join(out_dir, "catalog.pickle"), "rb") as fh:
    catalog = pickle.load(fh)

engine = Engine(catalog)

tests = [
    # Igualdad (debería usar ExtHash o B+Tree/ISAM si no está ExtHash completo)
    "SELECT * FROM empleados WHERE Employee_ID = 100",
    # Rango (debería usar B+Tree.range_search o ISAM.rangeSearch)
    "SELECT Employee_ID, Name, Salary FROM empleados WHERE Employee_ID BETWEEN 95 AND 105",
    # Sin WHERE -> como no tienes SequentialFile, debe fallar (NotImplementedError)
    "SELECT * FROM empleados",
]

for q in tests:
    print("\n===", q)
    try:
        res = engine.execute(q)
        print(f"rows: {len(res['rows'])}")
        for r in res["rows"][:5]:
            print(r)
        if len(res["rows"]) > 5:
            print("...")
    except NotImplementedError as e:
        print("[Expected NotImplementedError]", e)
    except Exception as e:
        print("[ERROR]", e)
