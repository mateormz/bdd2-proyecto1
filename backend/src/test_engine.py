import os, pickle
from engine import Engine

repo = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
out_dir = os.path.join(repo, "out")
with open(os.path.join(out_dir, "catalog.pickle"), "rb") as fh:
    catalog = pickle.load(fh)

engine = Engine(catalog)

tests = [
    # Crear tabla desde schema (tu parser soporta CREATE)
    """CREATE TABLE test_tabla (
        id INT KEY INDEX BTREE,
        nombre VARCHAR[20],
        salario INT
    )""",

    # Insertar un registro
    "INSERT INTO empleados VALUES (10001, 'Nuevo Emp', 30, 'Male', 'IT', 'Developer', 2, 'Bachelor', 'Lima', 40000)",

    # Buscar por igualdad
    "SELECT * FROM empleados WHERE Employee_ID = 10001",

    # Borrar por clave
    "DELETE FROM empleados WHERE Employee_ID = 10001",

    # Buscar de nuevo (debería ya no estar)
    "SELECT * FROM empleados WHERE Employee_ID = 10001",

    # Rango normal
    "SELECT Employee_ID, Name, Salary FROM empleados WHERE Employee_ID BETWEEN 95 AND 105",
]

for q in tests:
    print("\n===", q)
    try:
        res = engine.execute(q)
        if isinstance(res, dict):
            rows = res.get("rows", [])
            print(f"rows: {len(rows)}")
            for r in rows[:3]:
                print(r)
            if len(rows) > 3:
                print("...")
        else:
            print("ok:", res)
    except NotImplementedError as e:
        print("[Expected NotImplementedError]", e)
    except Exception as e:
        print("[ERROR]", e)