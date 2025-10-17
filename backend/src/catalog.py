# backend/src/catalog.py
from typing import Dict, Any, Optional
from parser_sql import IndexType

class Catalog:
    def __init__(self):
        self.tables: Dict[str, Dict[str, Any]] = {}

    def register_table(self, name: str, schema: Dict[str, Any], data_path: str) -> None:
        self.tables[name] = {"schema": schema, "data_path": data_path, "indexes": {}}

    def register_index(self, table: str, column: str, idx_type: IndexType, path: str) -> None:
        if table not in self.tables:
            raise ValueError(f"Tabla '{table}' no registrada")
        self.tables[table]["indexes"][column] = {"type": idx_type, "path": path}

    def get_table(self, name: str) -> Dict[str, Any]:
        return self.tables.get(name, {})

    def get_index(self, table: str, column: str) -> Optional[Dict[str, Any]]:
        t = self.tables.get(table, {})
        return t.get("indexes", {}).get(column)

    def list_tables(self):
        return list(self.tables.keys())

    def drop_index(self, table: str, column: str) -> None:
        t = self.tables.get(table, {})
        if "indexes" in t and column in t["indexes"]:
            del t["indexes"][column]

if __name__ == "__main__":
    c = Catalog()

    # Crear tabla
    c.register_table(
        "empleados",
        {"key": "id", "columns": ["id", "nombre", "edad"]},
        "out/empleados.dat"
    )
    print("Tablas registradas:", c.list_tables())

    # Agregar índice
    c.register_index("empleados", "edad", IndexType.BTREE, "out/empleados_edad_BTree.dat")
    print("Tabla empleados:", c.get_table("empleados"))
    print("Índice edad:", c.get_index("empleados", "edad"))

    # Eliminar índice
    c.drop_index("empleados", "edad")
    print("Después de borrar índice:", c.get_table("empleados"))