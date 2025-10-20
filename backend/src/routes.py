# backend/src/routes.py
from fastapi import APIRouter, UploadFile, File, HTTPException
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
import os
import time

from engine import Engine
from parser_sql import IndexType

eng = Engine()

router = APIRouter()


class SQLQuery(BaseModel):
    query: str = Field(..., description="Sentencia SQL completa entendida por tu parser/engine")

class LoadCSVRequest(BaseModel):
    table_name: str
    csv_path: str
    index_type: str = Field(..., description="SEQUENTIAL | ISAM | EXTHASH | BPTREE | RTREE")
    key_column: str = Field(..., description="Nombre de la columna clave/label")
    x: str = "x"
    y: str = "y"
    z: Optional[str] = "z"

class SpatialRangeRequest(BaseModel):
    table: str
    # se acepta 2D [x,y] o 3D [x,y,z]; radio opcional
    point: List[float] = Field(..., description="[x,y] o [x,y,z]")
    radius: Optional[float] = None
    coord_column: str = "x"

class SpatialKNNRequest(BaseModel):
    table: str
    point: List[float] = Field(..., description="[x,y] o [x,y,z]")
    k: int = 5
    coord_column: str = "x"

# Helpers

def _normalize_index(name: str) -> IndexType:
    n = (name or "").upper()
    mapping = {
        "SEQ": IndexType.SEQUENTIAL,
        "SEQUENTIAL": IndexType.SEQUENTIAL,
        "ISAM": IndexType.ISAM,
        "EXTHASH": IndexType.EXTENDIBLE_HASH,
        "EXTENDIBLE_HASH": IndexType.EXTENDIBLE_HASH,
        "BTREE": IndexType.BTREE,
        "BPTREE": IndexType.BTREE,
        "BPTREE_CLUSTERED": IndexType.BPTREE_CLUSTERED,
        "RTREE": IndexType.RTREE,
    }
    if n not in mapping:
        raise HTTPException(status_code=400, detail=f"Índice no soportado: {name}")
    return mapping[n]

def _as_sql_value(v: Any) -> str:
    if isinstance(v, str):
        return f"'{v}'"
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, list):
        inner = ", ".join(_as_sql_value(x) for x in v)
        return f"[{inner}]"
    return str(v)

@router.post("/sql")
def execute_sql(req: SQLQuery):
    """
    Ejecuta SQL libre contra el Engine (usa tu parser_sql y engine.execute).
    Devuelve {status, rows, message}, igual a engine.execute, con timing.
    """
    t0 = time.perf_counter()
    try:
        result = eng.execute(req.query)
        result["_elapsed_ms"] = round((time.perf_counter() - t0) * 1000, 2)
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/tables")
def list_tables():
    """
    Lista tablas registradas en el Engine (name, key, idx_type, columns).
    """
    out = []
    for name, th in eng.tables.items():
        out.append({
            "name": name,
            "key": th.key,
            "idx_type": th.idx_type.name,
            "columns": [f.name for f in th.schema.fields],
        })
    return {"tables": out}


@router.post("/upload-csv")
async def upload_csv(file: UploadFile = File(...)):
    data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data"))
    os.makedirs(data_dir, exist_ok=True)
    dest = os.path.join(data_dir, file.filename)

    content = await file.read()
    with open(dest, "wb") as f:
        f.write(content)

    return {"status": "ok", "path": dest}


@router.post("/load-csv")
def load_csv(req: LoadCSVRequest):
    idx = _normalize_index(req.index_type)

    # Validación simple existencia del CSV
    if not os.path.exists(req.csv_path):
        raise HTTPException(status_code=400, detail=f"No existe el archivo: {req.csv_path}")
    sql = (
        f'CREATE TABLE {req.table_name} FROM FILE "{req.csv_path}" '
        f'USING INDEX {idx.name}("{req.key_column}")'
    )

    try:
        result = eng.execute(sql)
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/spatial/range")
def spatial_range(req: SpatialRangeRequest):

    p = req.point
    if len(p) not in (2, 3):
        raise HTTPException(status_code=400, detail="point debe ser [x,y] o [x,y,z]")
    if req.radius is not None:
        arr = [*p, float(req.radius)]
    else:
        arr = p

    sql = f"SELECT * FROM {req.table} WHERE {req.coord_column} IN (point, {_as_sql_value(arr)})"
    try:
        return eng.execute(sql)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/spatial/knn")
def spatial_knn(req: SpatialKNNRequest):
    p = req.point
    if len(p) not in (2, 3):
        raise HTTPException(status_code=400, detail="point debe ser [x,y] o [x,y,z]")

    sql = f"SELECT * FROM {req.table} WHERE {req.coord_column} IN ({req.k}, {_as_sql_value(p)})"
    try:
        return eng.execute(sql)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))