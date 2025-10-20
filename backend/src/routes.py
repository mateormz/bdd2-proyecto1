# backend/src/routes.py
from __future__ import annotations
import os
import pickle
import time
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, UploadFile, File, Form
from pydantic import BaseModel

# Importa core
from src.engine import Engine
from src.catalog import Catalog
from src.parser_sql import parse_sql, CreateTableStatement, SelectStatement, InsertStatement, DeleteStatement
from src.core.schema import Schema, Field, Kind

router = APIRouter()

HERE = Path(__file__).resolve()
SRC_DIR = HERE.parent
BACKEND_DIR = SRC_DIR.parent
OUT_DIR = BACKEND_DIR / "out"
OUT_UPLOADS = OUT_DIR / "uploads"
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_UPLOADS.mkdir(parents=True, exist_ok=True)

CATALOG_PATH = OUT_DIR / "catalog.pickle"

_engine: Optional[Engine] = None
_catalog: Optional[Catalog] = None

def _load_or_init_catalog() -> Catalog:
    global _catalog
    if _catalog is not None:
        return _catalog
    if CATALOG_PATH.exists():
        with open(CATALOG_PATH, "rb") as fh:
            _catalog = pickle.load(fh)
    else:
        _catalog = Catalog()
    return _catalog

def _save_catalog() -> None:
    if _catalog is None:
        return
    CATALOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(CATALOG_PATH, "wb") as fh:
        pickle.dump(_catalog, fh)

def _get_engine() -> Engine:
    global _engine
    if _engine is None:
        cat = _load_or_init_catalog()
        _engine = Engine(cat)
    return _engine

class SQLBody(BaseModel):
    sql: str

@router.get("/health")
def health():
    return {"status": "ok"}

@router.get("/catalog")
def get_catalog():
    """Vista rápida del catálogo (tablas, data_path, índices)."""
    cat = _load_or_init_catalog()
    tables = []
    for tname, meta in cat.tables.items():
        schema: Schema = meta["schema"]
        fields = [{"name": f.name, "kind": f.kind.name, "size": f.size} for f in schema.fields]
        idxs = []
        for col, imeta in meta.get("indexes", {}).items():
            # acepta tanto forma plana como anidada
            if "type" in imeta and "path" in imeta:
                idxs.append({"column": col, "type": getattr(imeta["type"], "name", str(imeta["type"])), "path": imeta["path"]})
            else:
                for _, v in imeta.items():
                    idxs.append({"column": col, "type": getattr(v["type"], "name", str(v["type"])), "path": v["path"]})
        tables.append({
            "name": tname,
            "data_path": meta.get("data_path"),
            "fields": fields,
            "indexes": idxs,
        })
    return {"status": "success", "tables": tables}

@router.post("/sql")
def execute_sql(body: SQLBody):
    """
    Ejecuta SQL con el Engine real.
    Devuelve { status, rows|ok, metrics, message, timing_ms }
    """
    engine = _get_engine()
    sql = body.sql.strip()
    if not sql:
        raise HTTPException(status_code=400, detail="Query vacía")

    t0 = time.perf_counter()
    try:
        res = engine.execute(sql)
        _save_catalog()

        timing_ms = round((time.perf_counter() - t0) * 1000, 3)
        if "rows" in res:
            return {"status": "success", "rows": res["rows"], "metrics": res.get("metrics", {}), "timing_ms": timing_ms}
        else:
            return {"status": "success", **{k: v for k, v in res.items()}, "timing_ms": timing_ms}
    except NotImplementedError as e:
        raise HTTPException(status_code=501, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fallo ejecutando SQL: {e}")

@router.post("/upload")
def upload_csv(file: UploadFile = File(...)):
    """
    Sube un CSV al servidor y retorna su ruta absoluta (para CREATE ... FROM FILE "...")
    """
    try:
        fname = file.filename or "data.csv"
        if not fname.lower().endswith(".csv"):
            fname += ".csv"
        dst = OUT_UPLOADS / fname
        if dst.exists():
            stem = dst.stem
            ext = dst.suffix
            i = 1
            while True:
                cand = dst.with_name(f"{stem}_{i}{ext}")
                if not cand.exists():
                    dst = cand
                    break
                i += 1
        with open(dst, "wb") as fh:
            shutil.copyfileobj(file.file, fh)
        return {"status": "success", "path": str(dst)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"No se pudo subir el archivo: {e}")