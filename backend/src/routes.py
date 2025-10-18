from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import json

router = APIRouter()

# Modelos para las requests
class SQLQuery(BaseModel):
    query: str

# Datos hardcodeados de las tablas (misma info que en el frontend)
TABLES_DATA = [
    {
        "name": "Customers",
        "columns": [
            {"name": "ID del cliente", "type": "entero"},
            {"name": "nombre de pila", "type": "varchar[100]"},
            {"name": "apellidos", "type": "varchar[100]"},
            {"name": "edad", "type": "entero"},
            {"name": "pais", "type": "varchar[50]"}
        ]
    },
    {
        "name": "Pedidos",
        "columns": [
            {"name": "ID del pedido", "type": "entero"},
            {"name": "articulo", "type": "varchar[100]"},
            {"name": "cantidad", "type": "entero"},
            {"name": "ID del cliente", "type": "entero"}
        ]
    },
    {
        "name": "Envios",
        "columns": [
            {"name": "ID de envio", "type": "entero"},
            {"name": "estado", "type": "entero"},
            {"name": "cliente", "type": "entero"}
        ]
    }
]

# Datos de ejemplo para los resultados de consultas
SAMPLE_RESULTS = [
    {"nombre de pila": "John", "edad": 31},
    {"nombre de pila": "Roberto", "edad": 22},
    {"nombre de pila": "David", "edad": 22},
    {"nombre de pila": "John", "edad": 25},
    {"nombre de pila": "Betty", "edad": 28}
]

@router.get("/health")
def health():
    return {"status": "ok"}

@router.get("/tables")
def get_tables():
    """Endpoint para obtener la lista de tablas disponibles"""
    return {
        "status": "success",
        "tables": TABLES_DATA
    }

@router.post("/execute")
def execute_sql(sql_query: SQLQuery):
    """Endpoint para ejecutar consultas SQL"""
    try:
        query = sql_query.query.strip()
        
        # Simulación básica de ejecución de consultas
        if not query:
            raise HTTPException(status_code=400, detail="Query vacía")
        
        # Normalizar query para análisis básico
        query_upper = query.upper()
        
        if query_upper.startswith("SELECT"):
            # Para consultas SELECT, devolver datos de ejemplo
            if "CUSTOMERS" in query_upper or "CUSTOMER" in query_upper:
                return {
                    "status": "success",
                    "result": SAMPLE_RESULTS,
                    "query_executed": query,
                    "execution_time": "0.045s",
                    "rows_affected": len(SAMPLE_RESULTS)
                }
            else:
                # Para otras tablas, devolver resultado vacío o datos genéricos
                return {
                    "status": "success", 
                    "result": [],
                    "query_executed": query,
                    "execution_time": "0.032s",
                    "rows_affected": 0
                }
        
        elif query_upper.startswith("INSERT"):
            return {
                "status": "success",
                "result": [],
                "message": "1 fila insertada exitosamente",
                "query_executed": query,
                "execution_time": "0.028s",
                "rows_affected": 1
            }
        
        elif query_upper.startswith("DELETE"):
            return {
                "status": "success",
                "result": [],
                "message": "Filas eliminadas exitosamente",
                "query_executed": query,
                "execution_time": "0.035s",
                "rows_affected": 2
            }
        
        elif query_upper.startswith("CREATE"):
            return {
                "status": "success",
                "result": [],
                "message": "Tabla creada exitosamente",
                "query_executed": query,
                "execution_time": "0.123s",
                "rows_affected": 0
            }
        
        else:
            return {
                "status": "success",
                "result": [],
                "message": "Consulta ejecutada exitosamente",
                "query_executed": query,
                "execution_time": "0.042s",
                "rows_affected": 0
            }
            
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Error al ejecutar la consulta: {str(e)}"
        )
