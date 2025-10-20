# backend/src/app.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routes import router as api_router

app = FastAPI(title="Mini SGBD 3D", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # ["http://localhost:5173"]
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router, prefix="/api")

@app.get("/health")
def health():
    return {"status": "ok"}