import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.routes import router

app = FastAPI(title="CS2702 – Proyecto 1 (Backend)")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router, prefix="/api")