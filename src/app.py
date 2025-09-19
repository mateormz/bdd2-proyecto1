from fastapi import FastAPI

app = FastAPI(title="CS2702 – Proyecto 1 (Backend mínimo)")

@app.get("/health")
def health():
    return {"status": "ok"}