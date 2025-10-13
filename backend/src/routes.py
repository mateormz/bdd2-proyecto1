from fastapi import APIRouter
from index.rtree_adapter import RTree

router = APIRouter()

@router.get("/health")
def health():
	return {"status": "ok"}
