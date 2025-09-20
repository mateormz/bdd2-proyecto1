# BDD2 - Proyecto 1

Repo mínimo para levantar la API con Docker Compose.

## Requisitos
- Docker y Docker Compose

## Cómo correr

```bash
docker compose up --build -d
```

## Estructura

```plaintext
bdd2-proyecto1/
├─ README.md
├─ .gitignore
├─ .env
├─ docker-compose.yml
├─ Dockerfile
├─ requirements.txt
├─ data/                # Datasets
├─ out/                 # Binarios generados (.dat, .idx, logs)
└─ src/
   ├─ app.py            # API principal FastAPI
   ├─ parser_sql.py     # ParserSQL
   ├─ engine.py         # Query Engine + Optimizer básico
   ├─ catalog.py        # Catálogo dinámico de tablas/índices
   ├─ sequential.py     # Sequential File
   ├─ isam.py           # ISAM
   ├─ ext_hash.py       # Extendible Hash
   ├─ bptree.py         # B+Tree
   ├─ rtree_adapter.py  # RTree
   ├─ io_counters.py    # Métricas de I/O y tiempos
   └─ utils.py          # Helpers: carga de CSV, validaciones, etc.
```
