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
├─ backend
│   ├─ Dockerfile
│   ├─ data                         # Datasets (.csv)
│   │   └─ Employers_data.csv
│   ├─ out                          # Archivos binarios generados (.dat, .idx, logs) 
│   │   ├─ bptree.dat
│   │   ├─ ext_hash.dat
│   │   ├─ isam.dat
│   │   ├─ rtree_adapter.dat
│   │   └─ sequential.dat
│   ├─ requirements.txt
│   └─ src
│       ├─ app.py                   # API principal con FastAPI
│       ├─ catalog.py               # Catálogo dinámico de tablas/índices
│       ├─ engine.py                # Query Engine + Optimizer básico
│       ├─ index                    # Carpeta con los índices
│       │   ├─ bptree.py            # B+Tree
│       │   ├─ ext_hash.py          # Extendible Hash
│       │   ├─ isam.py              # ISAM
│       │   ├─ rtree_adapter.py     # RTree
│       │   └─ avl.py               # AVL File
│       ├─ io_counters.py           # Métricas de I/O y tiempos
│       ├─ parser_sql.py            # ParserSQL
│       ├─ routes.py                # Rutas de la API
│       ├─ utils.py                 # Helpers: carga de CSV, validaciones, etc.
│       └─ core                     # Núcleo: definición de esquemas y estructuras
│           └─ schema.py            # Definición de esquemas de tablas/atributos
├─ compose.yml
└─ frontend
    ├─ Dockerfile
    ├─ README.md
    ├─ eslint.config.js
    ├─ index.html
    ├─ package-lock.json
    ├─ package.json
    ├─ public
    │   └─ vite.svg
    ├─ src
    │   ├─ App.css
    │   ├─ App.jsx
    │   ├─ assets
    │   │   └─ react.svg
    │   ├─ components              
    │   │   └─ GetHealth.jsx
    │   ├─ index.css
    │   ├─ main.jsx
    │   ├─ pages                   
    │   │   └─ Home.jsx
    │   └─ utils                   
    │       └─ api.js
    └─ vite.config.js
