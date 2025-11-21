# BDD2 - Proyecto 1

Repo mínimo para levantar la API con Docker Compose.

## Informe y Vide
- https://drive.google.com/drive/folders/19ZQRmRFhGbwM9wMsyloXNiheqUaLXiYA?usp=sharing

## Requisitos
- Docker y Docker Compose

## Cómo correr

```bash
docker compose up --build -d
```

## Estructura

```plaintext
bdd2-proyecto2/
├─ README.md
├─ compose.yml
│
├─ backend
│   ├─ Dockerfile
│   ├─ requirements.txt
│   ├─ data
│   │   ├─ Employers_data.csv
│   │   ├─ text_dataset/                 # Dataset textual
│   │   │   └─ *.txt / *.csv
│   │   ├─ images/                       # Dataset de imágenes
│   │   └─ audio/                        # Dataset de audio
│   │
│   ├─ out                               # Archivos generados
│   │   ├─ indexes_text/                 # Índice invertido textual
│   │   │   ├─ lexicon.dat
│   │   │   ├─ postings.dat
│   │   │   └─ norms.dat
│   │   ├─ spimi_blocks/                 # Bloques intermedios del SPIMI
│   │   ├─ bow/                          # Bag of Visual/Acoustic Words
│   │   │   ├─ codebook.npy
│   │   │   ├─ histograms.dat
│   │   │   └─ inverted_index.dat        # Índice invertido multimedia
│   │   └─ logs/
│   │
│   └─ src
│       ├─ app.py                        # API principal
│       ├─ routes.py                     # Nuevas rutas API text + multimedia
│       ├─ engine.py                     # Query Engine general
│       ├─ io_counters.py                # Métricas
│       ├─ utils.py                      # Helpers generales
│       │
│       ├─ text_index/                   # NUEVO: módulo de índice invertido textual
│       │   ├─ tokenizer.py              # Tokenización, stopwords, stemming
│       │   ├─ spimi.py                  # Implementación de SPIMI + merge
│       │   ├─ tfidf.py                  # Cálculo de TF-IDF + normas
│       │   ├─ inverted_index.py         # Lectura/escritura del índice
│       │   └─ search_text.py            # Búsqueda con similitud de coseno
│       │
│       ├─ multimedia_index/             # NUEVO: todo lo visual/audio
│       │   ├─ features/
│       │   │   ├─ sift_extractor.py     # Extractor SIFT
│       │   │   ├─ resnet_extractor.py   # Extractor CNN
│       │   │   └─ mfcc_extractor.py     # Extractor MFCC
│       │   ├─ codebook.py               # K-Means para crear visual words
│       │   ├─ histograms.py             # Construcción de histogramas
│       │   ├─ knn_sequential.py         # KNN secuencial sobre histogramas
│       │   ├─ knn_inverted.py           # KNN con índice invertido multimedia
│       │   └─ search_media.py           # Lógica de consulta completa
│       │
│       ├─ catalog.py                    # Catálogo actualizado (texto + multimedia)
│       ├─ parser_sql.py                 # Soporte de consultas tipo SQL
│       └─ core
│           └─ schema.py
│
└─ frontend
    ├─ Dockerfile
    ├─ public/
    └─ src
        ├─ App.jsx
        ├─ index.css
        │
        ├─ pages/
        │   ├─ Home.jsx
        │   ├─ TextSearch.jsx           # NUEVA vista
        │   └─ MediaSearch.jsx          # NUEVA vista
        │
        ├─ components/
        │   ├─ TextQueryBox.jsx         # Caja de búsqueda textual
        │   ├─ MediaUploader.jsx        # Uploader de imagen/audio
        │   ├─ ResultsText.jsx          # Render de resultados textuales
        │   ├─ ResultsMedia.jsx         # Render de imágenes/audio
        │   └─ PerformanceCharts.jsx    # Gráficos comparativos Experimentación
        │
        ├─ utils/api.js
        └─ assets/
