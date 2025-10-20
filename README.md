````markdown
# BD2 — Proyecto 1
**Organización e Indexación Eficiente de Archivos con Datos Tabulares y Espaciales**

SGBD didáctico con API HTTP para comparar **Sequential/AVL**, **ISAM**, **B+ Tree**, **Extendible Hashing** y **R-Tree**.  
Esta entrega trabaja con datos **tabulares (CSV)** y **espaciales** (coordenadas/regiones).

---

## Contenido
- [Introducción](#introducción)
- [Objetivos](#objetivos)
- [Arquitectura y Repositorio](#arquitectura-y-repositorio)
  - [Despliegue con Docker Compose](#despliegue-con-docker-compose)
  - [Estructura del Repositorio](#estructura-del-repositorio)
  - [Endpoints principales](#endpoints-principales)
- [Técnicas de Indexación y Operaciones](#técnicas-de-indexación-y-operaciones)
  - [Parser SQL](#parser-sql)
- [Resumen Técnico de Implementación](#resumen-técnico-de-implementación)
  - [AVL on-disk](#avl-on-disk)
  - [B-tree--clustered-file](#b-tree--clustered-file)
  - [Extendible Hashing](#extendible-hashing)
  - [R-tree-adapter](#r-tree-adapter)
  - [Backend y Endpoints](#backend-y-endpoints)
  - [Artefactos en `out/`](#artefactos-en-out)
- [Síntesis de soporte por índice](#síntesis-de-soporte-por-índice)
- [Guía rápida de elección](#guía-rápida-de-elección)
- [Limitaciones y riesgos](#limitaciones-y-riesgos)
- [Metodología de medición](#metodología-de-medición)
- [Complejidad Teórica en I/O](#complejidad-teórica-en-io)
- [Resultados (plantilla)](#resultados-plantilla)
- [FrontEnd y Pruebas de Uso](#frontend-y-pruebas-de-uso)
- [Conclusiones](#conclusiones)
- [Trabajo Futuro](#trabajo-futuro)
- [Apéndices](#apéndices)
  - [Comandos de Ejecución](#comandos-de-ejecución)
  - [SQL de ejemplo](#sql-de-ejemplo)
- [Créditos](#créditos)

---

## Introducción
El proyecto propone el diseño e implementación de un **sistema de base de datos multimodal** capaz de indexar y consultar datos estructurados y no estructurados. Nuestra solución presenta una API backend con procesamiento de consultas, subsistema de indexación y persistencia en disco; y aplicaciones *front-end* para ejecutar pruebas y revisar resultados. El sistema está pensado para manejar distintos tipos de datos según uso (texto, imágenes, audio, video y tablas), aunque en esta entrega se trabaja únicamente con datos **tabulares** (CSV) y **espaciales** (coordenadas y regiones).

Para este proyecto, implementamos y comparamos las siguientes técnicas de indexación: **Sequential/AVL**, **ISAM**, **B+ Tree**, **Extendible Hashing** y **R-Tree**. El motor registra `#lecturas`, `#escrituras` y tiempo de ejecución (ms) para medir el costo en memoria secundaria en escenarios de *insert*, *search* (hit/miss), *range* y *remove*.

**Aportes del proyecto:**
- Motor en disco con *parser* SQL-like, *planner/executor* y *catalog*.
- Implementación de índices primarios y secundarios con reglas de inserción, búsqueda, rango y eliminación.
- Adaptador de R-Tree para datos espaciales y consultas de rango/kNN (según implementación).
- API HTTP y una interfaz mínima para ejecutar consultas y visualizar resultados.
- Metodología experimental para comparar técnicas usando contadores de I/O y tiempo.

---

## Objetivos

### Objetivo general
Diseñar, implementar e integrar las estructuras de datos vistas en clase para evaluar su desempeño en distintos contextos de uso (tipo de dato y patrón de acceso). El propósito no es declarar una estructura “mejor” en términos absolutos, sino **mostrar en qué escenarios cada una resulta adecuada** y cuáles son sus límites.

### Objetivos específicos
- Implementar *Sequential/AVL*, *ISAM*, *B+ Tree*, *Extendible Hashing* y *R-Tree* con sus operaciones básicas (*insert*, *search*, *range*, *remove*; y *range/kNN* para R-Tree).
- Integrar los índices al motor (parser SQL-like, planner/executor y catálogo) y persistir en disco.
- Definir conjuntos de datos tabulares y espaciales y escenarios de prueba (hit/miss, rango por ventanas, cargas por lotes).
- Medir **costo en memoria secundaria** (`#lecturas/#escrituras`) y **latencia** (ms); verificar **correctitud** de resultados (y *margen de error* cuando aplique, p. ej., heurísticas espaciales/kNN).
- Comparar resultados y **caracterizar los *trade-offs*** (rendimiento, soporte de rango, manejo de duplicados/overflow, tamaño en disco).
- Elaborar una **guía de elección** de índice según patrón de acceso y tipo de dato.
- Entregar una API reproducible (Docker) y scripts de experimentación para replicar métricas.

---

## Arquitectura y Repositorio

### Despliegue con Docker Compose
El **backend** es Python y persiste sus estructuras en `./out/`. No requiere un motor externo de BD; solo filesystem. El índice espacial usa `rtree` (requiere `libspatialindex`). Para que los archivos de índices sobrevivan a reinicios, se mapea `./out` como volumen.

> Requisitos mínimos: Python 3.10+, `pip install rtree`. En Docker, instalar `libspatialindex` **antes** de `pip install rtree`.

```yaml
# compose.yml (mínimo viable)
services:
  backend:
    build: ./backend
    ports: ["8000:8000"]
    volumes:
      - ./backend/out:/app/out
      - ./backend/data:/app/data
    environment:
      - PYTHONUNBUFFERED=1
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/health"]
      interval: 10s
      timeout: 3s
      retries: 3
  frontend:
    build: ./frontend
    ports: ["5173:5173"]
    depends_on: [backend]
````

**Ejecutar:**

```bash
docker compose up --build -d
# Backend: http://localhost:8000/docs
# Frontend: http://localhost:5173
```

**Persistencia:** los índices se escriben en `./backend/out`, incluyendo `.dat`, `.idx` y el directorio del R-Tree.

### Estructura del Repositorio

```
backend/src/index/
├─ avl.py              # Índice AVL on-disk (índice + heap de datos)
├─ bptree.py           # B+ Tree (clustered): .dat ordenado + .idx (pickle)
├─ ext_hash.py         # Extendible Hash: directorio + buckets fijos
├─ isam.py             # ISAM: base ordenada + overflow + 2 niveles de índice
└─ rtree_adapter.py    # R-Tree (rtree/libspatialindex) para consultas espaciales
```

### Endpoints principales

* `GET /health` — verificación de estado (usado por *healthcheck* de Docker).
* `POST /api/sql` — ejecuta una consulta SQL-like sobre las estructuras disponibles (punto, rango y operaciones espaciales según implementación).

---

## Técnicas de Indexación y Operaciones

**Sequential/AVL.**
Modelo *heap + índice* sobre archivo binario. El AVL mantiene claves y offsets al heap, rebalancea con rotaciones y soporta *search* (devuelve duplicados) y *range*. Ventajas: rango ordenado y duplicados correctos. Costes: mantenimiento de árbol y ausencia de borrado físico (se propone *lazy delete*).

**ISAM.**
Estructura estática basada en hojas ordenadas con *overflow areas*. Útil cuando las inserciones reales se dan por lotes; coste de búsqueda cercano a `O(log_B N)`. (En el código convive con las demás, pero la parte práctica se centra en AVL/B+ Tree/Hash.)

**B+ Tree (fanout alto).**
Archivo `.dat` ordenado por clave (*clustered*) y árbol B+ serializado. Las hojas están encadenadas, lo que hace eficiente *range* y *order by*. Inserción/borrado reescriben el archivo y reconstruyen el índice (diseño didáctico), por lo que las actualizaciones son más costosas que las lecturas.

**Extendible Hashing (BLOCK_FACTOR, MAX_CHAINING).**
Igualdad con latencia estable. Directorio en disco + *buckets* de tamaño fijo; encadenamiento limitado. Si se supera, se dispara un *rehash* global que incrementa la profundidad y redistribuye. No soporta rango.

**R-Tree (datos espaciales).**
Índice espacial en disco con MBRs. Insertamos puntos como MBR degenerados; *rangeSearch* combina filtro por intersección de MBR con verificación por distancia euclídea (ordena por `dist`). *kNN* usa el operador `nearest`. El rendimiento depende de solapamiento y distribución.

### Parser SQL

El parser traduce a planes simples:

* `CREATE TABLE ... FROM FILE ...` — carga CSV, construye `.dat` e índice.
* `SELECT ... WHERE id = c` — igualdad (Hash / AVL / B+ Tree).
* `SELECT ... WHERE nombre BETWEEN a AND b` — rango (AVL / B+ Tree).
* `... WHERE ubicacion IN (POINT, RADIUS)` — espacial (R-Tree).

**Ejemplo:**

```sql
CREATE TABLE Restaurantes (
  id INT KEY INDEX SEQ,
  nombre VARCHAR[20] INDEX BTree,
  fechaRegistro DATE,
  ubicacion ARRAY[FLOAT] INDEX RTree
);

CREATE TABLE Restaurantes FROM FILE "C:\restaurantes.csv"
  USING INDEX ISAM("id");

SELECT * FROM Restaurantes WHERE id = 42;
SELECT * FROM Restaurantes WHERE nombre BETWEEN "A" AND "H";
SELECT * FROM Restaurantes WHERE ubicacion IN (POINT(-77.03, -12.05), RADIUS 5.0);
```

---

## Resumen Técnico de Implementación

### AVL on-disk

**Layout/Persistencia.**
Archivo de **datos** binario con cabecera (`AVLDAT01`, versión, tamaño de registro) y archivo de **índice** con cabecera (`AVLIDX01`, raíz, conteo, tamaño de nodo). Cada nodo: `(key, height, left_off, right_off, value_off)`.
**Operaciones.**
`insert` con rotaciones; `search` retorna **todas** las coincidencias; `rangeSearch [l, r]` por *in-order*.
**I/O y métricas.**
Contadores de lecturas/escrituras separados; típico `O(log N + k)` en rango. Borrado físico no implementado (proponer *lazy delete*).

### B+ Tree — *clustered file*

**Layout/Persistencia.**
Desde CSV: ordenar por clave, volcar `.dat` y hacer *bulk-load* del árbol (`.idx`). Hojas encadenadas; fanouts altos.
**Operaciones.**
`search` y `range_search` devuelven offsets del `.dat`. `insert/remove`: reescritura del archivo y **rebuild** del árbol (`O(N)` por actualización).
**I/O y métricas.**
*Range* muy eficiente; mantenimiento caro al actualizar (diseño didáctico explícito).

### Extendible Hashing — *bucket file*

**Layout/Persistencia.**
Archivo inicia con profundidad global `D` (32 bits). Directorio lineal y *buckets* fijos (con puntero a overflow). Encadenamiento limitado; si se excede → *rehash* global (incrementa `D` y redistribuye).
**Operaciones.**
`insert`, `search`, `remove`. Búsqueda recorre bucket base y su cadena.
**I/O y métricas.**
Igualdad promedio `O(1+α)`; **no** soporta rango. Importa parametrización para evitar rehash frecuente.

### R-Tree (adapter)

**Layout/Persistencia.**
Índice bajo `out/rtree_index`. Puntos como MBR degenerados `[x,x]×[y,y]`.
**Operaciones.**
`add/remove`; `rangeSearch` = MBR-intersect + verificación euclídea; `kNN` con `nearest`.
**I/O y métricas.**
Depende de solapamiento y distribución (reportar lecturas/escrituras y tiempo con distintos radios).

### Backend y Endpoints

* `GET /health` — usado en *healthcheck*.
* `POST /api/sql` — `=` y `between` (tabular), `IN(POINT, RADIUS)` (espacial).

### Artefactos en `out/`

* **AVL:** `<tabla>.dat` (heap de datos), `<tabla>.idx` (árbol).
* **B+ Tree:** `<tabla>.dat` (ordenado), `<tabla>.idx` (índice serializado).
* **Extendible Hash:** `<tabla>.dat` (directorio + buckets).
* **R-Tree:** `rtree_index/` (archivos del índice).

---

## Síntesis de soporte por índice

| Índice             | Ins. | Eq. | Rango | Rem. | kNN | Pers.          | Observaciones                                                              |
| ------------------ | :--: | :-: | :---: | :--: | :-: | -------------- | -------------------------------------------------------------------------- |
| Sequential/AVL     |  Sí  |  Sí |   Sí  |  No  | n/a | `.dat + .idx`  | Devuelve duplicados; no hay borrado físico (proponer *lazy delete*).       |
| B+ Tree            |  Sí  |  Sí |   Sí  |  Sí  | n/a | `.dat + .idx`  | *Range* eficiente; `insert/remove` reescriben y reconstruyen (clustered).  |
| Extendible Hashing |  Sí  |  Sí |   No  |  Sí  | n/a | `.dat`         | *Rehash* al exceder *chaining*; ajustar `BLOCK_FACTOR`/`MAX_CHAINING`.     |
| R-Tree             |  Sí  | n/a |   Sí  |  Sí  |  Sí | `rtree_index/` | Filtro MBR + verificación por distancia; coste depende de la distribución. |

---

## Guía rápida de elección

| Patrón de acceso                                    | Índice recomendado                                                   |
| --------------------------------------------------- | -------------------------------------------------------------------- |
| Consultas por igualdad en clave, sin rango          | **Extendible Hashing** → latencia estable; sin soporte de rango.     |
| Rangos ordenados frecuentes (textuales o numéricos) | **B+ Tree** → hojas encadenadas; *range*/*order by* eficientes.      |
| Claves con duplicados y necesidad de rango          | **AVL** → correcto en punto/rango; considerar coste de mantenimiento |
| Consultas espaciales (circular/rectangular) o kNN   | **R-Tree** → MBR + heurísticas; medir con distintas coberturas.      |

---

## Limitaciones y riesgos

* **AVL:** codec rígido; ausencia de `delete` físico (proponer *tombstones* + *compaction*).
* **B+ Tree:** mantenimiento `O(N)` por actualización (decisión *clustered*).
* **Ext. Hash:** parámetros conservadores pueden disparar *rehash* (impacto en I/O).
* **R-Tree:** sólo almacena `id` y bbox; atributos se resuelven fuera (join por offset/clave).

---

## Metodología de medición

* **Escenarios:** *search* hit/miss, *range* (ventanas pequeña/media/grande), *insert/remove* por lotes; en espacial: radios 0.5%, 5%, 20% del universo.
* **Métricas:** `#lecturas`, `#escrituras` y tiempo (ms). Media y desviación estándar (3–5 repeticiones).
* **Reporte:** tabla comparativa y gráfico por operación (incluir tiempos y contadores I/O).

---

## Complejidad Teórica en I/O

| Técnica            | Costos (accesos a índice + datos)                                                                                                |
| ------------------ | -------------------------------------------------------------------------------------------------------------------------------- |
| Sequential/AVL     | Búsqueda `O(log N + t)`; Rango `O(log N + k)`; Inserción `O(log N)`; Eliminación `O(log N)` por ocurrencia; Compactación `O(N)`. |
| ISAM               | Búsqueda `O(log_B N)`; Inserción amortiza con splits/overflow; Eliminación `O(log_B N)`.                                         |
| Extendible Hashing | Búsqueda `O(1+α)`; Inserción `O(1)` promedio; **no** soporte de rango.                                                           |
| B+ Tree            | Búsqueda `O(log_B N)`; Rango `O(log_B N + k)`; Inserción/Eliminación `O(log_B N)`.                                               |
| R-Tree             | Rango/kNN: dependiente de MBRs y heurística; promedio sublineal con buen *packing*.                                              |

---

## Resultados (plantilla)

> Completar con mediciones y contadores de I/O.

| Técnica            | Insert (ms) | Search (ms) | Range (ms) | Remove (ms) |
| ------------------ | ----------- | ----------- | ---------- | ----------- |
| Sequential/AVL     | —           | —           | —          | —           |
| ISAM               | —           | —           | —          | —           |
| Extendible Hashing | —           | —           | n/a        | —           |
| B+ Tree            | —           | —           | —          | —           |
| R-Tree             | —           | —           | —          | —           |

> **Gráfico**: incluir imagen del chart (renderizado externo) en `docs/tiempos.png` o similar.

---

## FrontEnd y Pruebas de Uso

La UI permite enviar consultas SQL al backend, visualizar resultados tabulares, cargar CSV y explorar índices. Se incluyen capturas y casos de uso que evidencian el aporte de los índices.

![Vista del Frontend del SGBD](Imagen%20de%20WhatsApp%202025-10-19%20a%20las%2023.54.25_e0e05edf.jpg)

---

## Conclusiones

Resumen de hallazgos, límites y escenarios recomendados por técnica.

---

## Trabajo Futuro

Optimización de I/O, políticas de split/merge, caching, compresión y extensión a búsquedas multimodales.

---

## Apéndices

### Comandos de Ejecución

```bash
docker compose up --build -d
# Backend: http://localhost:8000/docs
# Frontend: http://localhost:5173
```

### SQL de ejemplo

```sql
SELECT * FROM Restaurantes WHERE id = 42;
SELECT * FROM Restaurantes WHERE nombre BETWEEN "A" AND "H";
SELECT * FROM Restaurantes WHERE ubicacion IN (POINT(-77.03, -12.05), RADIUS 5.0);
```

---

## Créditos

* **Curso:** CS2702 — Base de Datos II (2025-I)
* **Profesor:** PhD. Heider Ysaias Sánchez Enríquez
* **Universidad:** Universidad de Ingeniería y Tecnología (UTEC)

**Integrantes**

* Badi Masud Rodriguez Ramirez — 202310299
* Santiago Miguel Silva Reyes — 202310266
* Edson Gustavo Guardamino Felipe — 202010136
* Alair Jairo Catacora Tupa — *(código)*
* Mateo Elias Ramirez Chuquimarca — 202310082

