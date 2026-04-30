# Tarea 1 — Sistemas Distribuidos 2026-1
### Plataforma de análisis de consultas geoespaciales con caché

**Profesor:** Nicolás Hidalgo
**Stack:** Python 3.12, FastAPI, Redis 7.4, Docker Compose v2

Este repositorio implementa el **Entregable 1** de la Tarea 1: cuatro
servicios distribuidos (Generador de Tráfico, Caché, Generador de Respuestas
y Almacenamiento de Métricas) coordinados por `docker compose` y respaldados
por Redis. El sistema procesa consultas Q1–Q5 sobre el dataset Google Open
Buildings (subconjunto correspondiente a la Región Metropolitana de Santiago)
precargado en memoria.

---

## Prerrequisitos

| Herramienta | Notas |
|---|---|
| **Docker Engine + Compose v2** | Se invoca como `docker compose` (con espacio, no `docker-compose`). Verifica con `docker compose version`. |
| **Python 3.10+** | Necesario solo para generar el dataset y correr los scripts de experimentos. |
| **redis-cli** *(opcional)* | Sólo lo usa `experiments/master_run.py` para reconfigurar políticas en runtime. |

> Todas las instrucciones asumen `docker compose` v2 (sin guión). Si tu
> sistema sólo tiene el comando legacy `docker-compose`, los scripts no
> funcionarán sin reemplazar el comando.

---

## Estructura del repositorio

```
.
├── data/santiago_buildings.parquet       # Subconjunto Open Buildings (≈ 8.5 MB)
├── traffic_generator/                    # Servicio 1 — Generador de Tráfico
├── cache_service/                        # Servicio 2 — Caché (Redis)
├── response_generator/                   # Servicio 3 — Generador de Respuestas
├── metrics_service/                      # Servicio 4 — Almacenamiento de Métricas
├── experiments/
│   ├── master_run.py                     # Batería completa (22 corridas)
│   └── build_figures.py                  # 7 figuras del informe
├── results/                              # Snapshots JSON de cada experimento
├── docker-compose.yml                    # Definición del stack
├── .env                                  # Configuración (política, tamaño, TTLs)
└── Tarea_1_Sistemas_Distribuidos_2026_1.pdf
```

---

## Arquitectura (4 servicios + Redis)

| Servicio | Puerto | Rol según el enunciado |
|---|---|---|
| `traffic_generator` | 8000 | Genera consultas Q1–Q5 con distribuciones Zipf y Uniforme. |
| `cache_service`     | 8001 | Intercepta consultas; sirve hits desde Redis o delega misses. |
| `response_generator`| 8002 | Calcula Q1–Q5 sobre datos precargados en memoria. |
| `metrics`           | 8003 | Registra hits, misses, latencias, throughput y evicciones. |
| `redis` (backing)   | 6379 | Almacén del caché con TTL y políticas de evicción. |

Las **cinco zonas** (Z1 Providencia, Z2 Las Condes, Z3 Maipú, Z4 Santiago
Centro, Z5 Pudahuel) y los **cinco tipos de consulta** (Q1 conteo, Q2 área,
Q3 densidad, Q4 comparación, Q5 distribución de confianza) replican
literalmente la Sección 4 y 5 del enunciado, con los mismos formatos de
*cache key*.

---

## Despliegue paso a paso

### 1) Verificar el dataset

El repositorio incluye el subconjunto Open Buildings para las cinco zonas
en `data/santiago_buildings.parquet` (≈ 8.5 MB, 319 225 edificaciones).
Confirma que el archivo está presente:

```bash
ls -la data/santiago_buildings.parquet
```

### 2) Levantar el stack

```bash
docker compose up -d --build
```

Espera ≈ 60 s a que los healthchecks pasen y verifica:

```bash
curl http://localhost:8000/health
curl http://localhost:8001/health
curl http://localhost:8002/health
curl http://localhost:8003/health
```

Los cuatro deben retornar `{"status":"ok",...}`.

### 3) Probar consultas Q1–Q5 manualmente

Cada consulta se envía al **Cache Service** (puerto 8001). El primer hit
devuelve `MISS`; al repetirla se obtiene `HIT`.

```bash
# Q1 — conteo en Providencia con confidence_min=0.8
curl -X POST http://localhost:8001/query \
  -H "Content-Type: application/json" \
  -d '{"query_type":"Q1","params":{"zone_id":"Z1","confidence_min":0.8}}'

# Q2 — área media y total en Las Condes
curl -X POST http://localhost:8001/query \
  -H "Content-Type: application/json" \
  -d '{"query_type":"Q2","params":{"zone_id":"Z2","confidence_min":0.0}}'

# Q3 — densidad en Maipú
curl -X POST http://localhost:8001/query \
  -H "Content-Type: application/json" \
  -d '{"query_type":"Q3","params":{"zone_id":"Z3","confidence_min":0.0}}'

# Q4 — comparar densidad Las Condes vs Maipú
curl -X POST http://localhost:8001/query \
  -H "Content-Type: application/json" \
  -d '{"query_type":"Q4","params":{"zone_a":"Z2","zone_b":"Z3","confidence_min":0.0}}'

# Q5 — distribución de confianza en Pudahuel (5 bins)
curl -X POST http://localhost:8001/query \
  -H "Content-Type: application/json" \
  -d '{"query_type":"Q5","params":{"zone_id":"Z5","bins":5}}'
```

### 4) Test rápido del pipeline (≈ 1 min)

```bash
pip install numpy matplotlib
python experiments/master_run.py --suite demo
```

### 5) Batería oficial de experimentos (≈ 20 min)

```bash
python experiments/master_run.py --suite all
```

Esto ejecuta **18 experimentos oficiales** = 3 políticas (LRU, LFU, FIFO)
× 3 tamaños (50 MB, 200 MB, 500 MB) × 2 distribuciones (Zipf, Uniforme),
más 3 corridas adicionales en caché muy pequeño (forzar evicciones) y una
corrida larga de 180 s para evidenciar el efecto del TTL.

Snapshots resultantes en `results/snap_*.json`.

### 6) Generar las figuras del informe

```bash
python experiments/build_figures.py
```

Genera 7 figuras en `informe/figs/` (PDF y PNG):

| Figura | Contenido |
|---|---|
| fig1 | Hit rate por distribución y política |
| fig2 | Zipf vs Uniforme por política |
| fig3 | Hit rate por tamaño de caché |
| fig4 | Throughput por política y distribución |
| fig5 | Latencia hit vs miss (escala log) |
| fig6 | Hit rate por consulta Q1–Q5 |
| fig7 | Cache efficiency por política y duración |

### 7) Compilar el informe LaTeX

```bash
cd informe
pdflatex informe.tex
pdflatex informe.tex   # segunda pasada para referencias cruzadas
```

---

## Configuración (`.env`)

```ini
# Tamaño máximo de caché (50mb / 200mb / 500mb requeridos por enunciado)
REDIS_MAXMEMORY=200mb

# Política nativa Redis: allkeys-lru / allkeys-lfu / noeviction (FIFO)
REDIS_POLICY_NATIVE=allkeys-lru

REDIS_PORT_HOST=6379

# Política expuesta al cache_service: LRU, LFU o FIFO (debe ser consistente
# con REDIS_POLICY_NATIVE; FIFO se gestiona en el cliente con noeviction)
CACHE_POLICY=LRU

# TTL global por defecto (segundos). 0 = sin expiración.
CACHE_TTL_SEC=300

# TTL específicos por consulta
TTL_Q1=300
TTL_Q2=300
TTL_Q3=180
TTL_Q4=120
TTL_Q5=600

# Latencia simulada del Response Generator (cómputo geoespacial)
SIM_LATENCY_MIN_MS=30
SIM_LATENCY_MAX_MS=120
```

`experiments/master_run.py` reconfigura `maxmemory-policy` y `maxmemory` en
runtime con `docker compose exec redis redis-cli CONFIG SET`, evitando
reiniciar contenedores entre combinaciones.

---

## API HTTP

### Traffic Generator (8000)

| Método | Ruta | Descripción |
|---|---|---|
| POST | `/run` | Inicia un experimento |
| GET  | `/status` | Progreso del experimento actual |
| POST | `/stop` | Detiene experimento en curso |
| GET  | `/health` | Healthcheck |

Body de `/run`:

```json
{
  "distribution": "zipf",
  "rate_qps": 60,
  "duration_sec": 30,
  "zipf_s": 1.5,
  "concurrency": 16,
  "seed": 42,
  "label": "mi_experimento"
}
```

### Cache Service (8001)

| Método | Ruta | Descripción |
|---|---|---|
| POST | `/query` | Consulta Q1–Q5 (entrada del pipeline) |
| GET  | `/stats` | Stats agregados de Redis |
| POST | `/flush` | Limpia el caché |
| GET  | `/health` | Healthcheck |

### Response Generator (8002)

| Método | Ruta | Descripción |
|---|---|---|
| POST | `/query` | Ejecuta Q1–Q5 sobre datos en memoria |
| GET  | `/stats` | Edificaciones cargadas por zona |
| GET  | `/health` | Healthcheck |

### Metrics Service (8003)

| Método | Ruta | Descripción |
|---|---|---|
| POST | `/event` | Recibe eventos hit / miss / error |
| GET  | `/summary` | Hit rate, throughput, p50/p95, eviction rate, cache efficiency |
| GET  | `/summary/by_query` | Desglose por Q1–Q5 |
| POST | `/snapshot` | Persiste snapshot a disco |
| POST | `/reset` | Reinicia métricas |
| GET  | `/health` | Healthcheck |

---

## Troubleshooting

**`docker compose up` falla con "dataset no encontrado"**

Verifica que `data/santiago_buildings.parquet` existe en el repositorio
(≈ 8.5 MB). Si fue eliminado por error, recupéralo desde Git:

```bash
git checkout -- data/santiago_buildings.parquet
```

**El cache service reporta `OOM` o falla al iniciar**
Aumenta `REDIS_MAXMEMORY` en `.env` (mínimo recomendado `50mb`) y reinicia:

```bash
docker compose restart redis cache_service
```

**Cambiar política sin reiniciar todo el stack**

```bash
docker compose exec redis redis-cli CONFIG SET maxmemory-policy allkeys-lfu
docker compose exec redis redis-cli FLUSHDB
```

**Apagar y limpiar todo**

```bash
docker compose down -v
```

---

## Mapeo con el enunciado

| Requisito (PDF) | Implementación |
|---|---|
| 4 servicios independientes | `traffic_generator`, `cache_service`, `response_generator`, `metrics` |
| Caché Redis con TTL y evicción configurable | `redis:7.4` + `cache_service` (LRU/LFU nativos, FIFO en cliente) |
| Distribuciones Zipf y Uniforme | `traffic_generator/app/distributions.py` |
| Q1–Q5 sobre datos precargados | `response_generator/app/queries.py` |
| Cache keys exactos del enunciado | `cache_service/app/main.py:_build_cache_key` |
| Cinco zonas con bounding boxes | `response_generator/app/data_loader.py:ZONES` |
| Tamaños 50/200/500 MB | `experiments/master_run.py:SIZES` |
| Hit rate / throughput / p50/p95 / eviction rate / cache efficiency | `metrics_service/app/main.py:Metrics.summary` |
| Análisis comparativo y figuras | `experiments/build_figures.py` + `informe/informe.tex` |
| Despliegue Docker | `docker-compose.yml` (Compose v2) |

*Tarea 1 — Sistemas Distribuidos, UDP 2026-1*
