"""
Generador de Tráfico — emite consultas Q1-Q5 al Cache Service.

Comportamiento:
  - Selecciona zona, tipo de consulta y parámetros usando las distribuciones
    configuradas (Zipf o Uniforme).
  - Inter-arrival time exponencial (tasa Poisson configurable).
  - Emite consultas en paralelo con un pool de workers asíncronos.
  - Expone una API HTTP para iniciar/detener experimentos y reportar progreso.

Endpoints:
    POST /run     — inicia un experimento con configuración dada
    GET  /status  — estado actual
    POST /stop    — detiene experimento en curso
    GET  /health
"""
import os
import asyncio
import time
import logging
import random
import itertools
from contextlib import asynccontextmanager
from typing import Any, Optional

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from .distributions import build_selector, PoissonInterArrival

logging.basicConfig(level=logging.INFO, format="%(asctime)s [traffic-gen] %(message)s")
log = logging.getLogger(__name__)

CACHE_URL = os.getenv("CACHE_URL", "http://cache_service:8001")

# Las 5 zonas
ZONE_IDS = ["Z1", "Z2", "Z3", "Z4", "Z5"]
QUERY_TYPES = ["Q1", "Q2", "Q3", "Q4", "Q5"]
# Discretizamos confidence_min con suficiente granularidad para que cache
# size sea relevante. 21 niveles en [0, 1] con paso 0.05.
# Keyspace teórico: 5 zonas * 5 queries * 21 niveles ~ 525 + pairs Q4 + bins Q5
# = ~600+ keys distintas, suficiente para que 50MB << 500MB sea distinguible.
CONF_LEVELS = [round(i * 0.05, 2) for i in range(0, 21)]  # 0.0, 0.05, ..., 1.0
BIN_LEVELS = [3, 4, 5, 6, 8, 10, 12, 15, 20]

# Estado del experimento (singleton; un experimento a la vez)
class ExperimentState:
    def __init__(self):
        self.running = False
        self.config: dict = {}
        self.start_time: float = 0.0
        self.sent: int = 0
        self.errors: int = 0
        self.task: Optional[asyncio.Task] = None
        self.stop_flag = asyncio.Event()
        self.last_results: list[dict] = []  # rolling window

    def reset(self):
        self.running = False
        self.config = {}
        self.start_time = 0.0
        self.sent = 0
        self.errors = 0
        self.stop_flag = asyncio.Event()
        self.last_results = []


state = ExperimentState()
http: httpx.AsyncClient | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global http
    http = httpx.AsyncClient(timeout=30.0, limits=httpx.Limits(max_connections=200))
    log.info("Traffic Generator listo")
    yield
    if state.task:
        state.stop_flag.set()
        try:
            await state.task
        except Exception:
            pass
    await http.aclose()


app = FastAPI(title="Traffic Generator", lifespan=lifespan)


class RunRequest(BaseModel):
    """Configuración del experimento.

    distribution: 'zipf' o 'uniform' (selección de zona/consulta)
    rate_qps: tasa de arribo (Poisson, consultas por segundo)
    duration_sec: duración total del experimento
    n_queries: alternativa a duration; cantidad fija de consultas
    zipf_s: parámetro s de Zipf (default 1.2)
    concurrency: workers paralelos enviando requests
    seed: semilla para reproducibilidad
    """
    distribution: str = Field("zipf", pattern="^(zipf|uniform)$")
    rate_qps: float = Field(50.0, gt=0)
    duration_sec: float | None = None
    n_queries: int | None = None
    zipf_s: float = 1.2
    concurrency: int = 16
    seed: int = 42
    label: str = "exp"  # etiqueta legible para el experimento


def _build_query(zone_selector, query_selector, conf_selector, bin_selector,
                 rng: random.Random) -> dict:
    """Construye un payload de consulta sintética."""
    qt = query_selector.sample()
    if qt == "Q4":
        # Q4 necesita dos zonas distintas
        za = zone_selector.sample()
        zb = zone_selector.sample()
        attempts = 0
        while zb == za and attempts < 5:
            zb = zone_selector.sample()
            attempts += 1
        if zb == za:
            # fallback determinístico
            others = [z for z in ZONE_IDS if z != za]
            zb = rng.choice(others)
        return {
            "query_type": "Q4",
            "params": {
                "zone_a": za,
                "zone_b": zb,
                "confidence_min": conf_selector.sample(),
            },
        }
    if qt == "Q5":
        return {
            "query_type": "Q5",
            "params": {
                "zone_id": zone_selector.sample(),
                "bins": bin_selector.sample(),
            },
        }
    # Q1, Q2, Q3
    return {
        "query_type": qt,
        "params": {
            "zone_id": zone_selector.sample(),
            "confidence_min": conf_selector.sample(),
        },
    }


async def _send_one(query: dict) -> dict:
    """Envía una consulta al cache y retorna el resultado."""
    try:
        resp = await http.post(f"{CACHE_URL}/query", json=query, timeout=15.0)
        resp.raise_for_status()
        return resp.json()
    except httpx.HTTPError as e:
        return {"error": str(e)}


async def _worker(queue: asyncio.Queue):
    """Worker que consume queries de la cola y las dispara."""
    while True:
        try:
            q = await queue.get()
        except asyncio.CancelledError:
            return
        if q is None:
            queue.task_done()
            return
        result = await _send_one(q)
        if "error" in result:
            state.errors += 1
        else:
            state.sent += 1
            # Mantener una ventana de últimos resultados (max 1000)
            state.last_results.append({
                "query_type": q["query_type"],
                "cache": result.get("cache"),
                "latency_ms": result.get("latency_ms"),
            })
            if len(state.last_results) > 1000:
                state.last_results = state.last_results[-1000:]
        queue.task_done()


async def _run_experiment(cfg: RunRequest):
    """
    Bucle principal: programa consultas según inter-arrival exponencial
    y las pone en una cola consumida por workers concurrentes.
    """
    log.info(f"Iniciando experimento: {cfg.dict()}")
    state.running = True
    state.start_time = time.time()
    state.sent = 0
    state.errors = 0
    state.config = cfg.dict()
    state.last_results = []

    rng = random.Random(cfg.seed)

    # Selectores
    # Zonas tienen orden de "popularidad" predefinido para Zipf:
    # los centros urbanos van primero (Providencia, Santiago Centro, Las Condes)
    zone_order = ["Z1", "Z4", "Z2", "Z3", "Z5"]
    query_order = ["Q1", "Q3", "Q2", "Q5", "Q4"]  # Q1 más común, Q4 menos

    zone_sel = build_selector(cfg.distribution, zone_order, s=cfg.zipf_s, seed=cfg.seed)
    query_sel = build_selector(cfg.distribution, query_order, s=cfg.zipf_s, seed=cfg.seed + 1)
    conf_sel = build_selector(cfg.distribution, CONF_LEVELS, s=cfg.zipf_s, seed=cfg.seed + 2)
    bin_sel = build_selector(cfg.distribution, BIN_LEVELS, s=cfg.zipf_s, seed=cfg.seed + 3)

    arrival = PoissonInterArrival(cfg.rate_qps, seed=cfg.seed + 4)

    queue: asyncio.Queue = asyncio.Queue(maxsize=cfg.concurrency * 4)
    workers = [asyncio.create_task(_worker(queue)) for _ in range(cfg.concurrency)]

    deadline = None if cfg.duration_sec is None else state.start_time + cfg.duration_sec
    target_count = cfg.n_queries

    produced = 0
    try:
        while not state.stop_flag.is_set():
            now = time.time()
            if deadline is not None and now >= deadline:
                break
            if target_count is not None and produced >= target_count:
                break

            q = _build_query(zone_sel, query_sel, conf_sel, bin_sel, rng)
            await queue.put(q)
            produced += 1

            wait = arrival.next_wait()
            try:
                await asyncio.wait_for(state.stop_flag.wait(), timeout=wait)
                break  # stop solicitado
            except asyncio.TimeoutError:
                pass  # esperó normalmente

        log.info(f"Producción terminada. Esperando {queue.qsize()} en cola...")
        # Drenar cola
        await queue.join()
    finally:
        # Apagar workers
        for _ in workers:
            await queue.put(None)
        await asyncio.gather(*workers, return_exceptions=True)
        elapsed = time.time() - state.start_time
        log.info(
            f"Experimento '{cfg.label}' terminado. Sent={state.sent} "
            f"Errors={state.errors} Elapsed={elapsed:.1f}s "
            f"Throughput={state.sent / max(elapsed, 0.01):.1f} qps"
        )
        state.running = False


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/status")
async def status():
    elapsed = time.time() - state.start_time if state.running else 0
    # Calcular hit rate de la ventana reciente
    recent = state.last_results[-200:]
    hits = sum(1 for r in recent if r.get("cache") == "HIT")
    n = len(recent)
    hit_rate = (hits / n) if n > 0 else None
    return {
        "running": state.running,
        "config": state.config,
        "sent": state.sent,
        "errors": state.errors,
        "elapsed_sec": round(elapsed, 2),
        "throughput_qps": round(state.sent / max(elapsed, 0.01), 2) if elapsed > 0 else None,
        "hit_rate_window": round(hit_rate, 4) if hit_rate is not None else None,
        "window_size": n,
    }


@app.post("/run")
async def run(req: RunRequest):
    if state.running:
        raise HTTPException(409, "Ya hay un experimento corriendo. Llama /stop primero.")
    if req.duration_sec is None and req.n_queries is None:
        raise HTTPException(400, "Debes especificar duration_sec o n_queries")
    state.reset()
    state.task = asyncio.create_task(_run_experiment(req))
    return {"status": "started", "config": req.dict()}


@app.post("/stop")
async def stop():
    if not state.running:
        return {"status": "not_running"}
    state.stop_flag.set()
    if state.task:
        try:
            await asyncio.wait_for(state.task, timeout=20.0)
        except asyncio.TimeoutError:
            log.warning("Timeout esperando a que termine el experimento")
    return {"status": "stopped", "sent": state.sent, "errors": state.errors}
