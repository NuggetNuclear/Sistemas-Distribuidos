"""
Cache Service — intercepta consultas, sirve hits desde Redis, delega misses
al Response Generator. Reporta cada evento al Metrics Service.

Endpoints:
    POST /query   — punto de entrada principal del pipeline
    GET  /stats   — stats agregados del cache
    POST /flush   — limpia el cache (útil para experimentos)
    GET  /health  — healthcheck
"""
import os
import time
import logging
import asyncio
from contextlib import asynccontextmanager
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from .cache import CacheClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [cache-svc] %(message)s")
log = logging.getLogger(__name__)

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
RESPONSE_GEN_URL = os.getenv("RESPONSE_GEN_URL", "http://response_generator:8002")
METRICS_URL = os.getenv("METRICS_URL", "http://metrics:8003")
TTL_BY_QUERY = {
    # TTLs específicos por tipo de consulta (segundos).
    # Q4 (compare) y Q3 (density) se beneficia de TTL más corto porque dependen de Q1.
    # Q5 (distribución) es más estable, TTL largo.
    "Q1": int(os.getenv("TTL_Q1", "300")),
    "Q2": int(os.getenv("TTL_Q2", "300")),
    "Q3": int(os.getenv("TTL_Q3", "180")),
    "Q4": int(os.getenv("TTL_Q4", "120")),
    "Q5": int(os.getenv("TTL_Q5", "600")),
}

cache: CacheClient | None = None
http: httpx.AsyncClient | None = None


def _build_cache_key(query_type: str, params: dict[str, Any]) -> str:
    """Cache keys según el formato exacto del enunciado (Sección 5)."""
    qt = query_type.upper()
    if qt == "Q1":
        return f"count:{params['zone_id']}:conf={params.get('confidence_min', 0.0):.2f}"
    if qt == "Q2":
        return f"area:{params['zone_id']}:conf={params.get('confidence_min', 0.0):.2f}"
    if qt == "Q3":
        return f"density:{params['zone_id']}:conf={params.get('confidence_min', 0.0):.2f}"
    if qt == "Q4":
        return (
            f"compare:density:{params['zone_a']}:{params['zone_b']}"
            f":conf={params.get('confidence_min', 0.0):.2f}"
        )
    if qt == "Q5":
        return f"confidence_dist:{params['zone_id']}:bins={int(params.get('bins', 5))}"
    raise ValueError(f"Query type desconocido: {query_type}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global cache, http
    cache = CacheClient(REDIS_HOST, REDIS_PORT)
    http = httpx.AsyncClient(timeout=30.0)
    log.info("Cache Service listo")
    yield
    await http.aclose()


app = FastAPI(title="Cache Service", lifespan=lifespan)


class QueryRequest(BaseModel):
    query_type: str
    params: dict[str, Any] = Field(default_factory=dict)
    client_id: str | None = None  # útil para trackear orígenes


async def _send_metric(event: dict):
    """Envía evento al servicio de métricas. Fire-and-forget."""
    try:
        await http.post(f"{METRICS_URL}/event", json=event, timeout=2.0)
    except Exception as e:
        # No bloqueamos el flujo principal por errores de métricas
        log.debug(f"Metrics post failed: {e}")


@app.get("/health")
async def health():
    return {"status": "ok", "policy": cache.policy if cache else None}


@app.get("/stats")
async def stats():
    if cache is None:
        raise HTTPException(503, "Cache no inicializado")
    return cache.stats()


@app.post("/flush")
async def flush():
    if cache is None:
        raise HTTPException(503, "Cache no inicializado")
    cache.flushall()
    return {"status": "flushed"}


@app.post("/query")
async def query(req: QueryRequest):
    """
    Flujo principal:
      1. Construye cache key
      2. Lookup en cache
      3a. Hit → devuelve, registra hit
      3b. Miss → llama a response_generator, almacena, registra miss
    """
    if cache is None or http is None:
        raise HTTPException(503, "Servicio no inicializado")

    t_total_start = time.perf_counter()

    try:
        key = _build_cache_key(req.query_type, req.params)
    except (KeyError, ValueError) as e:
        raise HTTPException(400, f"Parámetros inválidos: {e}")

    # 1. Lookup
    t_lookup_start = time.perf_counter()
    cached = cache.get(key)
    t_lookup_ms = (time.perf_counter() - t_lookup_start) * 1000

    if cached is not None:
        # CACHE HIT
        latency_ms = (time.perf_counter() - t_total_start) * 1000
        # Métricas async sin bloquear
        asyncio.create_task(_send_metric({
            "event": "hit",
            "query_type": req.query_type.upper(),
            "key": key,
            "latency_ms": latency_ms,
            "lookup_ms": t_lookup_ms,
            "ts": time.time(),
        }))
        return {
            "result": cached,
            "cache": "HIT",
            "latency_ms": latency_ms,
            "key": key,
        }

    # CACHE MISS — delegar al response generator
    try:
        resp = await http.post(
            f"{RESPONSE_GEN_URL}/query",
            json={"query_type": req.query_type, "params": req.params},
            timeout=10.0,
        )
        resp.raise_for_status()
        data = resp.json()
    except httpx.HTTPError as e:
        # Registrar el error como evento (útil para Entrega 2 con fallback)
        asyncio.create_task(_send_metric({
            "event": "error",
            "query_type": req.query_type.upper(),
            "key": key,
            "error": str(e),
            "ts": time.time(),
        }))
        raise HTTPException(502, f"Response generator falló: {e}")

    result = data["result"]
    compute_ms = data["compute_time_ms"]

    # Almacenar en cache con TTL específico por tipo de consulta
    ttl = TTL_BY_QUERY.get(req.query_type.upper(), 300)
    cache.set(key, result, ttl=ttl)

    latency_ms = (time.perf_counter() - t_total_start) * 1000
    asyncio.create_task(_send_metric({
        "event": "miss",
        "query_type": req.query_type.upper(),
        "key": key,
        "latency_ms": latency_ms,
        "lookup_ms": t_lookup_ms,
        "compute_ms": compute_ms,
        "ttl": ttl,
        "ts": time.time(),
    }))

    return {
        "result": result,
        "cache": "MISS",
        "latency_ms": latency_ms,
        "compute_ms": compute_ms,
        "key": key,
        "ttl": ttl,
    }
