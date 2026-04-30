"""
Implementación de las consultas Q1-Q5 sobre datos en memoria.

Todas las consultas operan sobre arrays numpy/pandas para vectorización.
Se inyecta una latencia simulada (sleep aleatorio) para representar el
costo real de un cómputo geoespacial sobre datos no-cacheados; este es
el punto donde una caché aporta valor.
"""
import time
import random
import numpy as np
from typing import Any

from .data_loader import DataStore

# Latencia simulada de cómputo (en segundos). Configurable vía env vars.
import os
SIM_LATENCY_MIN = float(os.getenv("SIM_LATENCY_MIN_MS", "30")) / 1000
SIM_LATENCY_MAX = float(os.getenv("SIM_LATENCY_MAX_MS", "120")) / 1000


def _simulate_compute_latency():
    """Inyecta latencia para simular procesamiento geoespacial pesado."""
    if SIM_LATENCY_MAX > 0:
        time.sleep(random.uniform(SIM_LATENCY_MIN, SIM_LATENCY_MAX))


def q1_count(store: DataStore, zone_id: str, confidence_min: float = 0.0) -> dict[str, Any]:
    """Q1: Conteo de edificios en una zona con filtro de confianza."""
    _simulate_compute_latency()
    df = store.get_zone(zone_id)
    if confidence_min <= 0:
        n = int(len(df))
    else:
        n = int((df["confidence"].values >= confidence_min).sum())
    return {"query": "Q1", "zone_id": zone_id, "confidence_min": confidence_min, "count": n}


def q2_area(store: DataStore, zone_id: str, confidence_min: float = 0.0) -> dict[str, Any]:
    """Q2: Área promedio y total."""
    _simulate_compute_latency()
    df = store.get_zone(zone_id)
    if confidence_min > 0:
        mask = df["confidence"].values >= confidence_min
        areas = df["area_in_meters"].values[mask]
    else:
        areas = df["area_in_meters"].values
    n = int(len(areas))
    if n == 0:
        return {"query": "Q2", "zone_id": zone_id, "confidence_min": confidence_min,
                "avg_area": 0.0, "total_area": 0.0, "n": 0}
    return {
        "query": "Q2", "zone_id": zone_id, "confidence_min": confidence_min,
        "avg_area": float(np.mean(areas)),
        "total_area": float(np.sum(areas)),
        "n": n,
    }


def q3_density(store: DataStore, zone_id: str, confidence_min: float = 0.0) -> dict[str, Any]:
    """Q3: Densidad de edificios por km²."""
    _simulate_compute_latency()
    df = store.get_zone(zone_id)
    if confidence_min > 0:
        n = int((df["confidence"].values >= confidence_min).sum())
    else:
        n = int(len(df))
    area_km2 = store.zone_area_km2(zone_id)
    density = n / area_km2 if area_km2 > 0 else 0.0
    return {
        "query": "Q3", "zone_id": zone_id, "confidence_min": confidence_min,
        "count": n, "area_km2": float(area_km2), "density_per_km2": float(density),
    }


def q4_compare(store: DataStore, zone_a: str, zone_b: str,
               confidence_min: float = 0.0) -> dict[str, Any]:
    """Q4: Compara densidad entre dos zonas."""
    # Una sola latencia simulada — son dos sub-cálculos pero se reportan juntos.
    _simulate_compute_latency()
    da_full = q3_density(store, zone_a, confidence_min)
    db_full = q3_density(store, zone_b, confidence_min)
    winner = zone_a if da_full["density_per_km2"] > db_full["density_per_km2"] else zone_b
    return {
        "query": "Q4", "zone_a": zone_a, "zone_b": zone_b,
        "confidence_min": confidence_min,
        "density_a": da_full["density_per_km2"],
        "density_b": db_full["density_per_km2"],
        "winner": winner,
    }


def q5_confidence_dist(store: DataStore, zone_id: str, bins: int = 5) -> dict[str, Any]:
    """Q5: Distribución de score de confianza en intervalos."""
    _simulate_compute_latency()
    df = store.get_zone(zone_id)
    scores = df["confidence"].values
    counts, edges = np.histogram(scores, bins=bins, range=(0.0, 1.0))
    buckets = [
        {"bucket": int(i), "min": float(edges[i]), "max": float(edges[i + 1]),
         "count": int(counts[i])}
        for i in range(bins)
    ]
    return {"query": "Q5", "zone_id": zone_id, "bins": int(bins), "buckets": buckets}


# --- Routing por tipo de consulta ---

def execute_query(store: DataStore, query_type: str,
                  params: dict[str, Any]) -> dict[str, Any]:
    """Ejecuta la consulta correspondiente. Lanza ValueError si tipo desconocido."""
    qt = query_type.upper()
    if qt == "Q1":
        return q1_count(store, params["zone_id"], params.get("confidence_min", 0.0))
    if qt == "Q2":
        return q2_area(store, params["zone_id"], params.get("confidence_min", 0.0))
    if qt == "Q3":
        return q3_density(store, params["zone_id"], params.get("confidence_min", 0.0))
    if qt == "Q4":
        return q4_compare(store, params["zone_a"], params["zone_b"],
                          params.get("confidence_min", 0.0))
    if qt == "Q5":
        return q5_confidence_dist(store, params["zone_id"], int(params.get("bins", 5)))
    raise ValueError(f"Query type desconocido: {query_type}")


def build_cache_key(query_type: str, params: dict[str, Any]) -> str:
    """
    Construye la cache key según el formato exacto del enunciado (Sección 5).
    Provista para consistencia; el cache_service usa su propia implementación.
    """
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
