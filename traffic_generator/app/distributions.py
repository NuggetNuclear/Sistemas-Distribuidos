"""
Distribuciones para el Generador de Tráfico.

Dos ejes ortogonales:
  1. Tasa de arribo (inter-arrival time): exponencial pura (Poisson) con
     rate λ. Esto modela cuándo llegan las consultas.
  2. Selección del item consultado: Zipf (sesgada → favorece hits) o
     Uniforme (sin sesgo → estresa al cache).

El enunciado pide explícitamente comparar Zipf vs. Uniforme como
"distribuciones de tasa de arribo", pero conceptualmente Zipf no aplica
a tiempos sino a frecuencia de selección. Implementamos ambas
interpretaciones: la tasa siempre es Poisson, lo que cambia es CÓMO se
elige qué consultar.
"""
import numpy as np
from typing import Sequence


class ZipfSelector:
    """
    Selecciona items con frecuencia P(i) ∝ 1 / i^s.
    Genera ranking realista de "zonas hot" (centros urbanos consultados
    muchas veces) vs. "zonas cold".
    """

    def __init__(self, items: Sequence, s: float = 1.2, seed: int = 0):
        self.items = list(items)
        self.s = s
        self.rng = np.random.default_rng(seed)
        n = len(items)
        ranks = np.arange(1, n + 1)
        weights = 1.0 / np.power(ranks, s)
        self.probs = weights / weights.sum()

    def sample(self):
        idx = self.rng.choice(len(self.items), p=self.probs)
        return self.items[idx]

    def describe(self) -> dict:
        return {"distribution": "zipf", "s": self.s,
                "probs": [round(float(p), 4) for p in self.probs]}


class UniformSelector:
    """Selección uniforme — todos los items con igual probabilidad."""

    def __init__(self, items: Sequence, seed: int = 0):
        self.items = list(items)
        self.rng = np.random.default_rng(seed)

    def sample(self):
        idx = self.rng.integers(0, len(self.items))
        return self.items[idx]

    def describe(self) -> dict:
        return {"distribution": "uniform", "n": len(self.items)}


class PoissonInterArrival:
    """Tiempos entre arribos exponenciales con tasa λ (consultas/segundo)."""

    def __init__(self, rate_qps: float, seed: int = 0):
        self.rate = rate_qps
        self.rng = np.random.default_rng(seed)

    def next_wait(self) -> float:
        return float(self.rng.exponential(1.0 / self.rate))


def build_selector(kind: str, items: Sequence, **kwargs):
    kind = kind.lower()
    if kind == "zipf":
        return ZipfSelector(items, s=kwargs.get("s", 1.2), seed=kwargs.get("seed", 0))
    if kind == "uniform":
        return UniformSelector(items, seed=kwargs.get("seed", 0))
    raise ValueError(f"Distribución desconocida: {kind}")
