#  ============= dependencias del proyecto ============= #
import numpy as np
from typing import Sequence


#  ============= selector de consultas con distribucion zipf ============= #
class ZipfSelector:

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
        return {"distribution": "zipf", "s": self.s, "probs": [round(float(p), 4) for p in self.probs]}


#  ============= selector de consultas con distribucion uniforme ============= #
class UniformSelector:

    def __init__(self, items: Sequence, seed: int = 0):
        self.items = list(items)
        self.rng = np.random.default_rng(seed)

    def sample(self):
        idx = self.rng.integers(0, len(self.items))
        return self.items[idx]

    def describe(self) -> dict:
        return {"distribution": "uniform", "n": len(self.items)}


#  ============= modelo de llegadas poisson ============= #
class PoissonInterArrival:

    def __init__(self, rate_qps: float, seed: int = 0):
        self.rate = rate_qps
        self.rng = np.random.default_rng(seed)

    def next_wait(self) -> float:
        return float(self.rng.exponential(1.0 / self.rate))


#  ============= selectores de distribucion ============= #
def build_selector(kind: str, items: Sequence, **kwargs):
    kind = kind.lower()
    if kind == "zipf":
        return ZipfSelector(items, s=kwargs.get("s", 1.2), seed=kwargs.get("seed", 0))
    if kind == "uniform":
        return UniformSelector(items, seed=kwargs.get("seed", 0))
    raise ValueError(f"Distribución desconocida: {kind}")
