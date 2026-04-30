"""
Genera gráficos comparativos a partir de los snapshots de results/.

Produce figuras para el informe:
  - hit_rate por (política, tamaño, distribución)
  - latencia p50/p95 hit vs miss
  - throughput por configuración
  - eviction rate
  - desglose por tipo de consulta

Uso: python experiments/analyze_results.py
"""
import json
import re
from pathlib import Path
import matplotlib.pyplot as plt
import numpy as np

RESULTS_DIR = Path(__file__).parent.parent / "results"
FIG_DIR = Path(__file__).parent.parent / "informe" / "figs"
FIG_DIR.mkdir(parents=True, exist_ok=True)


def load_results():
    """Carga todos los snapshots y los parsea por etiqueta."""
    runs = []
    for f in sorted(RESULTS_DIR.glob("*.json")):
        with open(f) as fh:
            data = json.load(fh)
        label = data.get("label", f.stem)
        # Parsear etiqueta esperada: POLICY_SIZE_DIST  (ej: LRU_200mb_zipf)
        m = re.match(r"(LRU|LFU|FIFO)_(\d+mb)_(zipf|uniform)", label)
        meta = {}
        if m:
            meta["policy"] = m.group(1)
            meta["size"] = m.group(2)
            meta["distribution"] = m.group(3)
        meta["label"] = label
        meta["data"] = data
        runs.append(meta)
    return runs


def fig_hit_rate_by_policy(runs):
    """Hit rate × política, separado por tamaño y distribución."""
    sizes = sorted({r.get("size") for r in runs if r.get("size")},
                   key=lambda s: int(s.replace("mb", "")))
    policies = ["LRU", "LFU", "FIFO"]
    distributions = ["zipf", "uniform"]

    fig, axes = plt.subplots(1, 2, figsize=(12, 5), sharey=True)
    for ax, dist in zip(axes, distributions):
        x = np.arange(len(sizes))
        width = 0.25
        for i, pol in enumerate(policies):
            ys = []
            for sz in sizes:
                match = [r for r in runs if r.get("policy") == pol
                         and r.get("size") == sz and r.get("distribution") == dist]
                if match:
                    ys.append(match[0]["data"].get("hit_rate") or 0)
                else:
                    ys.append(0)
            ax.bar(x + i * width, ys, width, label=pol)
        ax.set_xticks(x + width)
        ax.set_xticklabels(sizes)
        ax.set_xlabel("Tamaño cache")
        ax.set_title(f"Distribución: {dist}")
        ax.set_ylim(0, 1)
        ax.grid(axis="y", alpha=0.3)
        if dist == "zipf":
            ax.set_ylabel("Hit rate")
        ax.legend()
    fig.suptitle("Hit rate por política y tamaño")
    fig.tight_layout()
    out = FIG_DIR / "hit_rate_by_policy.pdf"
    fig.savefig(out)
    print(f"[fig] {out}")


def fig_latency_p95(runs):
    """Latencia p95 hit vs miss por política."""
    policies = ["LRU", "LFU", "FIFO"]
    distributions = ["zipf", "uniform"]
    # Tomar tamaño 200mb como el de referencia
    target_size = "200mb"

    fig, axes = plt.subplots(1, 2, figsize=(12, 5), sharey=True)
    for ax, dist in zip(axes, distributions):
        p95_hits = []
        p95_miss = []
        for pol in policies:
            match = [r for r in runs if r.get("policy") == pol
                     and r.get("size") == target_size and r.get("distribution") == dist]
            if match:
                d = match[0]["data"]
                p95_hits.append((d.get("latency_ms_hit") or {}).get("p95") or 0)
                p95_miss.append((d.get("latency_ms_miss") or {}).get("p95") or 0)
            else:
                p95_hits.append(0)
                p95_miss.append(0)
        x = np.arange(len(policies))
        ax.bar(x - 0.2, p95_hits, 0.4, label="p95 hit")
        ax.bar(x + 0.2, p95_miss, 0.4, label="p95 miss")
        ax.set_xticks(x)
        ax.set_xticklabels(policies)
        ax.set_xlabel("Política")
        ax.set_title(f"Distribución: {dist}")
        if dist == "zipf":
            ax.set_ylabel("Latencia p95 (ms)")
        ax.legend()
        ax.grid(axis="y", alpha=0.3)
    fig.suptitle(f"Latencia p95 hit vs miss (cache={target_size})")
    fig.tight_layout()
    out = FIG_DIR / "latency_p95.pdf"
    fig.savefig(out)
    print(f"[fig] {out}")


def fig_throughput(runs):
    """Throughput por política × distribución."""
    policies = ["LRU", "LFU", "FIFO"]
    distributions = ["zipf", "uniform"]
    target_size = "200mb"

    fig, ax = plt.subplots(figsize=(8, 5))
    x = np.arange(len(policies))
    width = 0.35
    for i, dist in enumerate(distributions):
        ys = []
        for pol in policies:
            match = [r for r in runs if r.get("policy") == pol
                     and r.get("size") == target_size and r.get("distribution") == dist]
            ys.append(match[0]["data"].get("throughput_qps_total") or 0 if match else 0)
        ax.bar(x + (i - 0.5) * width, ys, width, label=dist)
    ax.set_xticks(x)
    ax.set_xticklabels(policies)
    ax.set_xlabel("Política")
    ax.set_ylabel("Throughput (qps)")
    ax.set_title(f"Throughput total (cache={target_size})")
    ax.legend()
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    out = FIG_DIR / "throughput.pdf"
    fig.savefig(out)
    print(f"[fig] {out}")


def fig_hit_rate_by_query(runs):
    """Hit rate desglosado por tipo de consulta (Q1-Q5) en LRU/200mb/zipf."""
    target = [r for r in runs if r.get("policy") == "LRU"
              and r.get("size") == "200mb"]
    if not target:
        print("[fig] No hay datos LRU/200mb para desglose por consulta")
        return
    fig, ax = plt.subplots(figsize=(10, 5))
    x = np.arange(5)
    width = 0.4
    for i, dist in enumerate(["zipf", "uniform"]):
        match = [r for r in target if r.get("distribution") == dist]
        if not match:
            continue
        bq = match[0]["data"].get("by_query", {})
        ys = [(bq.get(f"Q{j+1}") or {}).get("hit_rate") or 0 for j in range(5)]
        ax.bar(x + (i - 0.5) * width, ys, width, label=dist)
    ax.set_xticks(x)
    ax.set_xticklabels([f"Q{i+1}" for i in range(5)])
    ax.set_ylabel("Hit rate")
    ax.set_title("Hit rate por tipo de consulta (política=LRU, cache=200mb)")
    ax.legend()
    ax.set_ylim(0, 1)
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    out = FIG_DIR / "hit_rate_by_query.pdf"
    fig.savefig(out)
    print(f"[fig] {out}")


def print_summary_table(runs):
    """Tabla CSV-like impresa al stdout para copiar al informe."""
    print("\n=== Tabla resumen ===")
    print(f"{'policy':6} {'size':6} {'dist':8} {'hit_rate':>9} "
          f"{'thr_qps':>9} {'p50_hit':>9} {'p95_hit':>9} {'p95_miss':>9}")
    for r in runs:
        d = r["data"]
        print(f"{r.get('policy','-'):6} {r.get('size','-'):6} "
              f"{r.get('distribution','-'):8} "
              f"{d.get('hit_rate', 0) or 0:>9.4f} "
              f"{d.get('throughput_qps_total', 0) or 0:>9.2f} "
              f"{(d.get('latency_ms_hit') or {}).get('p50', 0) or 0:>9.2f} "
              f"{(d.get('latency_ms_hit') or {}).get('p95', 0) or 0:>9.2f} "
              f"{(d.get('latency_ms_miss') or {}).get('p95', 0) or 0:>9.2f}")


def main():
    runs = load_results()
    if not runs:
        print(f"No hay snapshots en {RESULTS_DIR}. Corre experimentos primero.")
        return
    print(f"[ana] {len(runs)} runs cargados.")
    fig_hit_rate_by_policy(runs)
    fig_latency_p95(runs)
    fig_throughput(runs)
    fig_hit_rate_by_query(runs)
    print_summary_table(runs)
    print("\n[ana] Listo. Figuras en informe/figs/")


if __name__ == "__main__":
    main()
