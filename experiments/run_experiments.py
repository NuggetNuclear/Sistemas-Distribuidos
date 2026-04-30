"""
Runner de experimentos.

Ejecuta toda la batería de pruebas requerida por el enunciado:
  1. Distribución de tráfico: Zipf vs. Uniforme (mismo cache, mismo tamaño)
  2. Política de evicción: LRU vs. LFU vs. FIFO
  3. Tamaño de cache: 50MB vs. 200MB vs. 500MB
  4. TTL: variando TTLs por consulta

Para los casos (2) y (3) NECESITAS reiniciar los contenedores con la
config correspondiente. Este script genera los .env y comandos. Para
correr todo de corrido, usar: bash scripts/run_all_experiments.sh

Para experimentos individuales sobre un mismo deploy (ej. variando
distribución), basta este script.
"""
import argparse
import json
import time
from pathlib import Path
import urllib.request
import urllib.error

DEFAULT_TRAFFIC = "http://localhost:8000"
DEFAULT_METRICS = "http://localhost:8003"
DEFAULT_CACHE = "http://localhost:8001"

RESULTS_DIR = Path(__file__).parent.parent / "results"
RESULTS_DIR.mkdir(exist_ok=True)


def http_post(url: str, body: dict | None = None, timeout: int = 30) -> dict:
    data = json.dumps(body or {}).encode("utf-8")
    req = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/json"}, method="POST"
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def http_get(url: str, timeout: int = 10) -> dict:
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def wait_for_traffic_to_finish(traffic_url: str, poll_sec: float = 2.0, max_wait: int = 600):
    """Bloquea hasta que el traffic generator reporte running=False."""
    deadline = time.time() + max_wait
    while time.time() < deadline:
        try:
            s = http_get(f"{traffic_url}/status")
            if not s.get("running"):
                return s
        except Exception as e:
            print(f"[wait] error consultando status: {e}")
        time.sleep(poll_sec)
    raise TimeoutError("Experimento no terminó en max_wait")


def run_single_experiment(
    label: str,
    distribution: str,
    rate_qps: float,
    duration_sec: float,
    zipf_s: float = 1.2,
    concurrency: int = 16,
    seed: int = 42,
    flush_cache: bool = True,
    traffic_url: str = DEFAULT_TRAFFIC,
    metrics_url: str = DEFAULT_METRICS,
    cache_url: str = DEFAULT_CACHE,
    extra: dict | None = None,
) -> dict:
    """Ejecuta UN experimento, lo persiste a snapshot y retorna el resumen."""
    print(f"\n{'='*70}\n[exp] {label} | dist={distribution} qps={rate_qps} dur={duration_sec}s")
    print(f"{'='*70}")

    if flush_cache:
        print("[exp] Flushing cache...")
        http_post(f"{cache_url}/flush")
    print("[exp] Reset metrics...")
    http_post(f"{metrics_url}/reset")

    cfg = {
        "distribution": distribution,
        "rate_qps": rate_qps,
        "duration_sec": duration_sec,
        "zipf_s": zipf_s,
        "concurrency": concurrency,
        "seed": seed,
        "label": label,
    }
    print(f"[exp] Lanzando traffic con cfg={cfg}")
    http_post(f"{traffic_url}/run", cfg)

    print("[exp] Esperando que termine...")
    final_status = wait_for_traffic_to_finish(traffic_url, max_wait=int(duration_sec * 3 + 60))
    print(f"[exp] Traffic terminado: sent={final_status.get('sent')} errors={final_status.get('errors')}")

    # Pequeña pausa para que las métricas terminen de drenar
    time.sleep(2.0)

    snap_body = {"label": label, "extra": {**cfg, **(extra or {})}}
    snap = http_post(f"{metrics_url}/snapshot", snap_body)
    summary = snap["summary"]

    # Guardar también localmente
    out_file = RESULTS_DIR / f"{int(time.time())}_{label}.json"
    with open(out_file, "w") as f:
        json.dump(summary, f, indent=2, default=str)
    print(f"[exp] Resultado guardado en {out_file}")

    print(f"[exp] hit_rate={summary.get('hit_rate')} "
          f"throughput={summary.get('throughput_qps_total')} "
          f"p50_all={summary.get('latency_ms_all', {}).get('p50')} "
          f"p95_all={summary.get('latency_ms_all', {}).get('p95')}")
    return summary


# --- Suites predefinidas ---

def suite_distribution_compare(args):
    """Compara Zipf vs. Uniforme con el deploy actual."""
    results = []
    for dist, s in [("zipf", 1.2), ("zipf", 2.0), ("uniform", None)]:
        label = f"dist_{dist}" + (f"_s{s}" if dist == "zipf" else "")
        kwargs = {"zipf_s": s} if dist == "zipf" else {}
        r = run_single_experiment(
            label=label,
            distribution=dist,
            rate_qps=args.rate,
            duration_sec=args.duration,
            concurrency=args.concurrency,
            seed=args.seed,
            **kwargs,
        )
        results.append({"label": label, "summary": r})
    print("\n--- RESUMEN suite_distribution ---")
    for r in results:
        s = r["summary"]
        print(f"  {r['label']}: hit_rate={s.get('hit_rate')} "
              f"thr={s.get('throughput_qps_total')} "
              f"p95_hit={s.get('latency_ms_hit', {}).get('p95')} "
              f"p95_miss={s.get('latency_ms_miss', {}).get('p95')}")


def suite_ttl_compare(args):
    """
    NOTE: TTL se configura en deploy time vía env vars, no se puede cambiar
    en caliente. Este suite es informativo: ejecuta el mismo experimento
    para que se pueda comparar contra otros deploys con diferente TTL.
    """
    label = f"ttl_default_{args.distribution}"
    run_single_experiment(
        label=label,
        distribution=args.distribution,
        rate_qps=args.rate,
        duration_sec=args.duration,
        concurrency=args.concurrency,
        seed=args.seed,
    )


def suite_baseline(args):
    """Un solo experimento con la config actual."""
    run_single_experiment(
        label=args.label,
        distribution=args.distribution,
        rate_qps=args.rate,
        duration_sec=args.duration,
        concurrency=args.concurrency,
        seed=args.seed,
    )


def main():
    parser = argparse.ArgumentParser(description="Experiment runner")
    parser.add_argument("--suite", choices=["baseline", "distribution", "ttl"], default="baseline")
    parser.add_argument("--label", default="exp1")
    parser.add_argument("--distribution", choices=["zipf", "uniform"], default="zipf")
    parser.add_argument("--rate", type=float, default=80.0, help="QPS objetivo")
    parser.add_argument("--duration", type=float, default=60.0, help="Duración en segundos")
    parser.add_argument("--concurrency", type=int, default=16)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    if args.suite == "baseline":
        suite_baseline(args)
    elif args.suite == "distribution":
        suite_distribution_compare(args)
    elif args.suite == "ttl":
        suite_ttl_compare(args)


if __name__ == "__main__":
    main()
