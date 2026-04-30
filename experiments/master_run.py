# ================== dependencias del proyecto ===================== #
import argparse
import json
import os
import time
import subprocess
import urllib.request
import urllib.error
from pathlib import Path

# ================== configuraciones globales ===================== #
TRAFFIC  = os.getenv("TRAFFIC_URL",  "http://localhost:8000")
CACHE    = os.getenv("CACHE_URL",    "http://localhost:8001")
METRICS  = os.getenv("METRICS_URL",  "http://localhost:8003")
REDIS_PORT = os.getenv("REDIS_PORT", "6379")
RESULTS  = Path(__file__).parent.parent / "results"

POLICIES = ["LRU", "LFU", "FIFO"]
SIZES    = ["50mb", "200mb", "500mb"]
SIZE_MAP = {"50mb": 50*1024*1024, "200mb": 200*1024*1024, "500mb": 500*1024*1024}
DISTS    = ["zipf", "uniform"]
DURATION = 25
RATE     = 60
ZIPF_S   = 1.5


# ================== helpers HTTP ===================== #

def post(url, body=None, timeout=60):
    data = json.dumps(body or {}).encode()
    req  = urllib.request.Request(
        url, data=data,
        headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def get(url, timeout=10):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.loads(r.read())


def redis(*args):
    """Ejecuta redis-cli dentro del contenedor redis vía docker compose exec.
    Evita requerir redis-cli en el host."""
    cmd = ["docker", "compose", "exec", "-T", "redis", "redis-cli"] + list(args)
    res = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
    return res.stdout.strip()


# ================== manejo servicios de cache ===================== #

def reconfigure_cache(policy: str, size_bytes: int):
    """Cambia política y tamaño de Redis en runtime y hace flush."""
    # politica nativa
    native = {"LRU": "allkeys-lru", "LFU": "allkeys-lfu",
              "FIFO": "noeviction"}[policy]
    redis("CONFIG", "SET", "maxmemory-policy", native)
    redis("CONFIG", "SET", "maxmemory", str(size_bytes))
    redis("FLUSHDB")
    redis("SET", "__fifo_evictions__", "0")

    try:
        post(f"{CACHE}/reconfigure",
             {"policy": policy}, timeout=5)
    except Exception:
        pass

    print(f"    Redis: policy={native} maxmemory={size_bytes//1024}KB",
          flush=True)

def wait_for_services(retries=60, interval=1.0):
    for svc, url in [("traffic", TRAFFIC), ("cache", CACHE),
                     ("metrics", METRICS)]:
        print(f"  Esperando {svc}...", end="", flush=True)
        for _ in range(retries):
            try:
                if get(f"{url}/health", timeout=2)["status"] == "ok":
                    print(" OK", flush=True)
                    break
            except Exception:
                pass
            time.sleep(interval)
        else:
            raise RuntimeError(f"{svc} no respondió en {retries}s")


# ================== ejecucion de expperimentos ===================== #

def run_exp(label, distribution, duration=DURATION, rate=RATE,
            zipf_s=ZIPF_S, extra=None):
    print(f"\n  ► {label}", flush=True)

    # reset de metricas y flush cache
    post(f"{METRICS}/reset")
    post(f"{CACHE}/flush")

    # lanzar trafico
    cfg = {
        "distribution": distribution,
        "rate_qps":      float(rate),
        "duration_sec":  float(duration),
        "zipf_s":        float(zipf_s),
        "concurrency":   16,
        "seed":          42,
        "label":         label,
    }
    post(f"{TRAFFIC}/run", cfg)

    # esperar a que termine
    deadline = time.time() + duration + 45
    while time.time() < deadline:
        try:
            s = get(f"{TRAFFIC}/status")
            if not s.get("running"):
                break
        except Exception:
            pass
        time.sleep(2.0)
    time.sleep(2.5) 

    # guardar snapshot
    snap_body = {"label": label, "extra": {**cfg, **(extra or {})}}
    snap      = post(f"{METRICS}/snapshot", snap_body, timeout=30)
    summary   = snap["summary"]

    # persitir localmente
    RESULTS.mkdir(parents=True, exist_ok=True)
    out = RESULTS / f"snap_{label}.json"
    with open(out, "w") as f:
        json.dump(snap, f, indent=2, default=str)

    # printear el resumen
    hr   = summary.get("hit_rate") or 0
    thr  = summary.get("throughput_qps_total") or 0
    keys = (summary.get("cache_redis_stats") or {}).get("n_keys") or 0
    ev   = (summary.get("eviction") or {}).get("total_evicted") or 0
    p50h = (summary.get("latency_ms_hit") or {}).get("p50") or 0
    p95m = (summary.get("latency_ms_miss") or {}).get("p95") or 0
    print(f"    hit={hr:.4f}  thr={thr:.1f}qps  keys={keys}  evicted={ev}"
          f"  p50_hit={p50h:.2f}ms  p95_miss={p95m:.0f}ms", flush=True)
    return summary


# ================== suites ===================== #
def suite_demo():
    print("\n=== DEMO (1 experimento) ===", flush=True)
    wait_for_services()
    reconfigure_cache("LRU", SIZE_MAP["50mb"])
    run_exp("DEMO_LRU_50mb_zipf", "zipf", duration=15, rate=30)
    print("\n✓ Demo OK — el sistema está funcionando correctamente.")


def suite_official():
    print("\n=== SUITE OFICIAL (18 experimentos) ===", flush=True)
    wait_for_services()
    t0 = time.time()
    for pol in POLICIES:
        print(f"\n─── Política: {pol} ───", flush=True)
        for size in SIZES:
            reconfigure_cache(pol, SIZE_MAP[size])
            for dist in DISTS:
                label = f"{pol}_{size}_{dist}"
                run_exp(label, dist,
                        extra={"policy": pol, "size": size})
    print(f"\n✓ Suite oficial completada en {(time.time()-t0)/60:.1f} min")


def suite_small_cache():
    print("\n=== SUITE CACHÉ PEQUEÑO (3 experimentos, 2MB) ===", flush=True)
    wait_for_services()
    SMALL = 2 * 1024 * 1024
    for pol in POLICIES:
        reconfigure_cache(pol, SMALL)
        label = f"{pol}_2mb_zipf"
        run_exp(label, "zipf", duration=35,
                extra={"policy": pol, "size": "2mb", "purpose": "eviction_test"})
    print("\n✓ Suite caché pequeño completada")


def suite_long():
    print("\n=== SUITE LARGA (180s, TTL test) ===", flush=True)
    wait_for_services()
    reconfigure_cache("LRU", SIZE_MAP["50mb"])
    run_exp("LRU_50mb_zipf_long", "zipf", duration=180, rate=40,
            extra={"policy": "LRU", "size": "50mb", "purpose": "TTL_test"})
    print("\n✓ Suite larga completada")


def suite_all():
    print("\n" + "="*65)
    print("BATERÍA COMPLETA: 22 experimentos")
    print("="*65, flush=True)
    t0 = time.time()
    suite_official()
    suite_small_cache()
    suite_long()
    elapsed = time.time() - t0
    print(f"\n{'='*65}")
    print(f"✓ BATERÍA COMPLETA en {elapsed/60:.1f} min")
    print("  Resultados en: results/")
    print("  Ejecutar ahora: python experiments/build_figures.py")
    print("="*65)


# ================== main ===================== #

def main():
    p = argparse.ArgumentParser(description="Batería de experimentos")
    p.add_argument("--suite",
                   choices=["all", "official", "small", "long", "demo"],
                   default="all")
    args = p.parse_args()

    suites = {
        "all":      suite_all,
        "official": suite_official,
        "small":    suite_small_cache,
        "long":     suite_long,
        "demo":     suite_demo,
    }
    suites[args.suite]()


if __name__ == "__main__":
    main()
