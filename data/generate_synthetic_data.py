"""
Generador de dataset sintético compatible con Google Open Buildings.

Razón de uso: el dataset original de Google Open Buildings para Chile pesa
varios GB y requiere descarga desde GCS. Para reproducibilidad y velocidad
de despliegue, generamos un subconjunto sintético con la misma estructura
y distribuciones realistas (densidades por zona basadas en datos del INE
y áreas típicas residenciales/comerciales de Santiago).

Estructura compatible con el dataset real:
    latitude, longitude, area_in_meters, confidence

Salida: data/santiago_buildings.parquet (formato eficiente, columnar)
"""
import numpy as np
import pandas as pd
from pathlib import Path

# Semilla para reproducibilidad
RNG = np.random.default_rng(42)

# Las 5 zonas predefinidas (idénticas al enunciado)
ZONES = {
    "Z1": {  # Providencia: comuna densa, mixta residencial-comercial
        "name": "Providencia",
        "lat_min": -33.445, "lat_max": -33.420,
        "lon_min": -70.640, "lon_max": -70.600,
        "n_buildings": 22000,
        "area_mean": 180.0, "area_std": 120.0,
        "conf_mean": 0.82, "conf_std": 0.10,
    },
    "Z2": {  # Las Condes: comuna alta densidad, edificios grandes
        "name": "Las Condes",
        "lat_min": -33.420, "lat_max": -33.390,
        "lon_min": -70.600, "lon_max": -70.550,
        "n_buildings": 28000,
        "area_mean": 240.0, "area_std": 180.0,
        "conf_mean": 0.85, "conf_std": 0.09,
    },
    "Z3": {  # Maipú: residencial extenso, casas más pequeñas
        "name": "Maipu",
        "lat_min": -33.530, "lat_max": -33.490,
        "lon_min": -70.790, "lon_max": -70.740,
        "n_buildings": 38000,
        "area_mean": 110.0, "area_std": 60.0,
        "conf_mean": 0.78, "conf_std": 0.12,
    },
    "Z4": {  # Santiago Centro: muy denso, mezcla edificios y comercios
        "name": "Santiago Centro",
        "lat_min": -33.460, "lat_max": -33.430,
        "lon_min": -70.670, "lon_max": -70.630,
        "n_buildings": 32000,
        "area_mean": 200.0, "area_std": 150.0,
        "conf_mean": 0.80, "conf_std": 0.11,
    },
    "Z5": {  # Pudahuel: mixto residencial/industrial, área grande
        "name": "Pudahuel",
        "lat_min": -33.470, "lat_max": -33.430,
        "lon_min": -70.810, "lon_max": -70.760,
        "n_buildings": 21000,
        "area_mean": 150.0, "area_std": 200.0,  # mucha varianza por industrial
        "conf_mean": 0.74, "conf_std": 0.14,
    },
}


def generate_zone(zone_id: str, cfg: dict) -> pd.DataFrame:
    """Genera registros sintéticos para una zona dada."""
    n = cfg["n_buildings"]

    # Distribución espacial: clusters para simular barrios reales
    # En vez de uniforme puro, mezclamos uniforme con clusters gaussianos
    n_clusters = max(8, n // 3000)
    cluster_centers_lat = RNG.uniform(cfg["lat_min"], cfg["lat_max"], n_clusters)
    cluster_centers_lon = RNG.uniform(cfg["lon_min"], cfg["lon_max"], n_clusters)

    # 70% de edificios en clusters, 30% disperso
    n_clustered = int(0.7 * n)
    n_disperse = n - n_clustered

    cluster_assign = RNG.integers(0, n_clusters, n_clustered)
    lat_clustered = cluster_centers_lat[cluster_assign] + RNG.normal(0, 0.003, n_clustered)
    lon_clustered = cluster_centers_lon[cluster_assign] + RNG.normal(0, 0.004, n_clustered)

    lat_disperse = RNG.uniform(cfg["lat_min"], cfg["lat_max"], n_disperse)
    lon_disperse = RNG.uniform(cfg["lon_min"], cfg["lon_max"], n_disperse)

    lats = np.concatenate([lat_clustered, lat_disperse])
    lons = np.concatenate([lon_clustered, lon_disperse])

    # Recortar a la bounding box (los clusters pueden salirse un poco)
    lats = np.clip(lats, cfg["lat_min"], cfg["lat_max"])
    lons = np.clip(lons, cfg["lon_min"], cfg["lon_max"])

    # Áreas: log-normal para que la mayoría sea pequeño-medio con cola larga
    # mu, sigma se calculan para que la media y desviación coincidan aprox.
    mu = np.log(cfg["area_mean"] ** 2 / np.sqrt(cfg["area_std"] ** 2 + cfg["area_mean"] ** 2))
    sigma = np.sqrt(np.log(1 + (cfg["area_std"] ** 2) / (cfg["area_mean"] ** 2)))
    areas = RNG.lognormal(mu, sigma, n)
    areas = np.clip(areas, 15.0, 50000.0)  # rango plausible

    # Confianza: distribución beta, sesgada hacia valores altos
    # Aproximamos parámetros alpha/beta a partir de mean/std
    mean = cfg["conf_mean"]
    var = cfg["conf_std"] ** 2
    alpha = mean * (mean * (1 - mean) / var - 1)
    beta_p = (1 - mean) * (mean * (1 - mean) / var - 1)
    confidences = RNG.beta(max(alpha, 0.5), max(beta_p, 0.5), n)
    confidences = np.clip(confidences, 0.0, 1.0)

    df = pd.DataFrame({
        "zone_id": zone_id,
        "latitude": lats.astype(np.float32),
        "longitude": lons.astype(np.float32),
        "area_in_meters": areas.astype(np.float32),
        "confidence": confidences.astype(np.float32),
    })
    return df


def main():
    out_dir = Path(__file__).parent
    out_path = out_dir / "santiago_buildings.parquet"

    parts = []
    for zid, cfg in ZONES.items():
        print(f"[gen] {zid} ({cfg['name']}): {cfg['n_buildings']:,} buildings")
        parts.append(generate_zone(zid, cfg))

    df = pd.concat(parts, ignore_index=True)
    print(f"[gen] Total: {len(df):,} buildings, mem ~{df.memory_usage(deep=True).sum() / 1e6:.1f} MB")

    df.to_parquet(out_path, compression="snappy", index=False)
    print(f"[gen] Wrote {out_path}")

    # Sanity check
    print("\nSummary by zone:")
    summary = df.groupby("zone_id").agg(
        n=("latitude", "count"),
        avg_area=("area_in_meters", "mean"),
        avg_conf=("confidence", "mean"),
    ).round(3)
    print(summary)


if __name__ == "__main__":
    main()
