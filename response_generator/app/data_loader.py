#  ============= dependencias del proyecto ============= #
import os
from pathlib import Path
import pandas as pd
import numpy as np

#  ============= zonas geograficas del dataset ============= #
ZONES = {
    "Z1": {"name": "Providencia",
           "lat_min": -33.445, "lat_max": -33.420,
           "lon_min": -70.640, "lon_max": -70.600},
    "Z2": {"name": "Las Condes",
           "lat_min": -33.420, "lat_max": -33.390,
           "lon_min": -70.600, "lon_max": -70.550},
    "Z3": {"name": "Maipu",
           "lat_min": -33.530, "lat_max": -33.490,
           "lon_min": -70.790, "lon_max": -70.740},
    "Z4": {"name": "Santiago Centro",
           "lat_min": -33.460, "lat_max": -33.430,
           "lon_min": -70.670, "lon_max": -70.630},
    "Z5": {"name": "Pudahuel",
           "lat_min": -33.470, "lat_max": -33.430,
           "lon_min": -70.810, "lon_max": -70.760},
}



def haversine_km2(lat_min: float, lat_max: float, lon_min: float, lon_max: float) -> float:

    lat_mid = (lat_min + lat_max) / 2
    dlat_km = (lat_max - lat_min) * 111.32
    dlon_km = (lon_max - lon_min) * 111.32 * np.cos(np.radians(lat_mid))
    return abs(dlat_km * dlon_km)

#  ============= area de las zonas ============= #
ZONE_AREA_KM2 = {
    zid: haversine_km2(z["lat_min"], z["lat_max"], z["lon_min"], z["lon_max"])
    for zid, z in ZONES.items()
}


class DataStore:
    
#  ============= inicializacion del datastore ============= #
    def __init__(self, parquet_path: str):
        self.path = parquet_path
        self.by_zone: dict[str, pd.DataFrame] = {}
        self._load()

#  ============= carga del dataset parquet en memoria ============= #
    def _load(self):
        if not os.path.exists(self.path):
            raise FileNotFoundError(
                f"Dataset no encontrado en {self.path}. "
                f"Genera con: python data/generate_synthetic_data.py"
            )
        df = pd.read_parquet(self.path)

        for zid in ZONES:
            sub = df[df["zone_id"] == zid].reset_index(drop=True)
            self.by_zone[zid] = sub
        total = sum(len(v) for v in self.by_zone.values())
        mem = sum(v.memory_usage(deep=True).sum() for v in self.by_zone.values()) / 1e6
        print(f"[data] Cargados {total:,} edificios en {len(self.by_zone)} zonas ({mem:.1f} MB)")

#  ============= sacar dataframe de una zona ============= #
    def get_zone(self, zone_id: str) -> pd.DataFrame:
        if zone_id not in self.by_zone:
            raise ValueError(f"Zona desconocida: {zone_id}")
        return self.by_zone[zone_id]

    def zone_area_km2(self, zone_id: str) -> float:
        return ZONE_AREA_KM2[zone_id]
