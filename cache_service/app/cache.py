"""
Wrapper de caché sobre Redis con tres políticas de evicción:
    - LRU  (nativo: maxmemory-policy allkeys-lru)
    - LFU  (nativo: maxmemory-policy allkeys-lfu)
    - FIFO (custom — Redis no la trae nativa)

Para FIFO mantenemos una lista paralela `fifo:order` con los keys en
orden de inserción. Cuando se detecta que el uso de memoria supera
maxmemory, eliminamos los keys más antiguos manualmente.
"""
import os
import json
import time
import logging
from typing import Optional

import redis

log = logging.getLogger("cache")

# Política configurada al desplegar Redis (env var)
POLICY = os.getenv("CACHE_POLICY", "LRU").upper()
# TTL global por defecto en segundos (0 = sin expiración)
DEFAULT_TTL = int(os.getenv("CACHE_TTL_SEC", "300"))
# Para FIFO: cada cuántas inserciones revisar memoria
FIFO_CHECK_EVERY = int(os.getenv("FIFO_CHECK_EVERY", "50"))

FIFO_ORDER_KEY = "__fifo_order__"


class CacheClient:
    """
    Cliente unificado que abstrae las tres políticas.
    LRU/LFU se delegan a Redis. FIFO se gestiona en cliente.
    """

    def __init__(self, host: str, port: int, db: int = 0):
        self.r = redis.Redis(host=host, port=port, db=db, decode_responses=True)
        self.policy = POLICY
        self._fifo_counter = 0
        self._wait_for_redis()
        self._configure_policy()

    def _wait_for_redis(self, timeout: int = 30):
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                self.r.ping()
                log.info(f"Redis conectado")
                return
            except redis.ConnectionError:
                time.sleep(0.5)
        raise RuntimeError("Redis no respondió a tiempo")

    def _configure_policy(self):
        """Configura la política nativa de Redis según POLICY."""
        if self.policy in ("LRU", "LFU"):
            policy_str = "allkeys-lru" if self.policy == "LRU" else "allkeys-lfu"
            try:
                self.r.config_set("maxmemory-policy", policy_str)
                log.info(f"Política Redis configurada: {policy_str}")
            except redis.ResponseError as e:
                log.warning(f"No se pudo configurar política ({e}); asumiendo set en docker")
        elif self.policy == "FIFO":
            # Para FIFO usamos noeviction y manejamos evicciones nosotros
            try:
                self.r.config_set("maxmemory-policy", "noeviction")
                log.info("Política Redis: noeviction (FIFO manejado por cliente)")
            except redis.ResponseError as e:
                log.warning(f"No se pudo configurar política: {e}")
        else:
            raise ValueError(f"Política desconocida: {self.policy}")

    def get(self, key: str) -> Optional[dict]:
        """Retorna el valor (parseado de JSON) o None si no existe."""
        raw = self.r.get(key)
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            log.error(f"Valor corrupto en key {key}")
            return None

    def set(self, key: str, value: dict, ttl: Optional[int] = None) -> bool:
        """Guarda un valor con TTL (default a DEFAULT_TTL si no se da)."""
        ttl_to_use = ttl if ttl is not None else DEFAULT_TTL
        payload = json.dumps(value, separators=(",", ":"))

        if ttl_to_use > 0:
            self.r.set(key, payload, ex=ttl_to_use)
        else:
            self.r.set(key, payload)

        if self.policy == "FIFO":
            # Empuja al final de la lista de orden
            self.r.rpush(FIFO_ORDER_KEY, key)
            self._fifo_counter += 1
            if self._fifo_counter % FIFO_CHECK_EVERY == 0:
                self._fifo_evict_if_needed()
        return True

    def _fifo_evict_if_needed(self):
        """
        Elimina los keys más antiguos hasta que used_memory < maxmemory.
        Solo aplica para política FIFO.
        """
        try:
            info = self.r.info("memory")
            used = int(info.get("used_memory", 0))
            maxmem = int(info.get("maxmemory", 0) or 0)
        except Exception as e:
            log.warning(f"FIFO: no pude leer info memory: {e}")
            return

        if maxmem == 0 or used <= maxmem:
            return

        evicted = 0
        # Borrar de a lotes de 50 hasta caber
        while True:
            try:
                info = self.r.info("memory")
                used = int(info.get("used_memory", 0))
            except Exception:
                break
            if used <= maxmem * 0.95:  # margen de 5% para no rebotar
                break

            # Pop el más viejo
            old_key = self.r.lpop(FIFO_ORDER_KEY)
            if old_key is None:
                break  # No quedan keys en lista
            # Borrar de Redis (puede ya haber expirado por TTL)
            n = self.r.delete(old_key)
            if n > 0:
                evicted += 1
                # Registrar evicción manual en contador
                self.r.incr("__fifo_evictions__")
            if evicted > 1000:  # safety
                break

        if evicted > 0:
            log.info(f"FIFO: evicted {evicted} keys")

    def stats(self) -> dict:
        """Stats agregados de Redis para métricas."""
        info = self.r.info()
        result = {
            "policy": self.policy,
            "used_memory": int(info.get("used_memory", 0)),
            "used_memory_human": info.get("used_memory_human", "0"),
            "maxmemory": int(info.get("maxmemory", 0) or 0),
            "n_keys": self.r.dbsize() - (1 if self.policy == "FIFO" else 0),  # excluye lista FIFO
            "evicted_keys": int(info.get("evicted_keys", 0)),
            "keyspace_hits": int(info.get("keyspace_hits", 0)),
            "keyspace_misses": int(info.get("keyspace_misses", 0)),
        }
        if self.policy == "FIFO":
            try:
                manual = int(self.r.get("__fifo_evictions__") or 0)
                result["evicted_keys"] = manual  # usar contador manual
            except Exception:
                pass
        return result

    def flushall(self):
        self.r.flushdb()
