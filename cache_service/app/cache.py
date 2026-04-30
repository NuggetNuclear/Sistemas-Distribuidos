# ======== dependencias del proyecto ======== #
import os
import json
import time
import logging
from typing import Optional
import redis


# ====== configuracion de logging ====== #
log = logging.getLogger("cache")


# ============= configuracion de politicas ============= #
POLICY = os.getenv("CACHE_POLICY", "LRU").upper()
DEFAULT_TTL = int(os.getenv("CACHE_TTL_SEC", "300"))
FIFO_CHECK_EVERY = int(os.getenv("FIFO_CHECK_EVERY", "50"))
FIFO_ORDER_KEY = "__fifo_order__"


#============ cliente de cache ================== #
class CacheClient:

    
# ================ inicializacion =============== #
    def __init__(self, host: str, port: int, db: int = 0):
        self.r = redis.Redis(host=host, port=port, db=db, decode_responses=True)
        self.policy = POLICY
        self._fifo_counter = 0
        self._wait_for_redis()
        self._configure_policy()

    
# ============== conexion con redis ============ #
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

    
# ==================== configuracion de politicas ==================== #
    def _configure_policy(self):
        if self.policy in ("LRU", "LFU"):
            policy_str = "allkeys-lru" if self.policy == "LRU" else "allkeys-lfu"
            try:
                self.r.config_set("maxmemory-policy", policy_str)
                log.info(f"Política Redis configurada: {policy_str}")
            except redis.ResponseError as e:
                log.warning(f"No se pudo configurar política ({e}); asumiendo set en docker")
        elif self.policy == "FIFO":
            try:
                self.r.config_set("maxmemory-policy", "noeviction")
                log.info("Política Redis: noeviction (FIFO manejado por cliente)")
            except redis.ResponseError as e:
                log.warning(f"No se pudo configurar política: {e}")
        else:
            raise ValueError(f"Política desconocida: {self.policy}")

    
# ======================= obtener el valor del cache ======================= #
    def get(self, key: str) -> Optional[dict]:
        raw = self.r.get(key)
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            log.error(f"Valor corrupto en key {key}")
            return None

    
# ======================= guardar el valor del cache ======================= #
    def set(self, key: str, value: dict, ttl: Optional[int] = None) -> bool:
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


# ======================= eviccion manual FIFO ======================= #
    def _fifo_evict_if_needed(self):
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
        
        while True:
            try:
                info = self.r.info("memory")
                used = int(info.get("used_memory", 0))
            except Exception:
                break
            if used <= maxmem * 0.95:
                break

            old_key = self.r.lpop(FIFO_ORDER_KEY)
            if old_key is None:
                break 
            n = self.r.delete(old_key)
            if n > 0:
                evicted += 1
                self.r.incr("__fifo_evictions__")
            if evicted > 1000:
                break

        if evicted > 0:
            log.info(f"FIFO: evicted {evicted} keys")


# ======================= stats del cache ======================= #
    def stats(self) -> dict:
        """Stats agregados de Redis para métricas."""
        info = self.r.info()
        result = {
            "policy": self.policy,
            "used_memory": int(info.get("used_memory", 0)),
            "used_memory_human": info.get("used_memory_human", "0"),
            "maxmemory": int(info.get("maxmemory", 0) or 0),
            "n_keys": self.r.dbsize() - (1 if self.policy == "FIFO" else 0),
            "evicted_keys": int(info.get("evicted_keys", 0)),
            "keyspace_hits": int(info.get("keyspace_hits", 0)),
            "keyspace_misses": int(info.get("keyspace_misses", 0)),
        }
        if self.policy == "FIFO":
            try:
                manual = int(self.r.get("__fifo_evictions__") or 0)
                result["evicted_keys"] = manual
            except Exception:
                pass
        return result

    
# ======================= limpieza ======================= #
    def flushall(self):
        self.r.flushdb()
