#!/usr/bin/env bash
# Batería completa de experimentos.
# Re-construye los servicios con distintas configuraciones de política y
# tamaño, y para cada combinación corre experimentos con Zipf y Uniforme.
#
# Estimación de duración total: ~30-40 minutos.
# Uso: bash scripts/run_all_experiments.sh

set -e

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_DIR"

POLICIES=("LRU" "LFU" "FIFO")
SIZES=("50mb" "200mb" "500mb")
DURATION=60   # segundos por experimento
RATE=80       # QPS

# Mapping política -> política nativa Redis
declare -A NATIVE_POLICY=(
  ["LRU"]="allkeys-lru"
  ["LFU"]="allkeys-lfu"
  ["FIFO"]="noeviction"
)

run_combo() {
  local policy=$1
  local size=$2
  local label_prefix="${policy}_${size}"

  echo ""
  echo "########################################################"
  echo "## Iniciando combo: policy=$policy size=$size"
  echo "########################################################"

  # Re-escribir .env con la combinación
  cat > .env <<EOF
REDIS_MAXMEMORY=${size}
REDIS_POLICY_NATIVE=${NATIVE_POLICY[$policy]}
REDIS_PORT_HOST=6379
CACHE_POLICY=${policy}
CACHE_TTL_SEC=300
TTL_Q1=300
TTL_Q2=300
TTL_Q3=180
TTL_Q4=120
TTL_Q5=600
SIM_LATENCY_MIN_MS=30
SIM_LATENCY_MAX_MS=120
EOF

  echo "[run_all] Re-arrancando contenedores..."
  docker compose down -v --remove-orphans >/dev/null 2>&1 || true
  docker compose up -d --build

  echo "[run_all] Esperando healthchecks..."
  sleep 15
  # Espera adicional con polling
  for i in {1..30}; do
    if curl -sf http://localhost:8000/health >/dev/null \
       && curl -sf http://localhost:8001/health >/dev/null \
       && curl -sf http://localhost:8002/health >/dev/null \
       && curl -sf http://localhost:8003/health >/dev/null; then
      echo "[run_all] Todos los servicios listos."
      break
    fi
    echo "[run_all] Esperando... intento $i"
    sleep 2
  done

  # 1) Zipf
  python3 experiments/run_experiments.py \
    --suite baseline \
    --label "${label_prefix}_zipf" \
    --distribution zipf \
    --rate $RATE \
    --duration $DURATION

  # 2) Uniforme
  python3 experiments/run_experiments.py \
    --suite baseline \
    --label "${label_prefix}_uniform" \
    --distribution uniform \
    --rate $RATE \
    --duration $DURATION
}

main() {
  echo "[run_all] Verificando dataset..."
  if [ ! -f "data/santiago_buildings.parquet" ]; then
    echo "[run_all] Dataset no existe. Generando..."
    python3 data/generate_synthetic_data.py
  fi

  for policy in "${POLICIES[@]}"; do
    for size in "${SIZES[@]}"; do
      run_combo "$policy" "$size"
    done
  done

  echo ""
  echo "########################################################"
  echo "## TODOS los experimentos completados."
  echo "## Resultados en: results/ y snapshots/"
  echo "########################################################"
}

main
