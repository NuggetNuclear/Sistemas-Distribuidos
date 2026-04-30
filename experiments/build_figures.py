# ================== dependencias del projecto ============== #
import json
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np


# ================== configuracion de los graficos ============== #
matplotlib.rcParams.update({
    "font.size": 11,
    "axes.titlesize": 12,
    "axes.labelsize": 11,
    "legend.fontsize": 10,
    "figure.dpi": 110,
    "savefig.dpi": 180,
    "savefig.bbox": "tight",
    "axes.spines.top": False,
    "axes.spines.right": False,
})


# ================== rutas para guardar ============== #
ROOT = Path(__file__).parent.parent
RES  = ROOT / "results"
FIG  = ROOT / "informe" / "figs"
FIG.mkdir(parents=True, exist_ok=True)


# ================== configuracion experimentos ============== #
POLICIES = ["LRU", "LFU", "FIFO"]
SIZES    = ["50mb", "200mb", "500mb"]
DISTS    = ["zipf", "uniform"]


# ================== colores ============== #
C = {"LRU": "#1d4faa",
     "LFU": "#0a8754",
     "FIFO": "#c1453b",
     "zipf": "#2c6dd6",
     "uniform": "#888888"
}


# ================== carga de snapshots ===================== #
def snap(label):
    p = RES / f"snap_{label}.json"
    if not p.exists():
        return None
    d = json.load(open(p))
    return d.get("summary", d)


# ================== acceso a las metricas ===================== #
def v(s, *keys, default=0):
    for k in keys:
        if s is None: return default
        s = (s or {}).get(k)
    return s if s is not None else default


# ================== guardar figuras ===================== #
def save(fig, name):
    for ext in ("pdf", "png"):
        fig.savefig(FIG / f"{name}.{ext}")
    plt.close()
    print(f"[fig] {name}")


# ================== figura 1 ===================== #
def fig1():
    labels  = ["LRU_50mb_uniform","LRU_50mb_zipf","LFU_50mb_zipf","FIFO_50mb_zipf"]
    pretty  = ["Uniforme\n(LRU)","Zipf s=1.5\n(LRU)","Zipf s=1.5\n(LFU)","Zipf s=1.5\n(FIFO)"]
    colors  = ["#888888","#1d4faa","#0a8754","#c1453b"]
    
    hrs     = [v(snap(l),"hit_rate") for l in labels]
    
    fig, ax = plt.subplots(figsize=(7,4))
    
    bars = ax.bar(pretty, hrs, color=colors, width=0.55, zorder=2)
    
    for b,h in zip(bars,hrs):
        ax.text(b.get_x()+b.get_width()/2, h+0.008, f"{h:.3f}", ha="center", fontsize=10, fontweight="bold")
        
    ax.set_ylabel("Hit rate");
    ax.set_ylim(0,1)
    
    ax.axhline(0.5, ls="--", color="grey", alpha=0.3)
    ax.set_title("Hit rate por distribución y política (caché 50 MB)",pad=12)
    ax.grid(axis="y",alpha=0.3,zorder=0);
    
    fig.tight_layout()
    
    save(fig,"fig1_hit_rate_distribution")


# ================== figura 2 ===================== #
def fig2():
    fig, ax = plt.subplots(figsize=(8,4.5))
    
    x = np.arange(len(POLICIES));
    w = 0.35
    
    for i, dist in enumerate(DISTS):
        hrs = [v(snap(f"{pol}_50mb_{dist}"),"hit_rate") for pol in POLICIES]
        
        bars = ax.bar(x+(i-0.5)*w, hrs, w, label=dist.capitalize(), color=C[dist], alpha=0.9, zorder=2)
        
        for b,h in zip(bars,hrs):
            ax.text(b.get_x()+b.get_width()/2, h+0.01, f"{h:.3f}", ha="center", fontsize=9)
            
    ax.set_xticks(x);
    ax.set_xticklabels(POLICIES)
    
    ax.set_ylabel("Hit rate");
    ax.set_ylim(0,1)
    
    ax.set_title("Hit rate: Zipf vs Uniforme por política (caché 50 MB)",pad=10)
    ax.legend(title="Distribución"); ax.grid(axis="y",alpha=0.3,zorder=0)
    
    fig.tight_layout();
    
    save(fig,"fig2_zipf_vs_uniform")


# ================== figura 3 ===================== #
def fig3():
    fig, ax = plt.subplots(figsize=(9,4.5))
    
    x = np.arange(len(SIZES));
    w = 0.25
    
    for i, pol in enumerate(POLICIES):
        
        hrs = [v(snap(f"{pol}_{sz}_zipf"),"hit_rate") for sz in SIZES]
        bars = ax.bar(x+(i-1)*w, hrs, w, label=pol, color=C[pol], alpha=0.9, zorder=2)
        
        for b,h in zip(bars,hrs):
            ax.text(b.get_x()+b.get_width()/2, h+0.008, f"{h:.3f}", ha="center", fontsize=8)

    
    ax.set_xticks(x);
    ax.set_xticklabels(SIZES)
    
    ax.set_xlabel("Tamaño del caché");
    ax.set_ylabel("Hit rate")
    
    ax.set_ylim(0.7,0.9)
    
    ax.set_title("Hit rate vs. tamaño de caché (Zipf s=1.5)",pad=10)
    ax.legend(title="Política"); ax.grid(axis="y",alpha=0.3,zorder=0)
    
    ax.text(0.5, 0.04, "Sin evicciones en ningún tamaño — el keyspace cabe en 50 MB", ha="center", va="bottom", transform=ax.transAxes, fontsize=9, color="grey", style="italic")
    
    fig.tight_layout();
    
    save(fig,"fig3_size_effect")


# ================== figura 4 ===================== #
def fig4():
    fig, ax = plt.subplots(figsize=(8,4.5))
    
    x = np.arange(len(POLICIES));
    w = 0.35
    
    for i, dist in enumerate(DISTS):
        
        thrs = [v(snap(f"{pol}_50mb_{dist}"),"throughput_qps_total") for pol in POLICIES]
        
        bars = ax.bar(x+(i-0.5)*w, thrs, w, label=dist.capitalize(), color=C[dist], alpha=0.9, zorder=2)
        
        for b,t in zip(bars,thrs):
            ax.text(b.get_x()+b.get_width()/2, t+0.4, f"{t:.1f}", ha="center", fontsize=9)
            
    ax.axhline(60, ls="--", color="red", alpha=0.4, label="Objetivo 60 qps")
    
    ax.set_xticks(x);
    ax.set_xticklabels(POLICIES)
    
    ax.set_ylabel("Throughput sostenido (qps)")
    ax.set_title("Throughput por política y distribución (caché 50 MB)",pad=10)
    
    ax.legend();
    ax.grid(axis="y",alpha=0.3,zorder=0)
    
    fig.tight_layout();
    
    save(fig,"fig4_throughput")


# ================== figura 5 ===================== #
def fig5():
    fig, ax = plt.subplots(figsize=(9,4.5))
    
    x = np.arange(len(POLICIES));
    w = 0.2

    # latencias percentil 50 y 95 para hits
    p50h = [v(snap(f"{p}_50mb_zipf"),"latency_ms_hit","p50") or 0.01 for p in POLICIES]
    p95h = [v(snap(f"{p}_50mb_zipf"),"latency_ms_hit","p95") or 0.01 for p in POLICIES]

    # latencias percentil 50 y 95 para miss
    p50m = [v(snap(f"{p}_50mb_zipf"),"latency_ms_miss","p50") or 0.01 for p in POLICIES]
    p95m = [v(snap(f"{p}_50mb_zipf"),"latency_ms_miss","p95") or 0.01 for p in POLICIES]

    # barras agrupadas para comparar hit vs miss
    ax.bar(x-1.5*w,p50h,w,label="p50 hit",  color="#1d4faa",alpha=0.9,zorder=2)
    ax.bar(x-0.5*w,p95h,w,label="p95 hit",  color="#6d9fdc",alpha=0.9,zorder=2)
    ax.bar(x+0.5*w,p50m,w,label="p50 miss", color="#c1453b",alpha=0.9,zorder=2)
    ax.bar(x+1.5*w,p95m,w,label="p95 miss", color="#e8958a",alpha=0.9,zorder=2)
    
    ax.set_yscale("log")
    
    ax.set_xticks(x);
    ax.set_xticklabels(POLICIES)
    
    ax.set_ylabel("Latencia (ms, escala log)")
    ax.set_title("Latencia p50/p95 hit vs miss — 3 políticas (Zipf, 50 MB)",pad=10)
    
    ax.legend(ncol=4,loc="upper center",bbox_to_anchor=(0.5,-0.13))
    ax.grid(axis="y",alpha=0.3,which="both",zorder=0)
    
    fig.tight_layout();
    
    save(fig,"fig5_latency_hit_miss")


# ================== figura 6 ===================== #
def fig6():
    queries = ["Q1","Q2","Q3","Q4","Q5"]
    
    x = np.arange(len(queries));
    w = 0.35
    
    fig, ax = plt.subplots(figsize=(9,4.5))
    
    for i, (label, name, color) in enumerate([
        ("LRU_50mb_zipf",    "Zipf (LRU)",     "#2c6dd6"),
        ("LRU_50mb_uniform", "Uniforme (LRU)",  "#888888"),
    ]):
        s  = snap(label)
        bq = (s or {}).get("by_query",{})
        
        if not bq:
            continue
            
        ys = [(bq.get(f"Q{j+1}") or {}).get("hit_rate") or 0 for j in range(5)]
        
        bars = ax.bar(x+(i-0.5)*w, ys, w, label=name, color=color, alpha=0.9, zorder=2)
        
        for b,y in zip(bars,ys):
            ax.text(b.get_x()+b.get_width()/2, y+0.01, f"{y:.2f}", ha="center", fontsize=9)
            
    ax.set_xticks(x);
    ax.set_xticklabels(queries)
    
    ax.set_ylabel("Hit rate");
    ax.set_ylim(0,1.05)
    
    ax.set_title("Hit rate por tipo de consulta Q1–Q5 (caché 50 MB, LRU)",pad=10)
    
    ax.legend();
    ax.grid(axis="y",alpha=0.3,zorder=0)
    
    fig.tight_layout();
    
    save(fig,"fig6_by_query")


# ================== figura 7 ===================== #
def fig7():
    
    labels = ["LRU_50mb_zipf",
              "LFU_50mb_zipf",
              "FIFO_50mb_zipf",
              "LRU_50mb_zipf_long"
             ]
    
    pretty = ["LRU\n(25 s)",
              "LFU\n(25 s)",
              "FIFO\n(25 s)",
              "LRU\n(180 s)"
             ]
    
    colors = [C["LRU"], 
              C["LFU"],
              C["FIFO"],
              "#8B4513"
             ]
    
    effs   = [v(snap(l),"cache_efficiency") for l in labels]
    
    fig, ax = plt.subplots(figsize=(7,4))
    
    bars = ax.bar(pretty, effs, color=colors, width=0.5, zorder=2)
    
    for b,e in zip(bars,effs):
        ax.text(b.get_x()+b.get_width()/2, e+3, f"{e:.0f}", ha="center", fontsize=10, fontweight="bold")
        
    ax.set_ylabel("Cache efficiency\n(ms/consulta ahorrado)")
    
    ax.set_title("Cache efficiency por política y duración (Zipf, 50 MB)",pad=10)
    ax.grid(axis="y",alpha=0.3,zorder=0);
    
    fig.tight_layout()
    
    save(fig,"fig7_cache_efficiency")

# ================== ejecucion del script ===================== #
if __name__ == "__main__":
    
    fig1();
    fig2();
    fig3();
    fig4();
    fig5();
    fig6();
    fig7()
    print(f"\n✓ 7 figuras en {FIG}/")
