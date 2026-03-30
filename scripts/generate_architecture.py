"""
generate_architecture.py — Render the pipeline architecture diagram as a PNG.

Output: architecture.png in the project root.
Run:    python scripts/generate_architecture.py
"""

import os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "..", "architecture.png")

# ── Colour palette (dark theme matching the dashboard) ────────────────────────
BG          = "#0a0a0a"
CARD        = "#111111"
BORDER      = "#2a2a2a"
TEXT        = "#f5f5f7"
SECONDARY   = "#86868b"
AAPL_BLUE   = "#0071e3"
TESLA_RED   = "#e31937"
APPLE_GREEN = "#30d158"
ORANGE      = "#ff9f0a"
PURPLE      = "#bf5af2"
ARROW       = "#3a3a3a"
ARROW_ACT   = "#555555"

FONT = "DejaVu Sans"

fig, ax = plt.subplots(figsize=(22, 14))
fig.patch.set_facecolor(BG)
ax.set_facecolor(BG)
ax.set_xlim(0, 22)
ax.set_ylim(0, 14)
ax.axis("off")


# ── Helpers ────────────────────────────────────────────────────────────────────

def box(ax, x, y, w, h, label, sublabel="", color=CARD, border=BORDER,
        fontsize=9.5, label_color=TEXT, sub_color=SECONDARY, radius=0.25):
    patch = FancyBboxPatch(
        (x, y), w, h,
        boxstyle=f"round,pad=0,rounding_size={radius}",
        linewidth=1.2,
        edgecolor=border,
        facecolor=color,
        zorder=2,
    )
    ax.add_patch(patch)
    cx, cy = x + w / 2, y + h / 2
    if sublabel:
        ax.text(cx, cy + 0.13, label, ha="center", va="center",
                fontsize=fontsize, color=label_color, fontfamily=FONT,
                fontweight="bold", zorder=3)
        ax.text(cx, cy - 0.18, sublabel, ha="center", va="center",
                fontsize=7.5, color=sub_color, fontfamily=FONT, zorder=3)
    else:
        ax.text(cx, cy, label, ha="center", va="center",
                fontsize=fontsize, color=label_color, fontfamily=FONT,
                fontweight="bold", zorder=3)


def small_box(ax, x, y, w, h, label, color=CARD, border=BORDER,
              fontsize=8, label_color=TEXT):
    patch = FancyBboxPatch(
        (x, y), w, h,
        boxstyle="round,pad=0,rounding_size=0.15",
        linewidth=0.8,
        edgecolor=border,
        facecolor=color,
        zorder=4,
    )
    ax.add_patch(patch)
    ax.text(x + w / 2, y + h / 2, label, ha="center", va="center",
            fontsize=fontsize, color=label_color, fontfamily=FONT, zorder=5)


def arrow(ax, x1, y1, x2, y2, color=ARROW_ACT, lw=1.5):
    ax.annotate(
        "",
        xy=(x2, y2), xytext=(x1, y1),
        arrowprops=dict(
            arrowstyle="-|>",
            color=color,
            lw=lw,
            mutation_scale=12,
        ),
        zorder=3,
    )


def label_arrow(ax, x, y, text, color=SECONDARY, fontsize=7.5):
    ax.text(x, y, text, ha="center", va="center",
            fontsize=fontsize, color=color, fontfamily=FONT,
            style="italic", zorder=6)


def section_title(ax, x, y, text, color=SECONDARY):
    ax.text(x, y, text.upper(), ha="left", va="center",
            fontsize=7, color=color, fontfamily=FONT,
            fontweight="bold", zorder=6)


def outline_box(ax, x, y, w, h, label, color="#1a1a1a", border="#333333"):
    patch = FancyBboxPatch(
        (x, y), w, h,
        boxstyle="round,pad=0,rounding_size=0.35",
        linewidth=1.0,
        edgecolor=border,
        facecolor=color,
        linestyle="--",
        zorder=1,
    )
    ax.add_patch(patch)
    ax.text(x + 0.18, y + h - 0.25, label, ha="left", va="top",
            fontsize=7.5, color=border, fontfamily=FONT,
            fontweight="bold", zorder=2)


# ── Main title ─────────────────────────────────────────────────────────────────
ax.text(11, 13.55, "Market Intelligence Data Pipeline — Architecture",
        ha="center", va="center", fontsize=16, color=TEXT,
        fontfamily=FONT, fontweight="bold", zorder=6)
ax.text(11, 13.2, "AAPL · TSLA · SPY  |  Alpha Vantage → GCS → BigQuery → dbt → Plotly",
        ha="center", va="center", fontsize=9, color=SECONDARY,
        fontfamily=FONT, zorder=6)

# ── Horizontal divider ────────────────────────────────────────────────────────
ax.plot([0.4, 21.6], [13.0, 13.0], color=BORDER, linewidth=0.8, zorder=1)


# ══════════════════════════════════════════════════════════════════════════════
# COLUMN 1 — External Source (x: 0.4 – 3.2)
# ══════════════════════════════════════════════════════════════════════════════
section_title(ax, 0.45, 12.75, "External API")
outline_box(ax, 0.4, 7.2, 2.8, 5.35, "")

box(ax, 0.6, 10.6, 2.4, 1.6,
    "Alpha Vantage",
    "TIME_SERIES_DAILY\nAAAPL · TSLA · SPY",
    color="#0d1a2e", border=AAPL_BLUE, label_color=AAPL_BLUE,
    fontsize=9)

# API bullet points
bullets = [
    "• Last 100 trading days",
    "• compact output size",
    "• 25 req/day free tier",
    "• 15 s sleep between calls",
]
for i, b in enumerate(bullets):
    ax.text(0.75, 10.2 - i * 0.28, b, fontsize=7, color=SECONDARY,
            fontfamily=FONT, zorder=6)


# ══════════════════════════════════════════════════════════════════════════════
# COLUMN 2 — Airflow / Docker (x: 3.6 – 9.8)
# ══════════════════════════════════════════════════════════════════════════════
section_title(ax, 3.65, 12.75, "Orchestration — Apache Airflow 2.8 (Docker Compose)")
outline_box(ax, 3.6, 0.4, 6.5, 12.3, "")

# Docker inner label
ax.text(3.78, 12.45, "docker-compose  |  LocalExecutor  |  PostgreSQL metadata",
        fontsize=7.5, color=SECONDARY, fontfamily=FONT, zorder=6)

# 8 Task boxes stacked
task_cfg = [
    ("1  extract_alpha_vantage",    "Fetch JSON → /tmp/raw/",              AAPL_BLUE),
    ("2  store_raw_to_gcs",         "Upload JSON → GCS raw/",              "#404040"),
    ("3  transform_to_parquet",     "pandas concat → Snappy Parquet",      "#404040"),
    ("4  upload_parquet_to_gcs",    "Upload Parquet → GCS curated/",       "#404040"),
    ("5  load_to_bigquery",         "BQ Load Job · WRITE_TRUNCATE (partition)", "#404040"),
    ("6  verify_bq_load",           "Assert row_count > 0 for date",       "#2a3a2a"),
    ("7  dbt_run",                  "dbt deps + dbt run --full-refresh",   PURPLE),
    ("8  dbt_test",                 "dbt test — 5 quality checks",         "#2a2a3a"),
]

task_y_start = 11.5
task_h       = 1.12
task_gap     = 0.07
task_x       = 3.85
task_w       = 5.95

for i, (name, sub, bdr) in enumerate(task_cfg):
    ty = task_y_start - i * (task_h + task_gap)
    c  = "#161616" if bdr == "#404040" else "#0d0d1a" if bdr == PURPLE else \
         "#0d1a0d" if bdr == "#2a3a2a" else "#0d1a2e" if bdr == AAPL_BLUE else "#12121a"
    box(ax, task_x, ty, task_w, task_h, name, sub,
        color=c, border=bdr, fontsize=8.5, label_color=TEXT)

    # Arrow between tasks
    if i < len(task_cfg) - 1:
        ax.annotate("", xy=(task_x + task_w / 2, ty - task_gap),
                    xytext=(task_x + task_w / 2, ty),
                    arrowprops=dict(arrowstyle="-|>", color=ARROW_ACT,
                                    lw=1.2, mutation_scale=9),
                    zorder=3)

# XCom label
ax.text(9.85, 7.2, "XCom\nmetadata", ha="center", fontsize=6.8,
        color=SECONDARY, fontfamily=FONT, style="italic", zorder=6)


# ══════════════════════════════════════════════════════════════════════════════
# COLUMN 3 — GCS (x: 10.5 – 14.0)
# ══════════════════════════════════════════════════════════════════════════════
section_title(ax, 10.55, 12.75, "Data Lake — Google Cloud Storage")
outline_box(ax, 10.5, 4.8, 3.65, 7.85, "GCS Bucket\n{project}-market-data-lake",
            color="#0d1a0d", border="#2a4a2a")

# Raw layer
box(ax, 10.7, 10.85, 3.2, 1.5,
    "raw/ layer",
    "raw/{sym}/date=YYYY-MM-DD\n/data.json",
    color="#101a10", border=APPLE_GREEN, label_color=APPLE_GREEN, fontsize=8.5)

# Curated layer
box(ax, 10.7, 8.85, 3.2, 1.65,
    "curated/ layer",
    "curated/stock_prices/\ndate=YYYY-MM-DD/data.parquet",
    color="#101a10", border=APPLE_GREEN, label_color=APPLE_GREEN, fontsize=8.5)

# Storage props
storage_props = [
    "• Versioning enabled",
    "• STANDARD → NEARLINE at 90 d",
    "• Deleted at 365 d",
    "• Hive-style partition paths",
]
for i, p in enumerate(storage_props):
    ax.text(10.75, 8.55 - i * 0.26, p, fontsize=7, color=SECONDARY,
            fontfamily=FONT, zorder=6)


# ══════════════════════════════════════════════════════════════════════════════
# COLUMN 4 — BigQuery (x: 14.5 – 18.3)
# ══════════════════════════════════════════════════════════════════════════════
section_title(ax, 14.55, 12.75, "Warehouse — BigQuery  (dataset: market_analytics)")
outline_box(ax, 14.5, 2.2, 3.9, 10.45, "GCP BigQuery", color="#0d0d1a", border="#2a2a5a")

# raw_stock_prices
box(ax, 14.7, 10.85, 3.5, 1.55,
    "raw_stock_prices",
    "AIRFLOW load target\nPartitioned (date) · Clustered (symbol)",
    color="#12121f", border="#4a4aaa", label_color="#a0a0ff", fontsize=8.5)

# stg_stock_prices
box(ax, 14.7, 8.7, 3.5, 1.75,
    "stg_stock_prices",
    "dbt VIEW — no storage cost\nType cast · Dedup · Null filter",
    color="#12121f", border=PURPLE, label_color=PURPLE, fontsize=8.5)

# mart_daily_metrics
box(ax, 14.7, 5.2, 3.5, 3.1,
    "mart_daily_metrics",
    "dbt TABLE\nPartitioned (date) · Clustered (symbol)\n"
    "─────────────────────\n"
    "sma_20 / sma_50 / sma_200\ndaily_return · cumulative_return\n"
    "rolling_volatility_20d · drawdown\nexcess_return_vs_spy",
    color="#12121f", border=PURPLE, label_color=PURPLE, fontsize=8)

# BQ props
bq_props = [
    "• Partition pruning on date",
    "• Symbol clustering",
    "• Serverless · auto-scale",
]
for i, p in enumerate(bq_props):
    ax.text(14.75, 4.9 - i * 0.26, p, fontsize=7, color=SECONDARY,
            fontfamily=FONT, zorder=6)


# ══════════════════════════════════════════════════════════════════════════════
# COLUMN 5 — Dashboard (x: 18.8 – 21.6)
# ══════════════════════════════════════════════════════════════════════════════
section_title(ax, 18.85, 12.75, "Visualisation — Plotly")
outline_box(ax, 18.8, 4.0, 2.75, 8.65, "", color="#1a0d0d", border="#5a2a2a")

box(ax, 18.95, 11.45, 2.4, 1.55,
    "dashboard.html",
    "Self-contained HTML\nNo server required",
    color="#1a0d0d", border=TESLA_RED, label_color=TESLA_RED, fontsize=8.5)

tiles = [
    ("KPI Table",          "Price · Return\nVol · Drawdown"),
    ("Tile 1",             "Performance Snapshot\nGrouped bar chart"),
    ("Tile 2",             "Price + SMA 20/50\nTime-series line"),
    ("Tile 3",             "Drawdown over time\nFilled area chart"),
]
tile_y = 10.5
for name, desc in tiles:
    tile_y -= 1.5
    small_box(ax, 18.98, tile_y, 2.32, 1.2,
              f"{name}\n{desc}",
              color="#1a0d10", border="#4a2a2a",
              fontsize=7.5, label_color=TEXT)


# ══════════════════════════════════════════════════════════════════════════════
# BOTTOM ROW — Terraform + Tests (x: 0.4 – 13.9, y: 0.4 – 3.8)
# ══════════════════════════════════════════════════════════════════════════════
section_title(ax, 0.45, 4.0, "Infrastructure as Code — Terraform")
outline_box(ax, 0.4, 0.4, 9.7, 3.35, "", color="#1a1000", border="#5a4a00")

tf_resources = [
    ("google_service_account",     "market-pipeline-sa\n+ JSON key file"),
    ("google_project_iam_member",  "objectAdmin · dataEditor\njobUser · metadataViewer"),
    ("google_storage_bucket",      "{project}-market-data-lake\nVersioning · lifecycle"),
    ("google_bigquery_dataset",    "market_analytics\nUS multi-region"),
    ("google_bigquery_table ×2",   "raw_stock_prices\nmart_daily_metrics"),
]

tf_x = 0.55
for res, detail in tf_resources:
    box(ax, tf_x, 0.6, 1.8, 2.85,
        res, detail,
        color="#1a1500", border=ORANGE, label_color=ORANGE, fontsize=7.2)
    tf_x += 1.9

# Tests / CI box
section_title(ax, 10.55, 4.0, "Tests & Quality")
outline_box(ax, 10.5, 0.4, 3.65, 3.35, "", color="#0d1a0d", border="#2a5a2a")

tests = [
    ("pytest (4 tests)",     "DAG import · config\nsymbols · _cast_schema"),
    ("dbt tests (5)",        "not_null · accepted_values\nunique_combo · custom ×2"),
]
tx = 10.65
for name, detail in tests:
    box(ax, tx, 0.6, 1.7, 2.85,
        name, detail,
        color="#0d1a0d", border=APPLE_GREEN, label_color=APPLE_GREEN, fontsize=7.5)
    tx += 1.85


# ══════════════════════════════════════════════════════════════════════════════
# ARROWS — between major components
# ══════════════════════════════════════════════════════════════════════════════

# Alpha Vantage → Airflow Task 1
arrow(ax, 3.0, 11.3, 3.85, 11.3, color=AAPL_BLUE, lw=1.8)
label_arrow(ax, 3.43, 11.55, "JSON", color=AAPL_BLUE)

# Task 2 → GCS raw
arrow(ax, 9.8, 10.45, 10.7, 11.2, color=APPLE_GREEN, lw=1.5)
label_arrow(ax, 10.2, 11.0, ".json", color=APPLE_GREEN)

# Task 4 → GCS curated
arrow(ax, 9.8, 8.95, 10.7, 9.5, color=APPLE_GREEN, lw=1.5)
label_arrow(ax, 10.2, 9.0, ".parquet", color=APPLE_GREEN)

# GCS curated → Task 5 (BQ load)
arrow(ax, 10.5, 8.9, 9.8, 7.85, color=APPLE_GREEN, lw=1.5)

# Task 5 → BQ raw_stock_prices
arrow(ax, 9.8, 7.1, 14.7, 11.2, color="#4a4aaa", lw=1.5)
label_arrow(ax, 12.3, 9.6, "BQ Load Job", color="#8080cc")

# BQ raw → stg (dbt view)
arrow(ax, 16.45, 10.85, 16.45, 10.45, color=PURPLE, lw=1.5)
label_arrow(ax, 17.2, 10.65, "dbt view", color=PURPLE)

# stg → mart (dbt table)
arrow(ax, 16.45, 8.7, 16.45, 8.3, color=PURPLE, lw=1.5)
label_arrow(ax, 17.35, 8.5, "dbt run", color=PURPLE)

# mart → Dashboard
arrow(ax, 18.4, 6.75, 18.95, 8.5, color=TESLA_RED, lw=1.8)
label_arrow(ax, 18.62, 7.85, "BQ\nquery", color=TESLA_RED)

# Terraform → BQ (bottom annotation)
ax.annotate("", xy=(16.45, 2.2), xytext=(16.45, 3.75),
            arrowprops=dict(arrowstyle="-|>", color=ORANGE, lw=1.2,
                            mutation_scale=9, linestyle="dashed"),
            zorder=3)
label_arrow(ax, 17.0, 3.1, "provisions", color=ORANGE)

ax.annotate("", xy=(12.35, 4.8), xytext=(12.35, 3.75),
            arrowprops=dict(arrowstyle="-|>", color=ORANGE, lw=1.2,
                            mutation_scale=9, linestyle="dashed"),
            zorder=3)


# ── Footer ─────────────────────────────────────────────────────────────────────
ax.plot([0.4, 21.6], [0.32, 0.32], color=BORDER, linewidth=0.6, zorder=1)
ax.text(11, 0.18,
        "Scheduled: 0 21 * * 1-5 (21:00 UTC, Mon–Fri)  ·  "
        "Idempotent: compact API always returns last 100 trading days  ·  "
        "Weekend-safe: re-run any day",
        ha="center", va="center", fontsize=7.2, color=SECONDARY,
        fontfamily=FONT, zorder=6)

plt.tight_layout(pad=0.2)
plt.savefig(OUTPUT_PATH, dpi=160, bbox_inches="tight",
            facecolor=BG, edgecolor="none")
print(f"[INFO] Architecture diagram saved to: {OUTPUT_PATH}")
