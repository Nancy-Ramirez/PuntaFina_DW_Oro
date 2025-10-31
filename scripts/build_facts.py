#!/usr/bin/env python3
"""
build_facts.py
Construye hechos y exporta a parquet/csv según config/settings.yaml.
Incluye:
  - fact_ventas_linea    (grano: pedido × línea × SKU × unidad)
  - fact_pago            (grano: transacción de pago)

Incremental:
  - order_lines: usa watermark sobre o.updated_at (tabla de órdenes)
  - payments:    usa watermark sobre t.updated_at (transacciones de pago)

Guarda/lee watermarks en data/_state/<fact>.json
"""

import os
import sys
import json
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import DictCursor
from dotenv import load_dotenv
import yaml

# ---------------------------
# 1) Rutas, logging, config
# ---------------------------
ROOT = Path(__file__).resolve().parents[1]
CONF_DIR = ROOT / "config"

# settings.yaml
with open(CONF_DIR / "settings.yaml", "r", encoding="utf-8") as fh:
    SETTINGS = yaml.safe_load(fh) or {}

EXPORTAR_PARQUET = bool(SETTINGS.get("exportar_parquet", True))
EXPORTAR_CSV     = bool(SETTINGS.get("exportar_csv", False))
OUT_PARQUET_DIR  = ROOT / SETTINGS.get("salida_parquet", "data/outputs/parquet")
OUT_CSV_DIR      = ROOT / SETTINGS.get("salida_csv", "data/outputs/csv")
LOGS_DIR         = ROOT / SETTINGS.get("logs_dir", "logs")
INCR_CFG         = SETTINGS.get("incremental", {}) or {}
SCHEMA_DESTINO   = SETTINGS.get("schema_destino", "dw_granular")
SOURCE_SCHEMAS   = SETTINGS.get("source_schemas", ["public"])

STATE_DIR = ROOT / "data" / "_state"
STATE_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=LOGS_DIR / "build_facts.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
console = logging.StreamHandler(sys.stdout)
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter("%(levelname)s | %(message)s"))
logging.getLogger().addHandler(console)

# .env
load_dotenv(CONF_DIR / ".env")
DB_HOST = os.getenv("ORO_DB_HOST", "localhost")
DB_PORT = int(os.getenv("ORO_DB_PORT", "5432"))
DB_NAME = os.getenv("ORO_DB_NAME", "orocommerce")
DB_USER = os.getenv("ORO_DB_USER", "postgres")
DB_PASS = os.getenv("ORO_DB_PASS", "changeme")

# ---------------------------
# 2) Utilidades
# ---------------------------
def open_connection():
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )
    conn.autocommit = True
    return conn

def set_search_path(cur, schema_destino, source_schemas):
    schemas = [schema_destino] + list(source_schemas)
    cur.execute(f"SET search_path = {', '.join(schemas)};")

def to_json_string(x):
    if isinstance(x, (dict, list, tuple)):
        try:
            return json.dumps(x, ensure_ascii=False)
        except Exception:
            return str(x)
    return x

def sanitize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        if df[col].dtype == "object":
            sample = df[col].dropna().head(10).tolist()
            if any(isinstance(v, (dict, list, tuple)) for v in sample):
                df[col] = df[col].map(to_json_string)
    return df

def export_table(df: pd.DataFrame, name: str):
    if df is None or df.empty:
        logging.warning(f"{name}: sin filas (no se exporta).")
        return
    df = sanitize_dataframe(df)
    if EXPORTAR_PARQUET:
        out_dir = OUT_PARQUET_DIR / name
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "part-00000.parquet").unlink(missing_ok=True)
        df.to_parquet(out_dir / "part-00000.parquet", index=False)
        logging.info(f"{name}: parquet -> {out_dir / 'part-00000.parquet'}")
    if EXPORTAR_CSV:
        OUT_CSV_DIR.mkdir(parents=True, exist_ok=True)
        out_csv = OUT_CSV_DIR / f"{name}.csv"
        df.to_csv(out_csv, index=False)
        logging.info(f"{name}: csv -> {out_csv}")

def read_sql_to_df(conn, query: str, params=None) -> pd.DataFrame:
    return pd.read_sql(query, conn, params=params)

def _state_path(fact_name: str) -> Path:
    return STATE_DIR / f"{fact_name}.json"

def read_watermark(fact_name: str) -> str | None:
    p = _state_path(fact_name)
    if not p.exists():
        return None
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        return data.get("watermark")
    except Exception:
        return None

def write_watermark(fact_name: str, wm: str):
    p = _state_path(fact_name)
    p.write_text(json.dumps({"watermark": wm}, ensure_ascii=False, indent=2), encoding="utf-8")

# ---------------------------
# 3) Queries (con soporte incremental)
# ---------------------------
# Ventas por línea: join por PK de orden = o.id
Q_FACT_VENTAS_LINEA_BASE = """
SELECT
    COALESCE(o.id, li.order_id)           AS order_id,
    li.line_item_id,
    li.product_id,
    li.sku,
    li.product_unit,
    li.quantity,
    li.price                                                AS precio_unitario,
    COALESCE(li.row_total, (li.quantity * li.price))        AS subtotal_linea,
    li.discount_amount                                      AS descuento_linea,
    li.tax_amount                                           AS impuestos_linea,
    COALESCE(o.currency, li.currency)                       AS currency_code,
    o.created_at                                            AS fecha_creacion_pedido,
    o.updated_at                                            AS fecha_actualizacion_pedido,
    o.website_id,
    o.customer_id,
    o.user_owner_id                                         AS usuario_owner_id,
    COALESCE(o.internal_status_name, 'Sin estado')          AS estado_pedido
FROM
    oro_order_line_item_granular li
LEFT JOIN
    oro_order_granular o
      ON o.id = li.order_id
{where_clause}
ORDER BY COALESCE(o.updated_at, o.created_at, '1900-01-01'::timestamp);
"""


# Pagos: una fila por transacción; usamos updated_at para incremental
Q_FACT_PAGO_BASE = """
SELECT
    t.transaction_id,
    t.order_id,
    t.amount            AS monto,
    t.currency          AS currency_code,
    t.action            AS tipo_accion,
    t.payment_method,
    t.status            AS status_raw,
    t.successful        AS es_exitoso,
    t.created_at,
    t.updated_at
FROM
    oro_payment_transaction_granular t
{where_clause}
ORDER BY COALESCE(t.updated_at, t.created_at, '1900-01-01'::timestamp);
"""

def build_where_incremental(fact: str, column: str, incr_enabled: bool, last_wm: str | None):
    """
    Retorna (where_clause, params, incr_used)
    """
    if incr_enabled and last_wm:
        where_clause = f"WHERE {column} > %(wm)s"
        params = {"wm": last_wm}
        return where_clause, params, True
    elif incr_enabled and not last_wm:
        # primera corrida incremental → full load (sin where), guardará watermark
        return "", None, False
    else:
        # incremental deshabilitado
        return "", None, False

def max_timestamp(df: pd.DataFrame, cols: list[str]) -> str | None:
    for c in cols:
        if c in df.columns and not df[c].isna().all():
            try:
                mx = pd.to_datetime(df[c]).max()
                if pd.isna(mx):
                    continue
                # ISO sin microsegundos para robustez
                return mx.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                continue
    return None

# ---------------------------
# 4) Main
# ---------------------------
def main():
    logging.info(f"Destino: {DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    logging.info(f"Schema destino: {SCHEMA_DESTINO} | Sources: {', '.join(SOURCE_SCHEMAS)}")
    logging.info(f"Exportar parquet={EXPORTAR_PARQUET} csv={EXPORTAR_CSV}")
    logging.info(f"Incremental: {INCR_CFG}")

    conn = None
    try:
        conn = open_connection()
        with conn.cursor(cursor_factory=DictCursor) as cur:
            set_search_path(cur, SCHEMA_DESTINO, SOURCE_SCHEMAS)

        # -------- fact_ventas_linea --------
        fact_name = "fact_ventas_linea"
        incr_enabled = bool(INCR_CFG.get("order_lines", False))
        last_wm = read_watermark(fact_name)
        where_clause, params, used_incr = build_where_incremental(
            fact_name, "o.updated_at", incr_enabled, last_wm
        )
        q = Q_FACT_VENTAS_LINEA_BASE.format(where_clause=where_clause)
        logging.info(f"[FACT] {fact_name} | incr={incr_enabled} | last_wm={last_wm}")
        try:
            df_li = read_sql_to_df(conn, q, params=params)
            logging.info(f"  Filas: {len(df_li)} | Columnas: {len(df_li.columns)}")
            export_table(df_li, fact_name)
            # watermark = max de updated_at (o created_at si updated_at vacío)
            wm = max_timestamp(df_li, ["fecha_actualizacion_pedido", "fecha_creacion_pedido"])
            if wm and incr_enabled:
                write_watermark(fact_name, wm)
                logging.info(f"  Nuevo watermark {fact_name}: {wm}")
        except Exception as e:
            logging.error(f"  Error en {fact_name}: {e}")

        # -------- fact_pago --------
        fact_name = "fact_pago"
        incr_enabled = bool(INCR_CFG.get("payments", False))
        last_wm = read_watermark(fact_name)
        where_clause, params, used_incr = build_where_incremental(
            fact_name, "COALESCE(t.updated_at, t.created_at)", incr_enabled, last_wm
        )
        q = Q_FACT_PAGO_BASE.format(where_clause=where_clause)
        logging.info(f"[FACT] {fact_name} | incr={incr_enabled} | last_wm={last_wm}")
        try:
            df_pay = read_sql_to_df(conn, q, params=params)
            logging.info(f"  Filas: {len(df_pay)} | Columnas: {len(df_pay.columns)}")
            export_table(df_pay, fact_name)
            wm = max_timestamp(df_pay, ["updated_at", "created_at"])
            if wm and incr_enabled:
                write_watermark(fact_name, wm)
                logging.info(f"  Nuevo watermark {fact_name}: {wm}")
        except Exception as e:
            logging.error(f"  Error en {fact_name}: {e}")

        logging.info("Hechos exportados.")
    finally:
        if conn is not None:
            conn.close()

if __name__ == "__main__":
    main()
