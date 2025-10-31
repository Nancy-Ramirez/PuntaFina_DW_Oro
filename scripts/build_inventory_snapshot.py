#!/usr/bin/env python3
"""
build_inventory_snapshot.py
Crea el hecho 'fact_inventario_snapshot_diario' tomando un snapshot de inventario
desde la vista granular 'oro_inventory_level_granular'.

- No asume nombres exactos de columnas: detecta dinámicamente product_id, warehouse_id,
  cantidad disponible/total/reservada si existen en la vista.
- Exporta a Parquet/CSV según config/settings.yaml.
- Admite parámetro --fecha (YYYY-MM-DD); por defecto usa hoy.

Requisitos:
  pip install psycopg2-binary python-dotenv pyyaml pandas pyarrow
"""

import os
import sys
import json
import logging
import argparse
from datetime import datetime, date
from pathlib import Path
from typing import Optional

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

with open(CONF_DIR / "settings.yaml", "r", encoding="utf-8") as fh:
    SETTINGS = yaml.safe_load(fh) or {}

EXPORTAR_PARQUET = bool(SETTINGS.get("exportar_parquet", True))
EXPORTAR_CSV     = bool(SETTINGS.get("exportar_csv", False))
OUT_PARQUET_DIR  = ROOT / SETTINGS.get("salida_parquet", "data/outputs/parquet")
OUT_CSV_DIR      = ROOT / SETTINGS.get("salida_csv", "data/outputs/csv")
LOGS_DIR         = ROOT / SETTINGS.get("logs_dir", "logs")
SCHEMA_DESTINO   = SETTINGS.get("schema_destino", "dw_granular")
SOURCE_SCHEMAS   = SETTINGS.get("source_schemas", ["public"])

LOGS_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=LOGS_DIR / "build_inventory_snapshot.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
console = logging.StreamHandler(sys.stdout)
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter("%(levelname)s | %(message)s"))
logging.getLogger().addHandler(console)

# .env
load_dotenv(CONF_DIR / ".env")
DB_HOST = os.getenv("ORO_DB_HOST")
DB_PORT = int(os.getenv("ORO_DB_PORT"))
DB_NAME = os.getenv("ORO_DB_NAME")
DB_USER = os.getenv("ORO_DB_USER")
DB_PASS = os.getenv("ORO_DB_PASS")

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

def read_full_inventory(conn) -> pd.DataFrame:
    # Leemos todas las columnas para poder mapear dinámicamente
    q = "SELECT * FROM oro_inventory_level_granular;"
    return pd.read_sql(q, conn)

def pick(colnames, candidates):
    """Devuelve el primer nombre de columna que exista en colnames."""
    for c in candidates:
        if c in colnames:
            return c
    return None

def to_numeric_safe(series: Optional[pd.Series]) -> Optional[pd.Series]:
    if series is None:
        return None
    try:
        return pd.to_numeric(series, errors="coerce")
    except Exception:
        return series

def sanitize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    # Convierte objetos que son dict/list/tuple a string JSON
    def to_json_string(x):
        if isinstance(x, (dict, list, tuple)):
            try:
                return json.dumps(x, ensure_ascii=False)
            except Exception:
                return str(x)
        return x

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

# ---------------------------
# 3) Transformación estándar
# ---------------------------
def build_snapshot(df_src: pd.DataFrame, fecha_snapshot: date) -> pd.DataFrame:
    cols = [c for c in df_src.columns]

    # Detectar columnas clave
    product_col   = pick(cols, ["product_id", "product", "productid", "producto_id"])
    warehouse_col = pick(cols, ["warehouse_id", "warehouse", "warehouseid", "almacen_id", "source_warehouse_id"])

    # Cantidades
    # disponibles / on-hand
    disp_col = pick(cols, [
        "stock_disponible", "available_qty", "available_quantity", "on_hand",
        "onhand", "quantity", "qty", "inventory_qty"
    ])
    # total
    total_col = pick(cols, [
        "stock_total", "total_qty", "total_quantity", "cantidad_total"
    ])
    # reservada/asignada
    resv_col = pick(cols, [
        "stock_reservado", "reserved_qty", "reserved_quantity", "allocated_qty", "asignado"
    ])

    # Unidades (opcional)
    unit_col = pick(cols, ["product_unit", "unit", "unidad", "product_unit_code"])

    # Crear frame estandarizado
    out = pd.DataFrame()
    out["fecha_snapshot"] = pd.to_datetime(str(fecha_snapshot)).date()

    # product_id obligatorio
    if product_col is None:
        raise ValueError("La vista 'oro_inventory_level_granular' no expone 'product_id' (o equivalente).")
    out["product_id"] = df_src[product_col]

    # warehouse opcional (si no hay multi-almacén, quedará nulo)
    out["almacen_id"] = df_src[warehouse_col] if warehouse_col else None

    # cantidades numéricas robustas
    disp = to_numeric_safe(df_src.get(disp_col)) if disp_col else None
    total = to_numeric_safe(df_src.get(total_col)) if total_col else None
    resv = to_numeric_safe(df_src.get(resv_col)) if resv_col else None

    # Derivar estándar:
    # stock_disponible = disponible si existe; si no, total - reservado; si no, nulo
    if disp is not None:
        out["stock_disponible"] = disp
    elif (total is not None) and (resv is not None):
        out["stock_disponible"] = total - resv
    else:
        out["stock_disponible"] = None

    # stock_reservado = reservado si existe; si no, total - disponible (si ambos)
    if resv is not None:
        out["stock_reservado"] = resv
    elif (total is not None) and ("stock_disponible" in out.columns):
        try:
            out["stock_reservado"] = total - out["stock_disponible"]
        except Exception:
            out["stock_reservado"] = None
    else:
        out["stock_reservado"] = None

    # stock_total = total si existe; si no, disponible + reservado (si ambos)
    if total is not None:
        out["stock_total"] = total
    elif ("stock_disponible" in out.columns) and ("stock_reservado" in out.columns):
        try:
            out["stock_total"] = pd.to_numeric(out["stock_disponible"], errors="coerce") + \
                                 pd.to_numeric(out["stock_reservado"], errors="coerce")
        except Exception:
            out["stock_total"] = None
    else:
        out["stock_total"] = None

    # unidad (opcional)
    out["unidad"] = df_src[unit_col] if unit_col else None

    # Ordenar columnas
    ordered = ["fecha_snapshot", "product_id", "almacen_id",
               "stock_disponible", "stock_reservado", "stock_total", "unidad"]
    # Añade columnas extra útiles del origen (por trazabilidad), si existen:
    extras = [c for c in cols if c not in set(ordered)]
    df_final = pd.concat([out[ordered], df_src[extras]], axis=1)

    return df_final

# ---------------------------
# 4) Main
# ---------------------------
def main():
    # CLI: --fecha YYYY-MM-DD
    parser = argparse.ArgumentParser(description="Snapshot diario de inventario desde oro_inventory_level_granular")
    parser.add_argument("--fecha", type=str, default=None, help="Fecha del snapshot (YYYY-MM-DD). Por defecto: hoy.")
    args = parser.parse_args()

    if args.fecha:
        try:
            fecha_snapshot = datetime.strptime(args.fecha, "%Y-%m-%d").date()
        except ValueError:
            raise SystemExit("Formato de --fecha inválido. Usa YYYY-MM-DD.")
    else:
        fecha_snapshot = date.today()

    logging.info(f"Destino: {DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    logging.info(f"Schema destino: {SCHEMA_DESTINO} | Sources: {', '.join(SOURCE_SCHEMAS)}")
    logging.info(f"Exportar parquet={EXPORTAR_PARQUET} csv={EXPORTAR_CSV}")
    logging.info(f"Snapshot de inventario para fecha: {fecha_snapshot}")

    conn = None
    try:
        conn = open_connection()
        with conn.cursor(cursor_factory=DictCursor) as cur:
            set_search_path(cur, SCHEMA_DESTINO, SOURCE_SCHEMAS)

        df_src = read_full_inventory(conn)
        logging.info(f"  Fuente: oro_inventory_level_granular -> filas={len(df_src)} cols={len(df_src.columns)}")

        if df_src.empty:
            logging.warning("  Sin datos de inventario en la vista. No se exporta snapshot.")
            return

        df_snap = build_snapshot(df_src, fecha_snapshot)
        logging.info(f"  fact_inventario_snapshot_diario: filas={len(df_snap)} cols={len(df_snap.columns)}")

        export_table(df_snap, "fact_inventario_snapshot_diario")
        logging.info("Inventario exportado.")
    finally:
        if conn is not None:
            conn.close()

if __name__ == "__main__":
    main()
