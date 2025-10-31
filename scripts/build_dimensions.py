#!/usr/bin/env python3
"""
build_dimensions.py
Materializa dimensiones simples desde las vistas granulares en Postgres
y las exporta a data/outputs/ (parquet y/o csv) según config/settings.yaml.

Requisitos:
  pip install psycopg2-binary python-dotenv pyyaml pandas pyarrow

Comportamiento:
  - Conecta a Postgres y fija search_path = dw_granular, public
  - Lee cada vista granular indicada en DIM_SOURCES
  - Convierte columnas complejas (dict/list/tuple) a JSON string antes de exportar
  - Escribe a:
      data/outputs/parquet/<nombre_dim>/
      data/outputs/csv/<nombre_dim>.csv  (si exportar_csv=true)
"""

import os
import sys
import json
import logging
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
OUT_PARQUET_DIR = ROOT / "data" / "outputs" / "parquet"
OUT_CSV_DIR = ROOT / "data" / "outputs" / "csv"
LOGS_DIR = ROOT / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=LOGS_DIR / "build_dimensions.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
console = logging.StreamHandler(sys.stdout)
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter("%(levelname)s | %(message)s"))
logging.getLogger().addHandler(console)

# Cargar .env
load_dotenv(CONF_DIR / ".env")

# Cargar settings.yaml
settings_path = CONF_DIR / "settings.yaml"
settings = {}
if settings_path.exists():
    with open(settings_path, "r", encoding="utf-8") as fh:
        settings = yaml.safe_load(fh) or {}

EXPORTAR_PARQUET = bool(settings.get("exportar_parquet", True))
EXPORTAR_CSV = bool(settings.get("exportar_csv", False))
SCHEMA_DESTINO = settings.get("schema_destino", "dw_granular")
SOURCE_SCHEMAS = settings.get("source_schemas", ["public"])

DB_HOST = os.getenv("ORO_DB_HOST", "localhost")
DB_PORT = int(os.getenv("ORO_DB_PORT", "5432"))
DB_NAME = os.getenv("ORO_DB_NAME", "orocommerce")
DB_USER = os.getenv("ORO_DB_USER", "postgres")
DB_PASS = os.getenv("ORO_DB_PASS", "changeme")

# ---------------------------
# 2) Catálogo de dimensiones
# ---------------------------
# Ajustado: usamos 'oro_product_granular' (sin prefijo 'vw_')
DIM_SOURCES = {
    # producto / catálogo
    "dim_producto": "oro_product_granular",
    # clientes
    "dim_cliente": "oro_customer_granular",
    # usuario interno
    "dim_usuario": "oro_user_granular",
    # sitio y canal
    "dim_sitio_web": "oro_website_granular",
    "dim_canal": "orocrm_channel_granular",
    # precio y promo
    "dim_precio_lista": "oro_price_list_granular",
    "dim_promocion": "oro_promotion_granular",
    # pago (estatus como dim separada)
    "dim_pago_status": "oro_payment_status_granular",
    # si luego agregas método de pago como catálogo, añádelo aquí:
    # "dim_pago": "oro_payment_method_granular",
    # direcciones (si quieres exportarlas aparte):
    # "dim_direccion": "oro_address_granular",
}

# Si quieres excluir columnas problemáticas (ej. JSONB sin estructura), lista aquí por dimensión:
DROP_COLUMNS = {
    # "dim_sitio_web": ["serialized_data"],
    # "dim_promocion": ["serialized_data"],
}

# ---------------------------
# 3) Utilidades
# ---------------------------
def open_connection():
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )
    conn.autocommit = True
    return conn

def set_search_path(cur, schema_destino, source_schemas):
    schemas = [schema_destino] + list(source_schemas)
    safe = ", ".join(schemas)  # simple
    cur.execute(f"SET search_path = {safe};")

def fetch_dataframe(conn, view_name: str) -> pd.DataFrame:
    with conn.cursor(cursor_factory=DictCursor) as cur:
        set_search_path(cur, SCHEMA_DESTINO, SOURCE_SCHEMAS)
        query = f"SELECT * FROM {view_name};"
        logging.info(f"   Query: {query}")
        # pandas con psycopg2 lanza un warning, pero funciona
        df = pd.read_sql(query, conn)
        return df

def to_json_string(x):
    """Convierte objetos no escalares (dict/list/tuple) a JSON string; deja escalares tal cual."""
    if isinstance(x, (dict, list, tuple)):
        try:
            return json.dumps(x, ensure_ascii=False)
        except Exception:
            return str(x)
    return x

def sanitize_dataframe(df: pd.DataFrame, dim_name: str) -> pd.DataFrame:
    # 1) Eliminar columnas listadas explícitamente
    cols_to_drop = DROP_COLUMNS.get(dim_name, [])
    df = df.drop(columns=[c for c in cols_to_drop if c in df.columns], errors="ignore")

    # 2) Convertir a texto JSON cualquier columna object con dict/list/tuple
    for col in df.columns:
        if df[col].dtype == "object":
            sample = df[col].dropna().head(10).tolist()
            if any(isinstance(v, (dict, list, tuple)) for v in sample):
                df[col] = df[col].map(to_json_string)
    return df

def export_dimension(df: pd.DataFrame, dim_name: str):
    if df is None or df.empty:
        logging.warning(f"   {dim_name}: sin filas (no se exporta).")
        return

    df = sanitize_dataframe(df, dim_name)

    if EXPORTAR_PARQUET:
        out_dir = OUT_PARQUET_DIR / dim_name
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "part-00000.parquet"
        df.to_parquet(out_path, index=False)
        logging.info(f"   {dim_name}: parquet -> {out_path}")

    if EXPORTAR_CSV:
        OUT_CSV_DIR.mkdir(parents=True, exist_ok=True)
        out_csv = OUT_CSV_DIR / f"{dim_name}.csv"
        df.to_csv(out_csv, index=False)
        logging.info(f"   {dim_name}: csv -> {out_csv}")

# ---------------------------
# 4) Main
# ---------------------------
def main():
    logging.info(f"Destino: {DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    logging.info(f"Schema destino: {SCHEMA_DESTINO} | Sources: {', '.join(SOURCE_SCHEMAS)}")
    logging.info(f"Exportar parquet={EXPORTAR_PARQUET} csv={EXPORTAR_CSV}")

    conn = None
    try:
        conn = open_connection()
        ok = 0
        for dim_name, view_name in DIM_SOURCES.items():
            logging.info(f"[DIM] {dim_name} <- {view_name}")
            try:
                df = fetch_dataframe(conn, view_name)
                logging.info(f"   Filas: {len(df)} | Columnas: {len(df.columns)}")
                export_dimension(df, dim_name)
                ok += 1
            except Exception as e:
                logging.error(f"   Error en {dim_name}: {e}")
                continue
        logging.info(f"Dimensiones exportadas: {ok}/{len(DIM_SOURCES)}")
    finally:
        if conn is not None:
            conn.close()

if __name__ == "__main__":
    main()
