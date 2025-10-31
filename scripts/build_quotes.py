#!/usr/bin/env python3
"""
build_quotes.py
Construye hechos de cotizaciones desde las tablas Oro:
  - oro_sale_quote (cabecera)
  - oro_sale_quote_product (líneas)
  - oro_sale_quote_prod_request (cantidades, opcional)
  - oro_sale_quote_prod_offer (precio, opcional)

Hechos:
  - fact_cotizacion (grano: 1 x quote)
  - fact_cotizacion_linea (grano: quote x product x unit; con qty/price si existen)

Incremental:
  - Basado en COALESCE(updated_at, created_at).
  - Lee/escribe watermark en data/_state/<fact>.json
  - Clave en settings.yaml: incremental.quotes (si no existe, se asume True)

Requisitos:
  pip install psycopg2-binary python-dotenv pyyaml pandas pyarrow
"""

import os
import sys
import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import psycopg2
from psycopg2.extras import DictCursor
from dotenv import load_dotenv
import yaml

# ---------------------------
# 1) Paths, settings, logging
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
INCR_CFG         = SETTINGS.get("incremental", {}) or {}
INCR_QUOTES      = bool(INCR_CFG.get("quotes", True))

STATE_DIR = ROOT / "data" / "_state"
STATE_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=LOGS_DIR / "build_quotes.log",
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
# 2) DB utils
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

def table_exists(conn, schema: str, table: str) -> bool:
    q = """
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema = %s AND table_name = %s
    """
    with conn.cursor() as cur:
        cur.execute(q, (schema, table))
        return cur.fetchone() is not None

def list_columns(conn, schema: str, table: str) -> List[str]:
    q = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = %s AND table_name = %s
    ORDER BY ordinal_position
    """
    with conn.cursor() as cur:
        cur.execute(q, (schema, table))
        return [r[0] for r in cur.fetchall()]

def read_sql_to_df(conn, query: str, params=None) -> pd.DataFrame:
    # pandas puede advertir, pero funciona con psycopg2
    return pd.read_sql(query, conn, params=params)

# ---------------------------
# 3) Export helpers
# ---------------------------
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

def _make_unique_columns(cols):
    seen = {}
    out = []
    for c in cols:
        if c not in seen:
            seen[c] = 0
            out.append(c)
        else:
            seen[c] += 1
            out.append(f"{c}__{seen[c]}")
    return out


def export_table(df: pd.DataFrame, name: str):
    if df is None or df.empty:
        logging.warning(f"{name}: sin filas (no se exporta).")
        return

    df = df.copy()
    df.columns = _make_unique_columns(list(df.columns))

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

def _state_path(fact_name: str) -> Path:
    return STATE_DIR / f"{fact_name}.json"

def read_watermark(fact_name: str) -> Optional[str]:
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

def max_timestamp(df: pd.DataFrame, cols: List[str]) -> Optional[str]:
    for c in cols:
        if c in df.columns and not df[c].isna().all():
            try:
                mx = pd.to_datetime(df[c]).max()
                if pd.isna(mx):
                    continue
                return mx.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                continue
    return None

# ---------------------------
# 4) Fact: Cabecera (fact_cotizacion)
# ---------------------------
def build_query_fact_cotizacion(conn) -> str:
    # Trabajamos en public por defecto (las tablas de Oro están ahí)
    schema = "public"
    base = "oro_sale_quote"
    if not table_exists(conn, schema, base):
        raise RuntimeError("No existe public.oro_sale_quote")

    cols = set(list_columns(conn, schema, base))

    def pick(*cands):
        for c in cands:
            if c in cols:
                return c
        return None

    # Mapeo alias -> columna existente o NULL
    c_id       = pick("id")
    c_cust     = pick("customer_id")
    c_custusr  = pick("customer_user_id")
    c_web      = pick("website_id")
    c_po       = pick("po_number")
    c_curr     = pick("currency", "base_currency", "subtotal_currency", "total_currency")
    c_subtot   = pick("subtotal", "subtotal_value", "base_subtotal_value")
    c_total    = pick("total", "total_value", "base_total_value")
    c_created  = pick("created_at", "createdat")
    c_updated  = pick("updated_at", "updatedat")
    c_status   = pick("internal_status_name", "status", "status_name")

    # Armamos SELECT dinámico
    sel = []
    sel.append(f"{c_id} AS quote_id" if c_id else "NULL::bigint AS quote_id")
    sel.append(f"{c_cust} AS customer_id" if c_cust else "NULL::bigint AS customer_id")
    sel.append(f"{c_custusr} AS customer_user_id" if c_custusr else "NULL::bigint AS customer_user_id")
    sel.append(f"{c_web} AS website_id" if c_web else "NULL::bigint AS website_id")
    sel.append(f"{c_po} AS po_number" if c_po else "NULL::text AS po_number")
    sel.append(f"{c_curr} AS currency_code" if c_curr else "NULL::text AS currency_code")
    sel.append(f"{c_subtot} AS subtotal" if c_subtot else "NULL::numeric AS subtotal")
    sel.append(f"{c_total} AS total" if c_total else "NULL::numeric AS total")
    sel.append(f"{c_created} AS created_at" if c_created else "NULL::timestamp AS created_at")
    sel.append(f"{c_updated} AS updated_at" if c_updated else "NULL::timestamp AS updated_at")
    sel.append(f"COALESCE({c_status}, 'Sin estado') AS estado" if c_status else "'Sin estado'::text AS estado")

    order_expr = c_updated or c_created or c_id

    where_clause = ""  # se inyecta luego
    q = f"""
    SELECT
      {", ".join(sel)}
    FROM {schema}.{base}
    {{where_clause}}
    ORDER BY {order_expr} NULLS LAST;
    """
    return q

# ---------------------------
# 5) Fact: Líneas (fact_cotizacion_linea)
# ---------------------------
def build_query_fact_cotizacion_linea(conn) -> str:
    schema = "public"

    # Tablas base necesarias
    if not table_exists(conn, schema, "oro_sale_quote_product"):
        raise RuntimeError("No existe public.oro_sale_quote_product")

    # Columnas disponibles en product
    cols_qp = set(list_columns(conn, schema, "oro_sale_quote_product"))

    def has_table(t): return table_exists(conn, schema, t)
    def pick(cols_set, *cands):
        for c in cands:
            if c in cols_set:
                return c
        return None

    qp_id      = pick(cols_qp, "id")
    qp_quote   = pick(cols_qp, "quote_id")
    qp_prod    = pick(cols_qp, "product_id")
    qp_sku     = pick(cols_qp, "product_sku", "sku")
    qp_unit    = pick(cols_qp, "product_unit_code", "unit_code", "product_unit")

    # request/offer son opcionales
    has_req = has_table("oro_sale_quote_prod_request")
    has_off = has_table("oro_sale_quote_prod_offer")

    # Armamos SELECT base de qp
    base_sel = []
    base_sel.append(f"qp.{qp_quote} AS quote_id" if qp_quote else "NULL::bigint AS quote_id")
    base_sel.append(f"qp.{qp_id} AS quote_product_id" if qp_id else "ROW_NUMBER() OVER()::bigint AS quote_product_id")
    base_sel.append(f"qp.{qp_prod} AS product_id" if qp_prod else "NULL::bigint AS product_id")
    base_sel.append(f"qp.{qp_sku} AS sku" if qp_sku else "NULL::text AS sku")
    base_sel.append(f"qp.{qp_unit} AS product_unit" if qp_unit else "NULL::text AS product_unit")

    joins = []
    extra_sel = []

    # Si existen las tablas de request/offer, traemos qty/price/currency
    if has_req:
        cols_req = set(list_columns(conn, schema, "oro_sale_quote_prod_request"))
        req_id   = pick(cols_req, "id")
        req_qp   = pick(cols_req, "quote_product_id")
        req_qty  = pick(cols_req, "quantity", "qty")
        req_unit = pick(cols_req, "product_unit_code", "unit_code", "product_unit")

        # Subselect para tomar UNA request por quote_product (la de id mínimo)
        if req_qp and req_id:
            joins.append(f"""
            LEFT JOIN (
              SELECT r1.*
              FROM {schema}.oro_sale_quote_prod_request r1
              JOIN (
                SELECT {req_qp} AS quote_product_id, MIN({req_id}) AS min_id
                FROM {schema}.oro_sale_quote_prod_request
                GROUP BY {req_qp}
              ) rmin
                ON rmin.quote_product_id = r1.{req_qp} AND rmin.min_id = r1.{req_id}
            ) req ON req.{req_qp} = qp.{qp_id}
            """)
            extra_sel.append(f"req.{req_id} AS request_id")
            extra_sel.append(f"req.{req_qty} AS quantity") if req_qty else extra_sel.append("NULL::numeric AS quantity")
            # si unit viene en request y no en qp, úsalo como fallback
            if req_unit and not qp_unit:
                extra_sel.append(f"req.{req_unit} AS product_unit")
        else:
            extra_sel.append("NULL::bigint AS request_id")
            extra_sel.append("NULL::numeric AS quantity")
    else:
        extra_sel.append("NULL::bigint AS request_id")
        extra_sel.append("NULL::numeric AS quantity")

    if has_off:
        cols_off = set(list_columns(conn, schema, "oro_sale_quote_prod_offer"))
        off_id   = pick(cols_off, "id")
        off_req  = pick(cols_off, "quote_product_request_id")
        off_price= pick(cols_off, "price", "value", "amount")
        off_curr = pick(cols_off, "currency", "price_currency")

        # Subselect para tomar UNA offer por request (la de id mínimo)
        if off_req and off_id:
            joins.append(f"""
            LEFT JOIN (
              SELECT o1.*
              FROM {schema}.oro_sale_quote_prod_offer o1
              JOIN (
                SELECT {off_req} AS quote_product_request_id, MIN({off_id}) AS min_id
                FROM {schema}.oro_sale_quote_prod_offer
                GROUP BY {off_req}
              ) omin
                ON omin.quote_product_request_id = o1.{off_req} AND omin.min_id = o1.{off_id}
            ) off ON off.{off_req} = req.{req_id}
            """)
            extra_sel.append(f"off.{off_id} AS offer_id")
            extra_sel.append(f"off.{off_price} AS price") if off_price else extra_sel.append("NULL::numeric AS price")
            extra_sel.append(f"off.{off_curr} AS currency_code") if off_curr else extra_sel.append("NULL::text AS currency_code")
        else:
            extra_sel.append("NULL::bigint AS offer_id")
            extra_sel.append("NULL::numeric AS price")
            extra_sel.append("NULL::text AS currency_code")
    else:
        extra_sel.append("NULL::bigint AS offer_id")
        extra_sel.append("NULL::numeric AS price")
        extra_sel.append("NULL::text AS currency_code")

    sel = base_sel + extra_sel

    # Incremental por timestamp de la cabecera (si existe oro_sale_quote)
    where_clause = ""
    if table_exists(conn, schema, "oro_sale_quote"):
        cols_sq = set(list_columns(conn, schema, "oro_sale_quote"))
        c_created = "created_at" if "created_at" in cols_sq else None
        c_updated = "updated_at" if "updated_at" in cols_sq else None
        ts_expr = None
        if c_updated or c_created:
            ts_expr = f"COALESCE(sq.{c_updated or c_created}, sq.{c_created or c_updated})"
            joins.append(f"LEFT JOIN {schema}.oro_sale_quote sq ON sq.id = qp.{qp_quote or 'quote_id'}")
            where_clause = f"{{where_clause_ts}}"
            order_expr = ts_expr
        else:
            order_expr = f"qp.{qp_id or 'id'}"
    else:
        order_expr = f"qp.{qp_id or 'id'}"

    q = f"""
    SELECT
      {", ".join(s for s in sel)}
    FROM {schema}.oro_sale_quote_product qp
    {' '.join(joins)}
    {where_clause}
    ORDER BY {order_expr} NULLS LAST;
    """
    return q

# ---------------------------
# 6) Incremental helper
# ---------------------------
def build_where_incremental(column_sql: str, incr_enabled: bool, last_wm: Optional[str]):
    if incr_enabled and last_wm:
        return f"WHERE {column_sql} > %(wm)s", {"wm": last_wm}, True
    elif incr_enabled and not last_wm:
        # primera corrida = full load
        return "", None, False
    else:
        return "", None, False

# ---------------------------
# 7) Main
# ---------------------------
def main():
    logging.info(f"Destino: {DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    logging.info(f"Schema destino: {SCHEMA_DESTINO} | Sources: {', '.join(SOURCE_SCHEMAS)}")
    logging.info(f"Exportar parquet={EXPORTAR_PARQUET} csv={EXPORTAR_CSV}")
    logging.info(f"Incremental quotes={INCR_QUOTES}")

    conn = None
    try:
        conn = open_connection()
        with conn.cursor(cursor_factory=DictCursor) as cur:
            set_search_path(cur, SCHEMA_DESTINO, SOURCE_SCHEMAS)

        # -------- fact_cotizacion (cabecera) --------
        fact_name = "fact_cotizacion"
        wm_path = _state_path(fact_name)
        last_wm = read_watermark(fact_name)
        q_head = build_query_fact_cotizacion(conn)

        # Descubrimos qué columna timestamp usamos para el WHERE
        # (en build_query se ordena por updated_at/created_at/id; para WHERE, probamos ambas)
        # Usaremos COALESCE(updated_at, created_at)
        where_clause, params, _ = build_where_incremental(
            "COALESCE(updated_at, created_at)", INCR_QUOTES, last_wm
        )
        q_head = q_head.format(where_clause=where_clause)

        logging.info(f"[FACT] {fact_name} | incr={INCR_QUOTES} | last_wm={last_wm}")
        try:
            df_head = read_sql_to_df(conn, q_head, params=params)
            logging.info(f"  Filas: {len(df_head)} | Columnas: {len(df_head.columns)}")
            export_table(df_head, fact_name)
            wm = max_timestamp(df_head, ["updated_at", "created_at"])
            if wm and INCR_QUOTES:
                write_watermark(fact_name, wm)
                logging.info(f"  Nuevo watermark {fact_name}: {wm}")
        except Exception as e:
            logging.error(f"  Error en {fact_name}: {e}")

        # -------- fact_cotizacion_linea (líneas) --------
        fact_name = "fact_cotizacion_linea"
        last_wm = read_watermark(fact_name)
        q_lines = build_query_fact_cotizacion_linea(conn)

        # WHERE incremental por timestamp de la cabecera unida como sq (si existía)
        q_lines_effective = q_lines
        params = None
        if "{where_clause_ts}" in q_lines:
            where_clause, params, _ = build_where_incremental(
                "COALESCE(sq.updated_at, sq.created_at)", INCR_QUOTES, last_wm
            )
            q_lines_effective = q_lines.replace("{where_clause_ts}", where_clause)

        logging.info(f"[FACT] {fact_name} | incr={INCR_QUOTES} | last_wm={last_wm}")
        try:
            df_lines = read_sql_to_df(conn, q_lines_effective, params=params)
            logging.info(f"  Filas: {len(df_lines)} | Columnas: {len(df_lines.columns)}")
            export_table(df_lines, fact_name)
            wm = max_timestamp(df_lines, ["sq.updated_at", "sq.created_at", "updated_at", "created_at"])
            if wm and INCR_QUOTES:
                write_watermark(fact_name, wm)
                logging.info(f"  Nuevo watermark {fact_name}: {wm}")
        except Exception as e:
            logging.error(f"  Error en {fact_name}: {e}")

        logging.info("Hechos de cotización exportados.")
    finally:
        if conn is not None:
            conn.close()

if __name__ == "__main__":
    main()
