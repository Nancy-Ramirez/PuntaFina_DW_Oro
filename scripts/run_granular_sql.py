#!/usr/bin/env python3
"""
run_granular_sql.py
Ejecuta en orden todos los archivos .sql de ./sql/granular contra Postgres.

Requisitos:
  pip install psycopg2-binary python-dotenv pyyaml

Comportamiento:
  - Orden de ejecución por nombre de archivo (usa prefijos 01_, 02_, ... si necesitas control explícito).
  - Cada archivo se ejecuta en su propia transacción (commit/rollback por archivo).
  - Limpia comentarios /* ... */ y -- ... antes de dividir en ';'.
  - Fija search_path = <schema_destino>, public; para crear siempre en tu esquema (por defecto dw_granular).

Configuración:
  - Credenciales en config/.env
  - Opciones en config/settings.yaml (opcional):
      dry_run: true|false
      schema_destino: "dw_granular"
      source_schemas: ["public"]          # se agregan detrás del schema_destino en search_path
"""

import os
import sys
import time
import re
import logging
from pathlib import Path
from typing import List

import psycopg2
from psycopg2.extras import DictCursor
from dotenv import load_dotenv
import yaml

# ---------------------------
# 1) Rutas y logging
# ---------------------------
ROOT = Path(__file__).resolve().parents[1]
SQL_DIR = ROOT / "sql" / "granular"
CONF_DIR = ROOT / "config"
LOGS_DIR = ROOT / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=LOGS_DIR / "run_granular_sql.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
console = logging.StreamHandler(sys.stdout)
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter("%(levelname)s | %(message)s"))
logging.getLogger().addHandler(console)

# ---------------------------
# 2) Configuración (env + yaml)
# ---------------------------
load_dotenv(CONF_DIR / ".env")

settings_path = CONF_DIR / "settings.yaml"
settings = {}
if settings_path.exists():
    with open(settings_path, "r", encoding="utf-8") as fh:
        settings = yaml.safe_load(fh) or {}

DRY_RUN = bool(settings.get("dry_run", False))
SCHEMA_DESTINO = settings.get("schema_destino", "dw_granular")
SOURCE_SCHEMAS = settings.get("source_schemas", ["public"])

DB_HOST = os.getenv("ORO_DB_HOST", "localhost")
DB_PORT = int(os.getenv("ORO_DB_PORT", "5432"))
DB_NAME = os.getenv("ORO_DB_NAME", "orocommerce")
DB_USER = os.getenv("ORO_DB_USER", "postgres")
DB_PASS = os.getenv("ORO_DB_PASS", "changeme")

# ---------------------------
# 3) Utilidades
# ---------------------------
def list_sql_files(sql_dir: Path) -> List[Path]:
    """Lista todos los .sql ordenados alfabéticamente."""
    return sorted(Path(sql_dir).glob("*.sql"))

def normalize_sql_and_split(sql_path: Path) -> List[str]:
    """
    Carga el archivo, elimina comentarios y divide en sentencias por ';'.
    - Elimina comentarios de bloque /* ... */ con regex DOTALL.
    - Elimina líneas que comienzan con --.
    - No intenta parsear funciones; si las usas, ten 1 statement por archivo.
    """
    content = sql_path.read_text(encoding="utf-8")

    # Quitar comentarios de bloque /* ... */
    content = re.sub(r"/\*.*?\*/", "", content, flags=re.S)

    # Quitar comentarios de línea --
    lines = []
    for line in content.splitlines():
        if line.strip().startswith("--"):
            continue
        lines.append(line)
    content = "\n".join(lines)

    # Dividir por ';' y limpiar
    parts = content.split(";")
    stmts = [p.strip() for p in parts if p.strip()]
    return stmts

def open_connection():
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )
    conn.autocommit = False
    return conn

def build_search_path(schema_destino: str, source_schemas: List[str]) -> str:
    """
    Construye la sentencia SET search_path para crear en schema_destino y
    resolver fuentes en los esquemas indicados (por defecto: public).
    """
    schemas = [schema_destino] + list(source_schemas)
    safe = ", ".join([psycopg2.extensions.AsIs(s).getquoted().decode("utf-8").strip("'") for s in schemas])
    # Nota: AsIs se usa aquí solo para mantener una forma segura; search_path no acepta parámetros.
    return f"SET search_path = {safe};"

# ---------------------------
# 4) Main
# ---------------------------
def main():
    if not SQL_DIR.exists():
        logging.error(f"No existe la carpeta de SQL: {SQL_DIR}")
        sys.exit(1)

    files = list_sql_files(SQL_DIR)
    if not files:
        logging.warning(f"No se encontraron .sql en {SQL_DIR}")
        return

    logging.info(f"Base de datos destino: {DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    logging.info(f"Schema destino: {SCHEMA_DESTINO} | Sources: {', '.join(SOURCE_SCHEMAS)}")
    logging.info(f"Ejecutando {len(files)} archivo(s) desde {SQL_DIR}")
    if DRY_RUN:
        logging.info("DRY_RUN=True (no se ejecutará nada)")

    conn = None
    try:
        if not DRY_RUN:
            conn = open_connection()

        ok_files = 0
        for idx, sql_file in enumerate(files, start=1):
            inicio = time.time()
            logging.info(f"[{idx}/{len(files)}] Ejecutando: {sql_file.name}")

            stmts = normalize_sql_and_split(sql_file)
            logging.info(f"   Sentencias detectadas: {len(stmts)}")

            if DRY_RUN:
                logging.info(f"   DRY_RUN: {sql_file.name} listado sin ejecutar")
                ok_files += 1
                continue

            try:
                with conn.cursor(cursor_factory=DictCursor) as cur:
                    # Asegura crear en tu schema y leer de public (u otros)
                    cur.execute(build_search_path(SCHEMA_DESTINO, SOURCE_SCHEMAS))

                    for sidx, stmt in enumerate(stmts, start=1):
                        cur.execute(stmt)
                        logging.info(f"   OK sentencia {sidx}/{len(stmts)}")

                conn.commit()
                ok_files += 1
                dur = time.time() - inicio
                logging.info(f"   COMMIT ({dur:.2f}s)")

            except Exception as e:
                conn.rollback()
                logging.error(f"   ROLLBACK por error en {sql_file.name}: {e}")
                # Continúa con el siguiente archivo
                continue

        logging.info(f"Archivos ejecutados con éxito: {ok_files}/{len(files)}")

    finally:
        if conn is not None:
            conn.close()

if __name__ == "__main__":
    main()
