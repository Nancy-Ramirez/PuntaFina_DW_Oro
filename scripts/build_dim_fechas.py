#!/usr/bin/env python3
"""
build_dim_fechas.py
Lee data/inputs/dim_fechas.csv y materializa dim_fecha a data/outputs/ (parquet/csv).

CSV mínimo esperado:
- columna 'fecha' (YYYY-MM-DD)

Genera columnas estándar:
- id_fecha (YYYYMMDD int)
- anio, mes, dia, trimestre
- nombre_mes, nombre_dia
- anio_mes (YYYY-MM string)
"""

import os
import sys
import logging
from pathlib import Path
import pandas as pd

from dotenv import load_dotenv
import yaml

ROOT = Path(__file__).resolve().parents[1]
CONF_DIR = ROOT / "config"
IN_PATH = ROOT / "data" / "inputs" / "dim_fechas.csv"
OUT_PARQUET_DIR = ROOT / "data" / "outputs" / "parquet"
OUT_CSV_DIR = ROOT / "data" / "outputs" / "csv"
LOGS_DIR = ROOT / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=LOGS_DIR / "build_dim_fecha.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
console = logging.StreamHandler(sys.stdout)
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter("%(levelname)s | %(message)s"))
logging.getLogger().addHandler(console)

load_dotenv(CONF_DIR / ".env")

settings_path = CONF_DIR / "settings.yaml"
settings = {}
if settings_path.exists():
    with open(settings_path, "r", encoding="utf-8") as fh:
        settings = yaml.safe_load(fh) or {}

EXPORTAR_PARQUET = bool(settings.get("exportar_parquet", True))
EXPORTAR_CSV = bool(settings.get("exportar_csv", False))

NOMBRES_MESES = ["Enero","Febrero","Marzo","Abril","Mayo","Junio",
                 "Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"]
NOMBRES_DIAS = ["Lunes","Martes","Miércoles","Jueves","Viernes","Sábado","Domingo"]

def main():
    if not IN_PATH.exists():
        logging.error(f"No existe el CSV: {IN_PATH}")
        sys.exit(1)

    df = pd.read_csv(IN_PATH)
    if "fecha" not in df.columns:
        logging.error("El CSV debe tener una columna 'fecha' (YYYY-MM-DD).")
        sys.exit(1)

    df["fecha"] = pd.to_datetime(df["fecha"]).dt.date
    df["id_fecha"] = pd.to_datetime(df["fecha"]).dt.strftime("%Y%m%d").astype(int)
    df["anio"] = pd.to_datetime(df["fecha"]).dt.year
    df["mes"] = pd.to_datetime(df["fecha"]).dt.month
    df["dia"] = pd.to_datetime(df["fecha"]).dt.day
    df["trimestre"] = pd.to_datetime(df["fecha"]).dt.quarter
    df["nombre_mes"] = df["mes"].map(lambda m: NOMBRES_MESES[m-1])
    # dt.weekday: Monday=0,... Sunday=6
    df["_weekday"] = pd.to_datetime(df["fecha"]).dt.weekday
    df["nombre_dia"] = df["_weekday"].map(lambda d: NOMBRES_DIAS[d])
    df.drop(columns=["_weekday"], inplace=True)
    df["anio_mes"] = pd.to_datetime(df["fecha"]).dt.strftime("%Y-%m")

    # Orden sugerido
    cols = ["id_fecha","fecha","anio","mes","nombre_mes","dia","nombre_dia","trimestre","anio_mes"]
    cols = [c for c in cols if c in df.columns] + [c for c in df.columns if c not in cols]
    df = df[cols].drop_duplicates().sort_values("fecha")

    # Export
    if EXPORTAR_PARQUET:
        out_dir = OUT_PARQUET_DIR / "dim_fecha"
        out_dir.mkdir(parents=True, exist_ok=True)
        df.to_parquet(out_dir / "part-00000.parquet", index=False)
        logging.info(f"dim_fecha: parquet -> {out_dir / 'part-00000.parquet'}")

    if EXPORTAR_CSV:
        OUT_CSV_DIR.mkdir(parents=True, exist_ok=True)
        out_csv = OUT_CSV_DIR / "dim_fecha.csv"
        df.to_csv(out_csv, index=False)
        logging.info(f"dim_fecha: csv -> {out_csv}")

    logging.info(f"Filas: {len(df)} | Rango: {df['fecha'].min()} → {df['fecha'].max()}")

if __name__ == "__main__":
    main()
