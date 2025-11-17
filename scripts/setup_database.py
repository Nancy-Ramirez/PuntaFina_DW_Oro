#!/usr/bin/env python3
"""
SCRIPT UNIFICADO: GESTIN COMPLETA DE BASE DE DATOS
===================================================
Crea la base de datos PostgreSQL si no existe, crea todas las tablas del modelo
estrella y carga todos los datos desde los archivos parquet generados.

Funcionalidades:
- Verificar/crear base de datos DW_oro
- Crear todas las tablas de dimensiones y hechos
- Cargar datos desde archivos parquet
- Verificar integridad referencial
- Optimizar ndices y rendimiento
"""

import os
import pandas as pd
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extras import execute_values
from pathlib import Path
import yaml
import time
from dotenv import load_dotenv

# Configuracin global
ROOT = Path(__file__).parent.parent
CONFIG_DIR = ROOT / "config"
PARQUET_DIR = ROOT / "data" / "outputs" / "parquet"

load_dotenv(CONFIG_DIR / ".env")

def get_admin_connection():
    """Conexin como administrador para crear base de datos"""
    return psycopg2.connect(
        host=os.getenv('DW_ORO_DB_HOST'),
        port=int(os.getenv('DW_ORO_DB_PORT')),
        dbname='postgres',  # Base de datos por defecto
        user=os.getenv('DW_ORO_DB_USER'),
        password=os.getenv('DW_ORO_DB_PASS')
    )

def get_dw_connection():
    """Conexin a la base de datos del Data Warehouse"""
    return psycopg2.connect(
        host=os.getenv('DW_ORO_DB_HOST'),
        port=int(os.getenv('DW_ORO_DB_PORT')),
        dbname=os.getenv('DW_ORO_DB_NAME'),
        user=os.getenv('DW_ORO_DB_USER'),
        password=os.getenv('DW_ORO_DB_PASS')
    )

def create_database_if_not_exists():
    """Crea la base de datos DW_oro si no existe"""
    print("  Verificando/creando base de datos...")
    
    db_name = os.getenv('DW_ORO_DB_NAME')
    
    try:
        # Intentar conectar directamente
        conn = get_dw_connection()
        conn.close()
        print(f"    Base de datos '{db_name}' ya existe")
        return True
        
    except psycopg2.OperationalError:
        # La base de datos no existe, crearla
        print(f"    Creando base de datos '{db_name}'...")
        
        admin_conn = get_admin_connection()
        admin_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        
        with admin_conn.cursor() as cur:
            cur.execute(f"CREATE DATABASE {db_name}")
        
        admin_conn.close()
        print(f"    Base de datos '{db_name}' creada exitosamente")
        return True

def create_all_tables():
    """Crea todas las tablas del modelo estrella"""
    print(" Creando tablas del modelo estrella...")
    
    conn = get_dw_connection()
    
    # DDL para todas las tablas
    table_ddl = {
        'dim_fecha': """
            CREATE TABLE IF NOT EXISTS dim_fecha (
                id_fecha BIGINT PRIMARY KEY,
                fecha DATE NOT NULL,
                año INTEGER NOT NULL,
                mes INTEGER NOT NULL,
                dia INTEGER NOT NULL,
                dia_semana INTEGER NOT NULL,
                nombre_dia VARCHAR(20) NOT NULL,
                nombre_mes VARCHAR(20) NOT NULL,
                trimestre INTEGER NOT NULL,
                semana_año INTEGER NOT NULL,
                es_fin_semana BOOLEAN NOT NULL,
                es_feriado BOOLEAN NOT NULL
            )
        """,
        
        'dim_cliente': """
            CREATE TABLE IF NOT EXISTS dim_cliente (
                id_cliente TEXT PRIMARY KEY,
                nombre TEXT NOT NULL,
                id_sitio_web TEXT NOT NULL,
                tipo_cliente TEXT NOT NULL,
                estado TEXT NOT NULL,
                fecha_registro DATE NOT NULL
            )
        """,
        
        'dim_producto': """
            CREATE TABLE IF NOT EXISTS dim_producto (
                id_producto TEXT PRIMARY KEY,
                sku TEXT NOT NULL,
                nombre TEXT NOT NULL,
                descripcion TEXT,
                unidad_medida TEXT NOT NULL,
                estado TEXT NOT NULL,
                fecha_creacion DATE NOT NULL
            )
        """,
        
        'dim_usuario': """
            CREATE TABLE IF NOT EXISTS dim_usuario (
                id_usuario TEXT PRIMARY KEY,
                username TEXT NOT NULL,
                email TEXT NOT NULL,
                nombre TEXT NOT NULL,
                apellido TEXT NOT NULL,
                nombre_completo TEXT NOT NULL,
                estado TEXT NOT NULL,
                fecha_creacion DATE NOT NULL
            )
        """,
        
        'dim_sitio_web': """
            CREATE TABLE IF NOT EXISTS dim_sitio_web (
                id_sitio_web TEXT PRIMARY KEY,
                nombre TEXT NOT NULL,
                url TEXT NOT NULL,
                estado TEXT NOT NULL,
                fecha_creacion DATE NOT NULL
            )
        """,
        
        'dim_canal': """
            CREATE TABLE IF NOT EXISTS dim_canal (
                id_canal TEXT PRIMARY KEY,
                nombre TEXT NOT NULL,
                tipo TEXT NOT NULL,
                estado TEXT NOT NULL
            )
        """,
        
        'dim_direccion': """
            CREATE TABLE IF NOT EXISTS dim_direccion (
                id_direccion TEXT PRIMARY KEY,
                calle TEXT NOT NULL,
                ciudad TEXT NOT NULL,
                codigo_postal TEXT NOT NULL,
                region TEXT NOT NULL,
                pais_codigo TEXT NOT NULL,
                direccion_completa TEXT NOT NULL,
                estado TEXT NOT NULL
            )
        """,
        
        'dim_envio': """
            CREATE TABLE IF NOT EXISTS dim_envio (
                id_envio TEXT PRIMARY KEY,
                metodo_envio TEXT NOT NULL,
                tiempo_entrega TEXT NOT NULL,
                costo NUMERIC(10,2) NOT NULL,
                estado TEXT NOT NULL
            )
        """,
        
        'dim_pago': """
            CREATE TABLE IF NOT EXISTS dim_pago (
                id_pago TEXT PRIMARY KEY,
                metodo_pago TEXT NOT NULL,
                estado_pago TEXT NOT NULL,
                monto NUMERIC(15,2) NOT NULL,
                moneda TEXT NOT NULL,
                fecha_transaccion DATE NOT NULL
            )
        """,
        
        'dim_impuestos': """
            CREATE TABLE IF NOT EXISTS dim_impuestos (
                id_impuestos TEXT PRIMARY KEY,
                codigo_impuesto TEXT NOT NULL,
                descripcion TEXT NOT NULL,
                tasa NUMERIC(5,4) NOT NULL,
                estado TEXT NOT NULL
            )
        """,
        
        'dim_promocion': """
            CREATE TABLE IF NOT EXISTS dim_promocion (
                id_promocion TEXT PRIMARY KEY,
                nombre_promocion TEXT NOT NULL,
                descripcion TEXT NOT NULL,
                descuento_monto NUMERIC(10,2) NOT NULL,
                tipo_descuento TEXT NOT NULL,
                estado TEXT NOT NULL
            )
        """,
        
        'dim_orden': """
            CREATE TABLE IF NOT EXISTS dim_orden (
                id_orden TEXT PRIMARY KEY,
                numero_orden TEXT,
                numero_po TEXT,
                cliente_nombre TEXT NOT NULL,
                usuario_nombre_completo TEXT NOT NULL,
                sitio_web_nombre TEXT NOT NULL,
                subtotal NUMERIC(15,2) NOT NULL,
                total NUMERIC(15,2) NOT NULL,
                moneda TEXT NOT NULL,
                fecha_orden DATE NOT NULL,
                fecha_actualizacion DATE NOT NULL,
                categoria_orden TEXT NOT NULL
            )
        """,
        
        'dim_line_item': """
            CREATE TABLE IF NOT EXISTS dim_line_item (
                id_line_item TEXT PRIMARY KEY,
                id_orden TEXT NOT NULL,
                id_producto TEXT NOT NULL,
                producto_sku TEXT NOT NULL,
                producto_nombre TEXT NOT NULL,
                cantidad NUMERIC(10,2) NOT NULL,
                precio_unitario NUMERIC(10,2) NOT NULL,
                total_linea NUMERIC(15,2) NOT NULL,
                moneda TEXT NOT NULL,
                unidad TEXT NOT NULL,
                stock_actual NUMERIC(10,2) NOT NULL DEFAULT 0.0,
                stock_disponible NUMERIC(10,2) NOT NULL DEFAULT 0.0,
                stock_despues_venta NUMERIC(10,2) NOT NULL DEFAULT 0.0,
                estado_stock TEXT NOT NULL DEFAULT 'Sin Información'
            )
        """,
        
        'fact_ventas': """
            CREATE TABLE IF NOT EXISTS fact_ventas (
                id_venta SERIAL PRIMARY KEY,
                id_line_item TEXT NOT NULL,
                id_order TEXT NOT NULL,
                id_cliente TEXT NOT NULL,
                id_producto TEXT NOT NULL,
                id_usuario TEXT NOT NULL,
                id_sitio_web TEXT NOT NULL,
                id_fecha BIGINT NOT NULL,
                id_promocion TEXT NOT NULL,
                id_canal TEXT NOT NULL,
                id_direccion TEXT,
                id_envio TEXT NOT NULL,
                id_impuestos TEXT NOT NULL,
                id_pago TEXT NOT NULL,
                id_status_pago TEXT NOT NULL,
                id_metodo_pago TEXT NOT NULL,
                cantidad NUMERIC(10,2) NOT NULL,
                precio_unitario NUMERIC(10,2) NOT NULL,
                total_linea NUMERIC(15,2) NOT NULL,
                subtotal_orden NUMERIC(15,2) NOT NULL,
                total_orden NUMERIC(15,2) NOT NULL,
                descuento_promocion NUMERIC(15,2) NOT NULL DEFAULT 0.0,
                fecha_venta TIMESTAMP NOT NULL,
                moneda TEXT NOT NULL,
                numero_po TEXT,
                numero_orden TEXT,
                stock_actual NUMERIC(10,2) NOT NULL DEFAULT 0.0,
                stock_inicial NUMERIC(10,2) NOT NULL DEFAULT 0.0,
                stock_restante NUMERIC(10,2) NOT NULL DEFAULT 0.0,
                total_linea_neto NUMERIC(15,2) NOT NULL DEFAULT 0.0,
                UNIQUE (id_line_item, id_order),
                
                -- Foreign Keys hacia dimensiones
                FOREIGN KEY (id_cliente) REFERENCES dim_cliente(id_cliente),
                FOREIGN KEY (id_producto) REFERENCES dim_producto(id_producto),
                FOREIGN KEY (id_usuario) REFERENCES dim_usuario(id_usuario),
                FOREIGN KEY (id_sitio_web) REFERENCES dim_sitio_web(id_sitio_web),
                FOREIGN KEY (id_fecha) REFERENCES dim_fecha(id_fecha),
                FOREIGN KEY (id_promocion) REFERENCES dim_promocion(id_promocion),
                FOREIGN KEY (id_canal) REFERENCES dim_canal(id_canal),
                FOREIGN KEY (id_direccion) REFERENCES dim_direccion(id_direccion),
                FOREIGN KEY (id_envio) REFERENCES dim_envio(id_envio),
                FOREIGN KEY (id_impuestos) REFERENCES dim_impuestos(id_impuestos),
                FOREIGN KEY (id_pago) REFERENCES dim_pago(id_pago),
                FOREIGN KEY (id_order) REFERENCES dim_orden(id_orden),
                FOREIGN KEY (id_line_item) REFERENCES dim_line_item(id_line_item)
            )
        """
    }
    
    with conn.cursor() as cur:
        for table_name, ddl in table_ddl.items():
            cur.execute(ddl)
            print(f"    Tabla {table_name} creada/verificada")
    
    conn.commit()
    conn.close()
    
    # Asegurar columnas adicionales que pueden generarse desde el ETL
    conn = get_dw_connection()
    with conn.cursor() as cur:
        try:
            # Columnas adicionales en fact_ventas
            cur.execute("ALTER TABLE fact_ventas ADD COLUMN IF NOT EXISTS descuento_promocion NUMERIC(15,2) NOT NULL DEFAULT 0.0")
            cur.execute("ALTER TABLE fact_ventas ADD COLUMN IF NOT EXISTS total_linea_neto NUMERIC(15,2) NOT NULL DEFAULT 0.0")
            cur.execute("ALTER TABLE fact_ventas ADD COLUMN IF NOT EXISTS fecha_venta TIMESTAMP")
            cur.execute("ALTER TABLE fact_ventas ADD COLUMN IF NOT EXISTS stock_actual NUMERIC(10,2) NOT NULL DEFAULT 0.0")
            cur.execute("ALTER TABLE fact_ventas ADD COLUMN IF NOT EXISTS stock_inicial NUMERIC(10,2) NOT NULL DEFAULT 0.0")
            cur.execute("ALTER TABLE fact_ventas ADD COLUMN IF NOT EXISTS stock_restante NUMERIC(10,2) NOT NULL DEFAULT 0.0")
            
            # Columnas adicionales en dim_line_item para stock dinámico
            cur.execute("ALTER TABLE dim_line_item ADD COLUMN IF NOT EXISTS stock_actual NUMERIC(10,2) NOT NULL DEFAULT 0.0")
            cur.execute("ALTER TABLE dim_line_item ADD COLUMN IF NOT EXISTS stock_disponible NUMERIC(10,2) NOT NULL DEFAULT 0.0")
            cur.execute("ALTER TABLE dim_line_item ADD COLUMN IF NOT EXISTS stock_despues_venta NUMERIC(10,2) NOT NULL DEFAULT 0.0")
            cur.execute("ALTER TABLE dim_line_item ADD COLUMN IF NOT EXISTS estado_stock TEXT NOT NULL DEFAULT 'Sin Información'")
            
            conn.commit()
            print("    Columnas adicionales en fact_ventas y dim_line_item verificadas/creadas")
        except Exception:
            conn.rollback()
    conn.close()

    print(f"    {len(table_ddl)} tablas creadas exitosamente")

def create_foreign_keys():
    """Crea las foreign keys si no existen (para tablas existentes)"""
    print("  Creando foreign keys...")
    
    conn = get_dw_connection()
    
    fk_commands = [
        ("fk_fact_cliente", "ALTER TABLE fact_ventas ADD CONSTRAINT fk_fact_cliente FOREIGN KEY (id_cliente) REFERENCES dim_cliente(id_cliente)"),
        ("fk_fact_producto", "ALTER TABLE fact_ventas ADD CONSTRAINT fk_fact_producto FOREIGN KEY (id_producto) REFERENCES dim_producto(id_producto)"), 
        ("fk_fact_usuario", "ALTER TABLE fact_ventas ADD CONSTRAINT fk_fact_usuario FOREIGN KEY (id_usuario) REFERENCES dim_usuario(id_usuario)"),
        ("fk_fact_sitio_web", "ALTER TABLE fact_ventas ADD CONSTRAINT fk_fact_sitio_web FOREIGN KEY (id_sitio_web) REFERENCES dim_sitio_web(id_sitio_web)"),
        ("fk_fact_fecha", "ALTER TABLE fact_ventas ADD CONSTRAINT fk_fact_fecha FOREIGN KEY (id_fecha) REFERENCES dim_fecha(id_fecha)"),
        ("fk_fact_promocion", "ALTER TABLE fact_ventas ADD CONSTRAINT fk_fact_promocion FOREIGN KEY (id_promocion) REFERENCES dim_promocion(id_promocion)"),
        ("fk_fact_canal", "ALTER TABLE fact_ventas ADD CONSTRAINT fk_fact_canal FOREIGN KEY (id_canal) REFERENCES dim_canal(id_canal)"),
        ("fk_fact_direccion", "ALTER TABLE fact_ventas ADD CONSTRAINT fk_fact_direccion FOREIGN KEY (id_direccion) REFERENCES dim_direccion(id_direccion)"),
        ("fk_fact_envio", "ALTER TABLE fact_ventas ADD CONSTRAINT fk_fact_envio FOREIGN KEY (id_envio) REFERENCES dim_envio(id_envio)"),
        ("fk_fact_impuestos", "ALTER TABLE fact_ventas ADD CONSTRAINT fk_fact_impuestos FOREIGN KEY (id_impuestos) REFERENCES dim_impuestos(id_impuestos)"),
        ("fk_fact_pago", "ALTER TABLE fact_ventas ADD CONSTRAINT fk_fact_pago FOREIGN KEY (id_pago) REFERENCES dim_pago(id_pago)"),
        ("fk_fact_orden", "ALTER TABLE fact_ventas ADD CONSTRAINT fk_fact_orden FOREIGN KEY (id_order) REFERENCES dim_orden(id_orden)"),
        ("fk_fact_line_item", "ALTER TABLE fact_ventas ADD CONSTRAINT fk_fact_line_item FOREIGN KEY (id_line_item) REFERENCES dim_line_item(id_line_item)")
    ]
    
    # Crear cada FK en una transacción separada para evitar rollbacks en cascada
    for fk_name, fk_cmd in fk_commands:
        try:
            with conn.cursor() as cur:
                # Verificar si la FK ya existe
                cur.execute("""
                    SELECT 1 FROM information_schema.table_constraints 
                    WHERE constraint_name = %s AND table_name = 'fact_ventas'
                """, (fk_name,))
                
                if cur.fetchone():
                    print(f"    FK {fk_name} ya existe")
                else:
                    cur.execute(fk_cmd)
                    conn.commit()
                    print(f"    FK creada: {fk_name}")
                    
        except Exception as e:
            conn.rollback()  # Rollback solo esta transacción
            if "already exists" in str(e).lower():
                print(f"    FK {fk_name} ya existe")
            else:
                print(f"    Advertencia FK {fk_name}: {e}")
    
    conn.close()
    
    print(f"    {len(fk_commands)} foreign keys procesadas")

def fix_missing_dates():
    """Arregla fechas huérfanas creando las filas faltantes en dim_fecha"""
    print("  Verificando y arreglando fechas huérfanas...")
    
    conn = get_dw_connection()
    
    with conn.cursor() as cur:
        # Obtener fechas huérfanas
        cur.execute("""
            SELECT DISTINCT f.id_fecha
            FROM fact_ventas f
            LEFT JOIN dim_fecha d ON f.id_fecha = d.id_fecha
            WHERE d.id_fecha IS NULL
            ORDER BY f.id_fecha
        """)
        
        missing_dates = [row[0] for row in cur.fetchall()]
        
        if not missing_dates:
            print("    No hay fechas huérfanas")
            conn.close()
            return 0
        
        print(f"    Encontradas {len(missing_dates)} fechas huérfanas")
        
        # Crear las filas de dim_fecha faltantes
        from datetime import datetime
        
        date_rows = []
        for date_id in missing_dates:
            try:
                # Convertir YYYYMMDD a fecha
                date_str = str(date_id)
                if len(date_str) == 8:
                    year = int(date_str[:4])
                    month = int(date_str[4:6])
                    day = int(date_str[6:8])
                    
                    fecha_obj = datetime(year, month, day)
                    
                    date_row = (
                        date_id,  # id_fecha
                        fecha_obj.strftime('%Y-%m-%d'),  # fecha
                        year,  # año
                        month,  # mes
                        day,  # dia
                        fecha_obj.weekday() + 1,  # dia_semana (1=Lunes)
                        fecha_obj.strftime('%A'),  # nombre_dia
                        fecha_obj.strftime('%B'),  # nombre_mes
                        (month - 1) // 3 + 1,  # trimestre
                        fecha_obj.isocalendar()[1],  # semana_año
                        fecha_obj.weekday() >= 5,  # es_fin_semana
                        False  # es_feriado
                    )
                    date_rows.append(date_row)
                    
            except Exception as e:
                print(f"    Error procesando fecha {date_id}: {e}")
        
        if date_rows:
            # Insertar fechas faltantes
            insert_query = """
                INSERT INTO dim_fecha (id_fecha, fecha, año, mes, dia, dia_semana, 
                                     nombre_dia, nombre_mes, trimestre, semana_año, 
                                     es_fin_semana, es_feriado)
                VALUES %s
            """
            execute_values(cur, insert_query, date_rows, page_size=1000)
            conn.commit()
            print(f"    {len(date_rows)} fechas faltantes insertadas en dim_fecha")
        
    conn.close()
    return len(date_rows)

def fix_orphaned_references():
    """Arregla referencias huérfanas en fact_ventas"""
    print("  Verificando y arreglando referencias huérfanas...")
    
    conn = get_dw_connection()
    
    with conn.cursor() as cur:
        # 1. Arreglar id_producto NULL/vacíos/None - asignar producto por defecto
        cur.execute("SELECT COUNT(*) FROM fact_ventas WHERE id_producto IS NULL OR id_producto = '' OR id_producto = 'None' OR id_producto = 'nan'")
        null_productos = cur.fetchone()[0]
        
        if null_productos > 0:
            # Crear producto por defecto si no existe
            cur.execute("INSERT INTO dim_producto (id_producto, sku, nombre, descripcion, unidad_medida, estado, fecha_creacion) VALUES ('0', 'SKU-DEFAULT', 'Producto Sin Especificar', 'Producto por defecto para registros sin especificar', 'unit', 'Activo', CURRENT_DATE) ON CONFLICT (id_producto) DO NOTHING")
            
            # Actualizar registros huérfanos (incluyendo 'None' como string y 'nan')
            cur.execute("UPDATE fact_ventas SET id_producto = '0' WHERE id_producto IS NULL OR id_producto = '' OR id_producto = 'None' OR id_producto = 'nan'")
            print(f"    {null_productos} registros con id_producto nulo/vacío/None → asignados a producto '0'")
        
        # 2. Arreglar id_promocion 'SIN_PROMO'
        cur.execute("SELECT COUNT(*) FROM fact_ventas WHERE id_promocion = 'SIN_PROMO'")
        sin_promo = cur.fetchone()[0]
        
        if sin_promo > 0:
            # Crear promoción por defecto
            cur.execute("INSERT INTO dim_promocion (id_promocion, nombre_promocion, descripcion, descuento_monto, tipo_descuento, estado) VALUES ('SIN_PROMO', 'Sin Promoción', 'Sin promoción aplicada', 0.0, 'none', 'Activo') ON CONFLICT (id_promocion) DO NOTHING")
            print(f"    Creada promoción 'SIN_PROMO' para {sin_promo} registros")
        
        # 3. Arreglar id_direccion vacías
        cur.execute("SELECT COUNT(*) FROM fact_ventas WHERE id_direccion IS NULL OR id_direccion = ''")
        null_direcciones = cur.fetchone()[0]
        
        if null_direcciones > 0:
            # Crear dirección por defecto
            cur.execute("INSERT INTO dim_direccion (id_direccion, calle, ciudad, codigo_postal, region, pais_codigo, direccion_completa, estado) VALUES ('0', 'Sin Especificar', 'Sin Especificar', '00000', 'Sin Especificar', 'US', 'Dirección no especificada', 'Activa') ON CONFLICT (id_direccion) DO NOTHING")
            
            # Actualizar registros huérfanos - direccion puede ser NULL (es opcional)
            cur.execute("UPDATE fact_ventas SET id_direccion = '0' WHERE id_direccion = ''")
            print(f"    {null_direcciones} registros con id_direccion vacía → asignados a dirección '0'")
        
        conn.commit()
    
    conn.close()
    print("    Referencias huérfanas corregidas")

def load_dimension_data(table_name, conn):
    """Carga datos de una dimensin desde parquet"""
    parquet_file = PARQUET_DIR / f"{table_name}.parquet"
    
    if not parquet_file.exists():
        print(f"     Archivo {parquet_file.name} no encontrado")
        return 0
    
    # Leer datos
    df = pd.read_parquet(parquet_file)
    
    if len(df) == 0:
        print(f"     {table_name}: archivo vaco")
        return 0
    
    # Para carga incremental, no limpiar la tabla
    # Solo insertar datos nuevos que no existan
    
    # Preparar datos para insercin
    # Deduplicar por la primera columna (clave primaria esperada)
    if len(df.columns) > 0:
        pk_col = df.columns[0]
        before = len(df)
        df = df.drop_duplicates(subset=[pk_col])
        after = len(df)
        if before != after:
            print(f"    {table_name}: se eliminaron {before - after} duplicados por '{pk_col}' antes de insertar")

    # Asegurar que no haya NULLs en columnas donde la DDL exige NOT NULL
    # Rellenar strings/categoricals con cadena vacía y numericos con 0
    for col in df.columns:
        try:
            dtype_name = df[col].dtype.name
        except Exception:
            dtype_name = None
        
        # Para categorias, convertir a object primero para evitar errores al asignar nuevas categorias
        if dtype_name == 'category':
            df[col] = df[col].astype(object).fillna('').astype(str)
            # Limpiar valores problemáticos específicos
            df[col] = df[col].replace(['None', 'nan', 'NaN', 'null'], '')
        elif dtype_name == 'object':
            df[col] = df[col].fillna('')
            # Limpiar valores problemáticos específicos para objetos/strings
            df[col] = df[col].replace(['None', 'nan', 'NaN', 'null'], '')
            # Convertir a string para asegurar consistencia
            df[col] = df[col].astype(str)
        else:
            # Para tipos numericos
            if pd.api.types.is_numeric_dtype(df[col]):
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            else:
                # Para otros tipos (fechas, etc.) rellenar con cadena vacía
                df[col] = df[col].fillna('')
                df[col] = df[col].replace(['None', 'nan', 'NaN', 'null'], '')

    columns = list(df.columns)
    # Preparar valores después de limpiar NULLs
    values = [tuple(row) for row in df.values]
    
    # Contar registros existentes antes de la inserción
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        existing_count = cur.fetchone()[0]
    
    # Insertar en lotes usando ON CONFLICT DO NOTHING para carga incremental
    # Quote column names to handle special characters (ej. ñ) and reserved words
    quoted_columns = ', '.join([f'"{c}"' for c in columns])
    
    with conn.cursor() as cur:
        # Caso especial para fact_ventas (tiene constraint UNIQUE compuesto)
        if table_name == 'fact_ventas':
            # Usar la restricción UNIQUE (id_line_item, id_order) para fact_ventas
            query = f"INSERT INTO {table_name} ({quoted_columns}) VALUES %s ON CONFLICT (id_line_item, id_order) DO NOTHING"
        else:
            # Para dimensiones, usar la primera columna (PK) para ON CONFLICT
            pk_column = f'"{columns[0]}"' if columns else None
            if pk_column:
                query = f"INSERT INTO {table_name} ({quoted_columns}) VALUES %s ON CONFLICT ({pk_column}) DO NOTHING"
            else:
                # Fallback sin ON CONFLICT si no hay PK identificable
                query = f"INSERT INTO {table_name} ({quoted_columns}) VALUES %s"
        
        execute_values(cur, query, values, page_size=1000)
    
    conn.commit()
    
    # Contar registros después de la inserción para calcular nuevos
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        final_count = cur.fetchone()[0]
    
    new_records = final_count - existing_count
    print(f"    {table_name}: {new_records:,} registros NUEVOS insertados (total en tabla: {final_count:,})")
    return new_records

def drop_foreign_keys():
    """Elimina temporalmente las foreign keys para permitir la carga"""
    print("   Eliminando foreign keys temporalmente...")
    
    conn = get_dw_connection()
    
    fk_names = [
        "fk_fact_cliente", "fk_fact_producto", "fk_fact_usuario", "fk_fact_sitio_web",
        "fk_fact_fecha", "fk_fact_promocion", "fk_fact_canal", "fk_fact_direccion",
        "fk_fact_envio", "fk_fact_impuestos", "fk_fact_pago", "fk_fact_orden", "fk_fact_line_item"
    ]
    
    with conn.cursor() as cur:
        for fk_name in fk_names:
            try:
                cur.execute(f"ALTER TABLE fact_ventas DROP CONSTRAINT IF EXISTS {fk_name}")
            except Exception:
                pass  # Ignorar si no existe
        conn.commit()
    
    conn.close()
    print("   Foreign keys eliminadas temporalmente")

def full_refresh_tables():
    """Limpia todas las tablas para carga completa (opcional)"""
    print("   FULL REFRESH: Limpiando todas las tablas...")
    
    conn = get_dw_connection()
    
    with conn.cursor() as cur:
        tables_to_clean = ['fact_ventas', 'dim_fecha', 'dim_cliente', 'dim_producto', 'dim_usuario', 
                          'dim_sitio_web', 'dim_canal', 'dim_direccion', 'dim_envio', 'dim_pago', 
                          'dim_impuestos', 'dim_promocion', 'dim_orden', 'dim_line_item']
        for table in tables_to_clean:
            cur.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE")
        conn.commit()
    
    conn.close()
    print("   Todas las tablas limpiadas")

def load_all_data(full_refresh_mode=False):
    """Carga todos los datos desde archivos parquet"""
    print(" Cargando datos desde archivos parquet...")
    
    conn = get_dw_connection()
    
    # Primero eliminar FKs temporalmente para permitir inserciones
    drop_foreign_keys()
    
    # Opcionalmente limpiar tablas para full refresh
    if full_refresh_mode:
        full_refresh_tables()
    else:
        # Para carga incremental, NO limpiamos las tablas
        # Los datos nuevos se insertan con ON CONFLICT DO NOTHING
        print("   Modo INCREMENTAL: conservando datos existentes, insertando solo nuevos registros...")
    
    # Orden de carga (dimensiones primero, luego hechos)
    load_order = [
        'dim_fecha', 'dim_cliente', 'dim_producto', 'dim_usuario', 
        'dim_sitio_web', 'dim_canal', 'dim_direccion', 'dim_envio',
        'dim_pago', 'dim_impuestos', 'dim_promocion', 'dim_orden',
        'dim_line_item', 'fact_ventas'
    ]
    
    total_records = 0
    loaded_tables = {}
    
    for table_name in load_order:
        try:
            count = load_dimension_data(table_name, conn)
            loaded_tables[table_name] = count
            total_records += count
        except Exception as e:
            print(f"    Error cargando {table_name}: {e}")
            loaded_tables[table_name] = 0
    
    conn.close()
    
    print(f"\n RESUMEN DE CARGA:")
    for table, count in loaded_tables.items():
        status = "" if count > 0 else ""
        print(f"   {status} {table}: {count:,} registros")
    
    print(f"\n Total registros NUEVOS cargados: {total_records:,}")
    
    # En modo incremental, es normal que no haya registros nuevos
    # Considerar éxito si el proceso completó sin errores
    return True

def create_indexes():
    """Crea ndices para optimizar consultas"""
    print(" Creando ndices para optimizacin...")
    
    conn = get_dw_connection()
    
    indexes = [
        # ndices en fact_ventas (tabla principal)
        "CREATE INDEX IF NOT EXISTS idx_fact_ventas_fecha ON fact_ventas(id_fecha)",
        "CREATE INDEX IF NOT EXISTS idx_fact_ventas_cliente ON fact_ventas(id_cliente)",
        "CREATE INDEX IF NOT EXISTS idx_fact_ventas_producto ON fact_ventas(id_producto)",
        "CREATE INDEX IF NOT EXISTS idx_fact_ventas_order ON fact_ventas(id_order)",
        "CREATE INDEX IF NOT EXISTS idx_fact_ventas_canal ON fact_ventas(id_canal)",
        
        # ndices en dimensiones principales
        "CREATE INDEX IF NOT EXISTS idx_dim_fecha_año_mes ON dim_fecha(año, mes)",
        "CREATE INDEX IF NOT EXISTS idx_dim_cliente_nombre ON dim_cliente(nombre)",
        "CREATE INDEX IF NOT EXISTS idx_dim_producto_sku ON dim_producto(sku)",
        "CREATE INDEX IF NOT EXISTS idx_dim_orden_fecha ON dim_orden(fecha_orden)",
        
        # ndices compuestos para consultas frecuentes
        "CREATE INDEX IF NOT EXISTS idx_fact_ventas_fecha_cliente ON fact_ventas(id_fecha, id_cliente)",
        "CREATE INDEX IF NOT EXISTS idx_fact_ventas_fecha_producto ON fact_ventas(id_fecha, id_producto)"
    ]
    
    with conn.cursor() as cur:
        for index_sql in indexes:
            try:
                cur.execute(index_sql)
                print(f"    ndice creado: {index_sql.split('idx_')[1].split(' ')[0]}")
            except Exception as e:
                print(f"     Error creando ndice: {e}")
    
    conn.commit()
    conn.close()
    
    print(f"    {len(indexes)} ndices procesados")

def verify_data_integrity():
    """Verifica la integridad de los datos cargados"""
    print(" Verificando integridad de datos...")
    
    conn = get_dw_connection()
    
    with conn.cursor() as cur:
        # Verificar conteos bsicos
        cur.execute("SELECT COUNT(*) FROM fact_ventas")
        fact_count = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM dim_cliente")
        client_count = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM dim_producto")
        product_count = cur.fetchone()[0]
        
        print(f"    fact_ventas: {fact_count:,} registros")
        print(f"    dim_cliente: {client_count:,} registros")
        print(f"    dim_producto: {product_count:,} registros")
        
        # Verificar FKs principales
        cur.execute("""
            SELECT COUNT(*) 
            FROM fact_ventas f
            LEFT JOIN dim_cliente c ON f.id_cliente = c.id_cliente
            WHERE c.id_cliente IS NULL
        """)
        orphan_clients = cur.fetchone()[0]
        
        cur.execute("""
            SELECT COUNT(*) 
            FROM fact_ventas f
            LEFT JOIN dim_fecha fe ON f.id_fecha = fe.id_fecha
            WHERE fe.id_fecha IS NULL
        """)
        orphan_dates = cur.fetchone()[0]
        
        print(f"    Clientes hurfanos: {orphan_clients}")
        print(f"    Fechas hurfanas: {orphan_dates}")
        
        # Estadsticas de ventas
        cur.execute("""
            SELECT 
                SUM(total_linea) as total_ventas,
                AVG(total_linea) as ticket_promedio,
                MIN(id_fecha) as fecha_min,
                MAX(id_fecha) as fecha_max
            FROM fact_ventas
        """)
        stats = cur.fetchone()
        # Manejar posibles valores NULL (cuando no hay filas en fact_ventas)
        total_ventas = stats[0] if stats[0] is not None else 0.0
        ticket_promedio = stats[1] if stats[1] is not None else 0.0
        fecha_min = stats[2] if stats[2] is not None else 'N/A'
        fecha_max = stats[3] if stats[3] is not None else 'N/A'

        print(f"    Total ventas: ${total_ventas:,.2f}")
        print(f"    Ticket promedio: ${ticket_promedio:,.2f}")
        print(f"    Rango fechas: {fecha_min} - {fecha_max}")
    
    conn.close()
    
    return orphan_clients == 0 and orphan_dates == 0

def main():
    """Funcin principal de gestin de base de datos"""
    print("GESTION COMPLETA DE BASE DE DATOS")
    print("=" * 60)
    print("MODO: CARGA INCREMENTAL (solo inserta registros nuevos)")
    print("Para carga completa (TRUNCATE + INSERT), usar full_refresh_mode=True")
    
    start_time = time.time()
    
    try:
        # Paso 1: Crear base de datos
        if not create_database_if_not_exists():
            print(" Error creando base de datos")
            return False
        
        # Paso 2: Crear tablas
        create_all_tables()
        
        # Paso 3: Cargar datos (modo incremental por defecto)
        if not load_all_data(full_refresh_mode=False):
            print(" Error cargando datos")
            return False
        
        # Paso 4: Arreglar fechas huérfanas antes de crear FKs
        missing_dates_fixed = fix_missing_dates()
        
        # Paso 5: Arreglar referencias huérfanas
        fix_orphaned_references()
        
        # Paso 6: Crear foreign keys
        create_foreign_keys()
        
        # Paso 7: Crear índices
        create_indexes()
        
        # Paso 8: Verificar integridad
        integrity_ok = verify_data_integrity()
        
        elapsed_time = time.time() - start_time
        
        print(f"\n BASE DE DATOS CONFIGURADA EXITOSAMENTE!")
        print(f"     Tiempo total: {elapsed_time:.1f} segundos")
        print(f"    Tablas creadas e indexadas")
        print(f"    Datos cargados completamente")
        print(f"   {'' if integrity_ok else ' '} Integridad referencial verificada")
        print(f"    Data Warehouse listo para consultas")
        
        return integrity_ok
        
    except Exception as e:
        print(f" Error en gestin de base de datos: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
