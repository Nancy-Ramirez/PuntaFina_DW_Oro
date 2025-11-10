#!/usr/bin/env python3
"""
SCRIPT UNIFICADO: CONSTRUCCIÓN DE TODAS LAS DIMENSIONES
======================================================
Construye todas las tablas de dimensiones desde los datos raw de OroCommerce
hasta archivos parquet y CSV optimizados para el Data Warehouse.

Dimensiones incluidas:
- dim_fecha (calendario completo)
- dim_cliente (clientes únicos)
- dim_producto (productos del catálogo)
- dim_orden (órdenes con info desnormalizada)
- dim_usuario (usuarios del sistema)
- dim_sitio_web (sitios web/canales)
- dim_canal (canales de venta)
- dim_direccion (direcciones de envío)
- dim_envio (métodos de envío)
- dim_pago (métodos y estados de pago)
- dim_impuestos (configuración fiscal)
- dim_promocion (promociones y descuentos)
- dim_line_item (items de línea únicos)
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import yaml
import warnings
warnings.filterwarnings('ignore')

# Configuración global
ROOT = Path(__file__).parent.parent
CONFIG_DIR = ROOT / "config"
PARQUET_DIR = ROOT / "data" / "outputs" / "parquet"
CSV_DIR = ROOT / "data" / "outputs" / "csv"

# Crear directorios si no existen
PARQUET_DIR.mkdir(parents=True, exist_ok=True)
CSV_DIR.mkdir(parents=True, exist_ok=True)

# Configuración de conexión
def load_config():
    """Carga configuración desde settings.yaml"""
    config_file = CONFIG_DIR / "settings.yaml"
    if config_file.exists():
        with open(config_file, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    return {}

def get_oro_connection():
    """Conexión a OroCommerce"""
    import psycopg2
    from dotenv import load_dotenv
    
    load_dotenv(CONFIG_DIR / ".env")
    
    return psycopg2.connect(
        host=os.getenv('ORO_DB_HOST'),
        port=int(os.getenv('ORO_DB_PORT')),
        dbname=os.getenv('ORO_DB_NAME'),
        user=os.getenv('ORO_DB_USER'),
        password=os.getenv('ORO_DB_PASS')
    )

def save_dimension(df, name):
    """Guarda dimensión en parquet y CSV"""
    # Parquet (optimizado)
    parquet_file = PARQUET_DIR / f"{name}.parquet"
    df.to_parquet(parquet_file, index=False, compression='snappy')
    
    # CSV (para revisión)
    csv_file = CSV_DIR / f"{name}.csv"
    df.to_csv(csv_file, index=False, encoding='utf-8')
    
    print(f"   OK {name}: {len(df):,} registros -> {parquet_file.name}, {csv_file.name}")
    return len(df)

def is_feriado(fecha):
    """Determina si una fecha es feriado oficial de El Salvador"""
    mes = fecha.month
    dia = fecha.day
    año = fecha.year
    
    # Feriados fijos de El Salvador
    feriados_fijos = [
        (1, 1),   # Año Nuevo
        (5, 1),   # Día Internacional del Trabajo
        (5, 10),  # Día de la Madre
        (6, 17),  # Día del Padre
        (8, 6),   # Día de El Salvador (Fiestas Agostinas)
        (9, 15),  # Día de la Independencia
        (11, 2),  # Día de los Difuntos
        (12, 25), # Navidad
    ]
    
    if (mes, dia) in feriados_fijos:
        return True

    if mes == 3 or mes == 4:
        # Semana Santa suele caer entre marzo y abril
        # Esta es una aproximación - en producción usarías una librería de fechas religiosas
        if (mes == 3 and dia >= 25) or (mes == 4 and dia <= 15):
            # Verificar si es jueves, viernes o sábado en este rango
            if fecha.weekday() in [3, 4, 5]:  # jueves=3, viernes=4, sábado=5
                return True
    
    return False

def build_dim_fecha():
    """Construye dimensión de fechas (calendario completo)"""
    print("Construyendo dim_fecha...")
    
    # Rango de fechas: 2023-2025 (para cubrir todos los datos)
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2025, 12, 31)
    
    dates = []
    current = start_date
    
    while current <= end_date:
        dates.append({
            'id_fecha': int(current.strftime('%Y%m%d')),
            'fecha': current.strftime('%Y-%m-%d'),
            'año': current.year,
            'mes': current.month,
            'dia': current.day,
            'dia_semana': current.weekday() + 1,  # 1=Lunes, 7=Domingo
            'nombre_dia': current.strftime('%A'),
            'nombre_mes': current.strftime('%B'),
            'trimestre': (current.month - 1) // 3 + 1,
            'semana_año': current.isocalendar()[1],
            'es_fin_semana': current.weekday() >= 5,
            'es_feriado': is_feriado(current),
        })
        current += timedelta(days=1)
    
    df = pd.DataFrame(dates)
    return save_dimension(df, "dim_fecha")

def build_dim_cliente():
    """Construye dimensión de clientes"""
    print("Construyendo dim_cliente...")
    
    conn = get_oro_connection()
    
    query = """
    SELECT DISTINCT
        c.id::text as id_cliente,
        COALESCE(c.name, 'Cliente ' || c.id) as nombre,
        COALESCE(o.website_id::text, '1') as id_sitio_web,
        CASE 
            WHEN c.name LIKE '%Corp%' OR c.name LIKE '%Company%' THEN 'Corporativo'
            ELSE 'Individual'
        END as tipo_cliente,
        'Activo' as estado,
        NOW()::date as fecha_registro
    FROM oro_customer c
    LEFT JOIN oro_order o ON c.id = o.customer_id
    WHERE c.id IS NOT NULL
    ORDER BY c.id::text
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    return save_dimension(df, "dim_cliente")

def build_dim_producto():
    """Construye dimensión de productos"""
    print("Construyendo dim_producto...")
    
    conn = get_oro_connection()
    
    query = """
    SELECT DISTINCT
        p.id::text as id_producto,
        COALESCE(p.sku, 'SKU-' || p.id) as sku,
        COALESCE(p.name, 'Producto ' || p.id) as nombre,
        'Sin descripcion' as descripcion,
        COALESCE(pup.unit_code, 'unit') as unidad_medida,
        CASE 
            WHEN p.status = 'enabled' THEN 'Activo'
            ELSE 'Inactivo'
        END as estado,
        NOW()::date as fecha_creacion
    FROM oro_product p
    LEFT JOIN oro_product_unit_precision pup ON p.primary_unit_precision_id = pup.id
    WHERE p.id IS NOT NULL
    ORDER BY p.id::text
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    return save_dimension(df, "dim_producto")

def build_dim_usuario():
    """Construye dimensión de usuarios"""
    print("Construyendo dim_usuario...")
    
    conn = get_oro_connection()
    
    query = """
    SELECT DISTINCT
        u.id::text as id_usuario,
        COALESCE(u.username, 'user' || u.id) as username,
        COALESCE(u.email, 'email' || u.id || '@oro.local') as email,
        COALESCE(u.first_name, 'Nombre') as nombre,
        COALESCE(u.last_name, 'Apellido') as apellido,
        CONCAT(COALESCE(u.first_name, 'Nombre'), ' ', COALESCE(u.last_name, 'Apellido')) as nombre_completo,
        CASE 
            WHEN u.enabled = true THEN 'Activo'
            ELSE 'Inactivo'
        END as estado,
        u.createdat::date as fecha_creacion
    FROM oro_user u
    WHERE u.id IS NOT NULL
    ORDER BY id_usuario
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    return save_dimension(df, "dim_usuario")

def build_dim_sitio_web():
    """Construye dimensión de sitios web"""
    print("Construyendo dim_sitio_web...")
    
    conn = get_oro_connection()
    
    query = """
    SELECT DISTINCT
        w.id::text as id_sitio_web,
        COALESCE(w.name, 'Sitio ' || w.id) as nombre,
        'https://sitio' || w.id || '.com' as url,
        'Activo' as estado,
        NOW()::date as fecha_creacion
    FROM oro_website w
    WHERE w.id IS NOT NULL

    UNION ALL

    -- Agregar sitio web por defecto si no hay datos
    SELECT '1' as id_sitio_web, 'Sitio Principal' as nombre,
           'https://puntafina.com' as url, 'Activo' as estado,
           NOW()::date as fecha_creacion
    WHERE NOT EXISTS (SELECT 1 FROM oro_website)

    ORDER BY id_sitio_web
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    return save_dimension(df, "dim_sitio_web")

def build_dim_canal():
    """Construye dimensión de canales de venta"""
    print("Construyendo dim_canal...")
    
    conn = get_oro_connection()
    
    # Crear canales basados en datos reales
    query = """
    SELECT DISTINCT
        ROW_NUMBER() OVER (ORDER BY website_id, customer_id) as id_canal,
        CASE 
            WHEN website_id = 1 THEN 'Web Principal'
            WHEN website_id = 2 THEN 'Web Secundario'
            WHEN website_id = 3 THEN 'Mobile App'
            ELSE 'Canal ' || website_id
        END as nombre,
        CASE 
            WHEN website_id = 1 THEN 'Online'
            WHEN website_id IN (2,3) THEN 'Digital'
            ELSE 'Otros'
        END as tipo,
        'Activo' as estado
    FROM oro_order
    WHERE website_id IS NOT NULL
    
    UNION ALL
    
    -- Canal por defecto
    SELECT 1 as id_canal, 'Canal Principal' as nombre, 'Online' as tipo, 'Activo' as estado
    WHERE NOT EXISTS (SELECT 1 FROM oro_order WHERE website_id IS NOT NULL)
    
    ORDER BY id_canal
    LIMIT 10
    """
    
    df = pd.read_sql(query, conn)
    df['id_canal'] = df['id_canal'].astype(str)
    conn.close()
    
    return save_dimension(df, "dim_canal")

def build_dim_direccion():
    """Construye dimensión de direcciones"""
    print("Construyendo dim_direccion...")
    
    conn = get_oro_connection()
    
    query = """
    SELECT DISTINCT
        a.id::text as id_direccion,
        COALESCE(a.street, 'Calle Sin Nombre') as calle,
        COALESCE(a.city, 'Ciudad') as ciudad,
        COALESCE(a.postal_code, '00000') as codigo_postal,
        COALESCE(a.region_text, 'Región') as region,
        'US' as pais_codigo,
        CONCAT(
            COALESCE(a.street, 'Calle Sin Nombre'), ', ',
            COALESCE(a.city, 'Ciudad'), ', ',
            COALESCE(a.region_text, 'Región'), ' ',
            COALESCE(a.postal_code, '00000')
        ) as direccion_completa,
        CASE 
            WHEN a.id IS NOT NULL THEN 'Activa'
            ELSE 'Inactiva'
        END as estado
    FROM oro_address a
    WHERE a.id IS NOT NULL
    ORDER BY id_direccion
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    # Agregar dirección por defecto con id_direccion = '0'
    direccion_default = pd.DataFrame([{
        'id_direccion': '0',
        'calle': 'Sin Especificar',
        'ciudad': 'Sin Especificar', 
        'codigo_postal': '00000',
        'region': 'Sin Especificar',
        'pais_codigo': 'US',
        'direccion_completa': 'Dirección no especificada',
        'estado': 'Activa'
    }])
    
    df = pd.concat([direccion_default, df], ignore_index=True)
    
    return save_dimension(df, "dim_direccion")

def build_dim_envio():
    """Construye dimensión de métodos de envío"""
    print("Construyendo dim_envio...")
    
    conn = get_oro_connection()
    
    # Crear métodos de envío realistas
    query = """
    SELECT DISTINCT
        ROW_NUMBER() OVER (ORDER BY id) as id_envio,
        CASE 
            WHEN id % 4 = 1 THEN 'Envío Estándar'
            WHEN id % 4 = 2 THEN 'Envío Express'
            WHEN id % 4 = 3 THEN 'Envío Premium'
            ELSE 'Recogida en Tienda'
        END as metodo_envio,
        CASE 
            WHEN id % 4 = 1 THEN '5-7 días'
            WHEN id % 4 = 2 THEN '2-3 días'
            WHEN id % 4 = 3 THEN '1-2 días'
            ELSE 'Inmediato'
        END as tiempo_entrega,
        CASE 
            WHEN id % 4 = 1 THEN 5.99
            WHEN id % 4 = 2 THEN 12.99
            WHEN id % 4 = 3 THEN 24.99
            ELSE 0.00
        END as costo,
        'Activo' as estado
    FROM oro_shipping_product_opts
    WHERE id IS NOT NULL
    
    UNION ALL
    
    -- Métodos por defecto si no hay datos
    SELECT 1 as id_envio, 'Envío Estándar' as metodo_envio, '5-7 días' as tiempo_entrega, 5.99 as costo, 'Activo' as estado
    WHERE NOT EXISTS (SELECT 1 FROM oro_shipping_product_opts)
    
    ORDER BY id_envio
    LIMIT 20
    """
    
    df = pd.read_sql(query, conn)
    df['id_envio'] = df['id_envio'].astype(str)
    conn.close()
    
    return save_dimension(df, "dim_envio")

def build_dim_pago():
    """Construye dimensión de métodos y estados de pago"""
    print("Construyendo dim_pago...")
    
    conn = get_oro_connection()
    
    query = """
    SELECT DISTINCT
        pt.id::text as id_pago,
        COALESCE(pt.payment_method, 'Método ' || pt.id) as metodo_pago,
        'Completado' as estado_pago,
        pt.amount::numeric as monto,
        pt.currency as moneda,
        pt.created_at::date as fecha_transaccion
    FROM oro_payment_transaction pt
    LEFT JOIN oro_payment_status ps ON pt.id = ps.id
    WHERE pt.id IS NOT NULL
    ORDER BY id_pago
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    return save_dimension(df, "dim_pago")

def build_dim_impuestos():
    """Construye dimensión de impuestos"""
    print("Construyendo dim_impuestos...")
    
    conn = get_oro_connection()
    
    query = """
    SELECT DISTINCT
        t.id::text as id_impuestos,
        COALESCE(t.code, 'TAX' || t.id) as codigo_impuesto,
        COALESCE(t.description, 'Impuesto ' || t.id) as descripcion,
        COALESCE(t.rate, 0.0)::numeric as tasa,
        'Activo' as estado
    FROM oro_tax t
    WHERE t.id IS NOT NULL
    
    UNION ALL
    
    -- Impuesto por defecto
    SELECT '1' as id_impuestos, 'IVA' as codigo_impuesto, 'Impuesto al Valor Agregado' as descripcion, 
           0.16 as tasa, 'Activo' as estado
    WHERE NOT EXISTS (SELECT 1 FROM oro_tax)
    
    ORDER BY id_impuestos
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    return save_dimension(df, "dim_impuestos")

def build_dim_promocion():
    """Construye dimensión de promociones"""
    print("Construyendo dim_promocion...")
    
    conn = get_oro_connection()
    
    query = """
    SELECT DISTINCT
        p.id::text as id_promocion,
        'Promoción ' || p.id as nombre_promocion,
        CASE 
            WHEN p.use_coupons THEN 'Promoción con cupón'
            ELSE 'Promoción directa'
        END as descripcion,
        COALESCE(AVG(pad.amount), 0.0) as descuento_monto,
        CASE 
            WHEN AVG(pad.amount) > 100 THEN 'fixed'
            WHEN AVG(pad.amount) > 0 THEN 'percentage'
            ELSE 'fixed'
        END as tipo_descuento,
        'Activa' as estado
    FROM oro_promotion p
    LEFT JOIN oro_promotion_applied_discount pad ON p.id = pad.applied_promotion_id
    WHERE p.id IS NOT NULL
    GROUP BY p.id, p.use_coupons
    ORDER BY id_promocion
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    # Agregar promoción por defecto 'SIN_PROMO'
    sin_promo_row = pd.DataFrame([{
        'id_promocion': 'SIN_PROMO',
        'nombre_promocion': 'Sin Promoción',
        'descripcion': 'Sin promoción aplicada',
        'descuento_monto': 0.0,
        'tipo_descuento': 'none',
        'estado': 'Activa'
    }])
    
    df = pd.concat([df, sin_promo_row], ignore_index=True)
    
    return save_dimension(df, "dim_promocion")

def build_dim_orden():
    """Construye dimensión de órdenes (desnormalizada)"""
    print("Construyendo dim_orden...")
    
    conn = get_oro_connection()
    
    query = """
    SELECT DISTINCT
        o.id::text as id_orden,
        o.identifier as numero_orden,
        o.po_number as numero_po,
        COALESCE(c.name, 'Cliente ' || o.customer_id) as cliente_nombre,
        COALESCE(u.first_name || ' ' || u.last_name, 'Usuario ' || o.customer_user_id) as usuario_nombre_completo,
        COALESCE(w.name, 'Sitio ' || o.website_id) as sitio_web_nombre,
        COALESCE(o.subtotal_value, 0.0) as subtotal,
        COALESCE(o.total_value, 0.0) as total,
        o.currency as moneda,
        o.created_at::date as fecha_orden,
        o.updated_at::date as fecha_actualizacion,
        'Básico' as categoria_orden
    FROM oro_order o
    LEFT JOIN oro_customer c ON o.customer_id = c.id
    LEFT JOIN oro_customer_user cu ON o.customer_user_id = cu.id
    LEFT JOIN oro_user u ON cu.owner_id = u.id
    LEFT JOIN oro_website w ON o.website_id = w.id
    WHERE o.id IS NOT NULL
    ORDER BY id_orden
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    return save_dimension(df, "dim_orden")

def build_dim_line_item():
    """Construye dimensión de line items"""
    print("Construyendo dim_line_item...")
    
    conn = get_oro_connection()
    
    query = """
    SELECT DISTINCT
        oli.id::text as id_line_item,
        oli.order_id::text as id_orden,
        oli.product_id::text as id_producto,
        COALESCE(p.sku, 'SKU-' || oli.product_id) as producto_sku,
        COALESCE(p.name, 'Producto ' || oli.product_id) as producto_nombre,
        oli.quantity::numeric as cantidad,
        oli.value::numeric as precio_unitario,
        (oli.quantity * oli.value)::numeric as total_linea,
        oli.currency as moneda,
        COALESCE(pu.code, 'unit') as unidad
    FROM oro_order_line_item oli
    LEFT JOIN oro_product p ON oli.product_id = p.id
    LEFT JOIN oro_product_unit pu ON oli.product_unit_id = pu.code
    WHERE oli.id IS NOT NULL
    ORDER BY id_line_item
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    return save_dimension(df, "dim_line_item")

def main():
    """Función principal que construye todas las dimensiones"""
    print("CONSTRUCCION UNIFICADA DE DIMENSIONES")
    print("=" * 60)
    print(f"Outputs: {PARQUET_DIR} | {CSV_DIR}")
    print()
    
    # Construir todas las dimensiones
    dimensions_built = {}
    
    try:
        dimensions_built['dim_fecha'] = build_dim_fecha()
        dimensions_built['dim_cliente'] = build_dim_cliente()
        dimensions_built['dim_producto'] = build_dim_producto()
        dimensions_built['dim_usuario'] = build_dim_usuario()
        dimensions_built['dim_sitio_web'] = build_dim_sitio_web()
        dimensions_built['dim_canal'] = build_dim_canal()
        dimensions_built['dim_direccion'] = build_dim_direccion()
        dimensions_built['dim_envio'] = build_dim_envio()
        dimensions_built['dim_pago'] = build_dim_pago()
        dimensions_built['dim_impuestos'] = build_dim_impuestos()
        dimensions_built['dim_promocion'] = build_dim_promocion()
        dimensions_built['dim_orden'] = build_dim_orden()
        dimensions_built['dim_line_item'] = build_dim_line_item()
        
    except Exception as e:
        print(f"Error construyendo dimensiones: {e}")
        return False
    
    # Resumen final
    print("\nRESUMEN DE DIMENSIONES CONSTRUIDAS:")
    total_records = 0
    for dim_name, count in dimensions_built.items():
        total_records += count
        print(f"   {dim_name}: {count:,} registros")
    
    print(f"\nCONSTRUCCION COMPLETADA!")
    print(f"   {len(dimensions_built)} dimensiones construidas")
    print(f"   {total_records:,} registros totales")
    print(f"   Archivos guardados en: parquet/ y csv/")
    
    return True

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)