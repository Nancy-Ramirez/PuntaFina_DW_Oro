# PuntaFina DW (OroCommerce + OroCRM) – ETL local

> **Objetivo:** construir el modelo estrella de Ventas–Inventario–Finanzas usando tus consultas granulares y un ETL local en Spark (sin S3).

## Estructura de carpetas
```
PuntaFina_DW_Oro/
├─ README.md
├─ config/
│  ├─ .env.sample
│  └─ settings.yaml
├─ sql/
│  ├─ granular/                # aquí van tus *.sql granulares
│  └─ helpers/                 # dim_fecha u otras utilidades
├─ scripts/
│  ├─ run_granular_sql.py      # ejecuta los SQL granulares (placeholder)
│  ├─ build_dimensions.py      # arma dim_*
│  ├─ build_facts.py           # arma fact_*
│  └─ quality_checks.py        # reglas simples de calidad
├─ etl/
│  ├─ lib/                     # utilidades comunes (placeholder)
│  └─ checkpoints/             # marcas incrementales (si decides usarlas)
├─ data/
│  ├─ inputs/                  # backups .backup o CSV auxiliares
│  ├─ intermediate/            # materializaciones temporales (opcional)
│  └─ outputs/
│     ├─ parquet/              # salida parquet por tabla DW
│     └─ csv/                  # export opcional a CSV
├─ logs/
└─ docs/
   ├─ diagrama_estrella.png
   └─ diccionario_campos.md
```

## Orden de ejecución (local)
1. Copia `config/.env.sample` a `config/.env` y completa credenciales de OroCommerce/OroCRM.
2. Coloca tus `*.sql` granulares en `sql/granular/`.
3. Ejecuta (placeholder):  
   ```bash
   # 1) SQL granulares
   python scripts/run_granular_sql.py
   # 2) Dimensiones
   python scripts/build_dimensions.py
   # 3) Hechos
   python scripts/build_facts.py
   # 4) Calidad
   python scripts/quality_checks.py
   ```
4. Los resultados aparecerán en `data/outputs/parquet/` y `csv/`.

## Checklists
- **Dimensiones**: dim_producto, dim_cliente, dim_usuario, dim_sitio_web, dim_canal, dim_precio_lista, dim_promocion, dim_pago, dim_fecha.
- **Hechos**: fact_ventas_linea (grano: pedido×línea×SKU×unidad), fact_pago (transacción), fact_inventario_snapshot (si aplicas snapshot).
- **Reglas de calidad** (mínimas): 
  - Suma de líneas ≈ totales de pedido.
  - Llaves naturales no nulas (producto, cliente, fecha).
  - Montos ≥ 0.
  - Dominio de estatus/métodos de pago conocido.

## Notas
- Mantengamos nombres sencillos y legibles.
