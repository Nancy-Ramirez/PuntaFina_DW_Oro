# Data Warehouse PuntaFina

## Descripción General

Este repositorio implementa una solución completa de data warehouse para el análisis de comercio electrónico de PuntaFina. El sistema extrae datos transaccionales de OroCommerce y los transforma en un modelo dimensional apropiado para inteligencia de negocios y reportes.

## Arquitectura

El data warehouse sigue un diseño de esquema estrella con una tabla de hechos central y trece tablas de dimensiones. Todos los datos se procesan a través de un pipeline ETL basado en Python que maneja la extracción desde sistemas fuente, transformación con lógica de negocio, y carga hacia PostgreSQL.

## Componentes Principales

### Scripts del Pipeline ETL

**build_all_dimensions.py**
Construye todas las tablas de dimensiones desde la base de datos fuente de OroCommerce. Procesa datos de clientes, catálogos de productos, información de usuarios y otros datos de referencia en estructuras dimensionales estandarizadas. Genera archivos Parquet para almacenamiento intermedio y archivos CSV para validación de datos.

**build_fact_ventas.py**  
Construye la tabla de hechos central que contiene datos de transacciones de ventas a nivel de línea de pedido. Incluye cálculos de descuentos promocionales, mapeo de llaves foráneas y validaciones de calidad de datos. Procesa más de 600,000 registros de transacciones con integridad referencial completa.

**setup_database.py**
Administra la base de datos PostgreSQL del data warehouse. Crea el esquema de la base de datos objetivo, carga datos desde archivos Parquet usando estrategias de carga incremental, establece restricciones de llaves foráneas y crea índices de rendimiento. Soporta tanto modos de recarga completa como carga incremental.

**orquestador_maestro.py**
Orquesta el flujo de trabajo completo del ETL. Ejecuta la construcción de dimensiones, construcción de tabla de hechos y carga de base de datos en secuencia apropiada con manejo integral de errores y registro de progreso.

### Modelo de Datos

**Tabla de Hechos: fact_ventas**
Contiene 613,005 registros de transacciones de ventas con 26 campos incluyendo cantidades, precios, descuentos y llaves foráneas hacia todas las tablas de dimensiones. La granularidad está a nivel de línea de pedido para máxima flexibilidad analítica.

**Tablas de Dimensiones**
- dim_cliente (437,514 registros): Datos maestros de clientes
- dim_producto (65 registros): Información del catálogo de productos  
- dim_fecha (796 registros): Dimensión de fechas con atributos de calendario fiscal
- dim_usuario (54 registros): Información de usuarios de ventas
- dim_orden (200,097 registros): Datos de encabezado de órdenes
- dim_line_item (613,005 registros): Detalles de líneas de pedido
- dim_direccion (980,066 registros): Información de direcciones de envío
- Dimensiones adicionales para canales, pagos, impuestos, promociones y métodos de envío

### Características de la Base de Datos

**Integridad Referencial**
Las 13 relaciones de llaves foráneas están implementadas y forzadas a nivel de base de datos. El sistema automáticamente maneja registros huérfanos creando entradas de dimensiones por defecto cuando es necesario.

**Carga Incremental**
El proceso de carga utiliza la funcionalidad ON CONFLICT de PostgreSQL para realizar actualizaciones incrementales. Solo se insertan registros nuevos durante ejecuciones rutinarias del ETL, preservando datos existentes y mejorando el rendimiento.

**Optimización de Rendimiento**
Incluye 11 índices estratégicos en campos consultados comúnmente y relaciones de llaves foráneas. El rendimiento de consultas está optimizado para patrones típicos de acceso de inteligencia de negocios.

## Instalación y Configuración

### Prerrequisitos

- Python 3.7 o superior
- PostgreSQL 12 o superior
- Paquetes de Python requeridos: pandas, psycopg2, pyarrow, python-dotenv, pyyaml

### Configuración

Crear un archivo de configuración en `config/.env` con los parámetros de conexión de base de datos:

```
# Source OroCommerce Database
ORO_DB_HOST=your_oro_host
ORO_DB_PORT=5432
ORO_DB_NAME=oro_database
ORO_DB_USER=oro_user
ORO_DB_PASS=oro_password

# Target Data Warehouse Database  
DW_ORO_DB_HOST=your_dw_host
DW_ORO_DB_PORT=5432
DW_ORO_DB_NAME=DW_oro
DW_ORO_DB_USER=dw_user
DW_ORO_DB_PASS=dw_password
```

### Ejecución

Ejecutar el pipeline ETL completo:

```bash
cd scripts
python orquestador_maestro.py
```

Para componentes individuales:

```bash
python build_all_dimensions.py
python build_fact_ventas.py
python setup_database.py
```

## Volumen de Datos y Rendimiento

El sistema procesa aproximadamente 2.8 millones de registros a través de todas las tablas con las siguientes características de rendimiento:

- Construcción de dimensiones: 60-90 segundos
- Construcción de tabla de hechos: 45-60 segundos  
- Carga de base de datos: 120-180 segundos
- Ejecución total del pipeline: 4-5 minutos

## Métricas de Negocio

El data warehouse habilita análisis de:

- Volumen de ventas: $736 millones en valor de transacciones
- Comportamiento del cliente: 437,000+ clientes únicos
- Rendimiento de productos: Analítica detallada a nivel SKU
- Análisis geográfico: Datos completos a nivel de dirección
- Efectividad promocional: Medición de impacto de descuentos
- Tendencias temporales: Historial de transacciones multi-año

## Estructura de Archivos

```
PuntaFina_DW_Oro/
├── scripts/                 # Scripts del pipeline ETL
├── config/                  # Archivos de configuración
├── data/outputs/           # Archivos Parquet y CSV generados
├── docs/                   # Documentación y diccionario de datos
├── logs/                   # Logs de ejecución
└── sql/                    # Consultas SQL de referencia
```

## Registro y Monitoreo

Todas las ejecuciones del pipeline generan logs detallados en el directorio `logs/`. Los archivos de log incluyen timestamps, conteos de registros, métricas de calidad de datos y detalles de errores para resolución de problemas y propósitos de auditoría.

## Mantenimiento

El sistema está diseñado para operación automatizada con requerimientos mínimos de mantenimiento. El enfoque de carga incremental reduce el tiempo de procesamiento para actualizaciones rutinarias mientras preserva el historial de datos para continuidad analítica.
- **15 Foreign Keys** en fact_ventas
- **Índices optimizados** para consultas BI

¡Listo para conectar Power BI, Tableau o cualquier herramienta de análisis!