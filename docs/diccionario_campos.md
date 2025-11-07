# Diccionario de Campos - Data Warehouse PuntaFina
===============================================

**Actualizado:** 6 de noviembre de 2025  
**Estado:** IMPLEMENTADO Y FUNCIONAL  
**Modelo:** Estrella con 13 dimensiones + 1 tabla de hechos

---

## 📊 TABLA DE HECHOS

### **fact_ventas** 
**Grano:** Línea de pedido (máximo detalle)  
**Registros:** 613,005 transacciones  

| Campo | Tipo | Descripción | FK hacia |
|-------|------|-------------|----------|
| `id_venta` | SERIAL PK | Clave primaria autoincremental | - |
| `id_line_item` | TEXT | ID único de línea de pedido | dim_line_item |
| `id_order` | TEXT | ID único de orden/pedido | dim_orden |
| `id_cliente` | TEXT | ID del cliente | dim_cliente |
| `id_producto` | TEXT | ID del producto | dim_producto |
| `id_usuario` | TEXT | ID del usuario vendedor | dim_usuario |
| `id_sitio_web` | TEXT | ID del sitio web/canal | dim_sitio_web |
| `id_fecha` | BIGINT | ID de fecha (YYYYMMDD) | dim_fecha |
| `id_promocion` | TEXT | ID de promoción aplicada | dim_promocion |
| `id_canal` | TEXT | ID del canal de venta | dim_canal |
| `id_direccion` | TEXT | ID de dirección de envío | dim_direccion |
| `id_envio` | TEXT | ID del método de envío | dim_envio |
| `id_impuestos` | TEXT | ID de configuración fiscal | dim_impuestos |
| `id_pago` | TEXT | ID del método de pago | dim_pago |
| `id_status_pago` | TEXT | Estado del pago | - |
| `id_metodo_pago` | TEXT | Método de pago específico | - |
| `cantidad` | NUMERIC(10,2) | Cantidad vendida | - |
| `precio_unitario` | NUMERIC(10,2) | Precio por unidad | - |
| `total_linea` | NUMERIC(15,2) | Total bruto de la línea | - |
| `descuento_promocion` | NUMERIC(15,2) | Descuento por promociones | - |
| `total_linea_neto` | NUMERIC(15,2) | Total neto después de descuentos | - |
| `subtotal_orden` | NUMERIC(15,2) | Subtotal del pedido completo | - |
| `total_orden` | NUMERIC(15,2) | Total del pedido completo | - |
| `moneda` | TEXT | Código de moneda (USD, etc.) | - |
| `numero_po` | TEXT | Número de orden de compra | - |
| `numero_orden` | TEXT | Número de orden interno | - |

**Restricciones:**  
- `UNIQUE (id_line_item, id_order)` - Evita duplicados por línea
- 13 Foreign Keys hacia todas las dimensiones

---

## 🏗️ DIMENSIONES

### **dim_fecha** (796 registros)
| Campo | Tipo | Descripción |
|-------|------|-------------|
| `id_fecha` | BIGINT PK | Fecha en formato YYYYMMDD |
| `fecha` | DATE | Fecha en formato DATE |
| `año` | INTEGER | Año (2023-2026) |
| `mes` | INTEGER | Mes (1-12) |
| `dia` | INTEGER | Día del mes (1-31) |
| `dia_semana` | INTEGER | Día de la semana (1=Lunes, 7=Domingo) |
| `nombre_dia` | VARCHAR(20) | Nombre del día (Monday, Tuesday...) |
| `nombre_mes` | VARCHAR(20) | Nombre del mes (January, February...) |
| `trimestre` | INTEGER | Trimestre (1-4) |
| `semana_año` | INTEGER | Semana del año (1-53) |
| `es_fin_semana` | BOOLEAN | True si es sábado o domingo |
| `es_feriado` | BOOLEAN | True si es día feriado |

### **dim_cliente** (437,514 registros)
| Campo | Tipo | Descripción |
|-------|------|-------------|
| `id_cliente` | TEXT PK | ID único del cliente |
| `nombre` | TEXT | Nombre o razón social del cliente |
| `id_sitio_web` | TEXT | Sitio web donde se registró |
| `tipo_cliente` | TEXT | Individual / Corporativo |
| `estado` | TEXT | Activo / Inactivo |
| `fecha_registro` | DATE | Fecha de registro del cliente |

### **dim_producto** (65 registros)
| Campo | Tipo | Descripción |
|-------|------|-------------|
| `id_producto` | TEXT PK | ID único del producto |
| `sku` | TEXT | Código SKU del producto |
| `nombre` | TEXT | Nombre del producto |
| `descripcion` | TEXT | Descripción del producto |
| `unidad_medida` | TEXT | Unidad de medida (unit, kg, etc.) |
| `estado` | TEXT | Activo / Inactivo |
| `fecha_creacion` | DATE | Fecha de creación del producto |

### **dim_usuario** (54 registros)
| Campo | Tipo | Descripción |
|-------|------|-------------|
| `id_usuario` | TEXT PK | ID único del usuario |
| `username` | TEXT | Nombre de usuario |
| `email` | TEXT | Correo electrónico |
| `nombre` | TEXT | Nombre del usuario |
| `apellido` | TEXT | Apellido del usuario |
| `nombre_completo` | TEXT | Nombre completo |
| `estado` | TEXT | Activo / Inactivo |
| `fecha_creacion` | DATE | Fecha de creación del usuario |

### **dim_sitio_web** (5 registros)
| Campo | Tipo | Descripción |
|-------|------|-------------|
| `id_sitio_web` | TEXT PK | ID único del sitio web |
| `nombre` | TEXT | Nombre del sitio web |
| `url` | TEXT | URL del sitio web |
| `estado` | TEXT | Activo / Inactivo |
| `fecha_creacion` | DATE | Fecha de creación |

### **dim_canal** (10 registros)
| Campo | Tipo | Descripción |
|-------|------|-------------|
| `id_canal` | TEXT PK | ID único del canal |
| `nombre` | TEXT | Nombre del canal de venta |
| `tipo` | TEXT | Online / Digital / Otros |
| `estado` | TEXT | Activo / Inactivo |

### **dim_direccion** (980,066 registros)
| Campo | Tipo | Descripción |
|-------|------|-------------|
| `id_direccion` | TEXT PK | ID único de la dirección |
| `calle` | TEXT | Dirección de la calle |
| `ciudad` | TEXT | Ciudad |
| `codigo_postal` | TEXT | Código postal |
| `region` | TEXT | Región o estado |
| `pais_codigo` | TEXT | Código del país |
| `direccion_completa` | TEXT | Dirección completa concatenada |
| `estado` | TEXT | Activa / Inactiva |

### **dim_envio** (20 registros)
| Campo | Tipo | Descripción |
|-------|------|-------------|
| `id_envio` | TEXT PK | ID único del método de envío |
| `metodo_envio` | TEXT | Nombre del método de envío |
| `tiempo_entrega` | TEXT | Tiempo estimado de entrega |
| `costo` | NUMERIC(10,2) | Costo del envío |
| `estado` | TEXT | Activo / Inactivo |

### **dim_pago** (97 registros)
| Campo | Tipo | Descripción |
|-------|------|-------------|
| `id_pago` | TEXT PK | ID único del pago |
| `metodo_pago` | TEXT | Método de pago utilizado |
| `estado_pago` | TEXT | Estado del pago |
| `monto` | NUMERIC(15,2) | Monto del pago |
| `moneda` | TEXT | Moneda del pago |
| `fecha_transaccion` | DATE | Fecha de la transacción |

### **dim_impuestos** (5 registros)
| Campo | Tipo | Descripción |
|-------|------|-------------|
| `id_impuestos` | TEXT PK | ID único de configuración fiscal |
| `codigo_impuesto` | TEXT | Código del impuesto |
| `descripcion` | TEXT | Descripción del impuesto |
| `tasa` | NUMERIC(5,4) | Tasa del impuesto (0.16 = 16%) |
| `estado` | TEXT | Activo / Inactivo |

### **dim_promocion** (7 registros)
| Campo | Tipo | Descripción |
|-------|------|-------------|
| `id_promocion` | TEXT PK | ID único de la promoción |
| `nombre_promocion` | TEXT | Nombre de la promoción |
| `descripcion` | TEXT | Descripción de la promoción |
| `descuento_monto` | NUMERIC(10,2) | Monto del descuento |
| `tipo_descuento` | TEXT | Tipo de descuento aplicado |
| `estado` | TEXT | Activa / Inactiva |

### **dim_orden** (200,097 registros)
| Campo | Tipo | Descripción |
|-------|------|-------------|
| `id_orden` | TEXT PK | ID único de la orden |
| `numero_orden` | TEXT | Número de orden visible |
| `numero_po` | TEXT | Número de orden de compra |
| `cliente_nombre` | TEXT | Nombre del cliente (desnormalizado) |
| `usuario_nombre_completo` | TEXT | Nombre del usuario (desnormalizado) |
| `sitio_web_nombre` | TEXT | Nombre del sitio web (desnormalizado) |
| `subtotal` | NUMERIC(15,2) | Subtotal de la orden |
| `total` | NUMERIC(15,2) | Total de la orden |
| `moneda` | TEXT | Moneda de la orden |
| `fecha_orden` | DATE | Fecha de creación de la orden |
| `fecha_actualizacion` | DATE | Fecha de última actualización |
| `categoria_orden` | TEXT | Categoría de la orden |

### **dim_line_item** (613,005 registros)
| Campo | Tipo | Descripción |
|-------|------|-------------|
| `id_line_item` | TEXT PK | ID único de la línea de pedido |
| `id_orden` | TEXT | ID de la orden padre |
| `id_producto` | TEXT | ID del producto |
| `producto_sku` | TEXT | SKU del producto (desnormalizado) |
| `producto_nombre` | TEXT | Nombre del producto (desnormalizado) |
| `cantidad` | NUMERIC(10,2) | Cantidad en la línea |
| `precio_unitario` | NUMERIC(10,2) | Precio unitario |
| `total_linea` | NUMERIC(15,2) | Total de la línea |
| `moneda` | TEXT | Moneda |
| `unidad` | TEXT | Unidad de medida |

---

## 📈 ESTADÍSTICAS DEL MODELO

- **Total registros:** 2,844,678
- **Tabla principal:** fact_ventas (613,005 transacciones)
- **Rango temporal:** 2023-11-02 a 2025-11-03
- **Total ventas:** $736,418,951.24
- **Ticket promedio:** $1,201.33
- **Foreign Keys:** 13/13 funcionando (100%)
- **Carga:** Incremental (ON CONFLICT DO NOTHING)

---

## 🔗 INTEGRIDAD REFERENCIAL

Todas las foreign keys están implementadas y funcionando:
- ✅ fact_ventas → dim_cliente
- ✅ fact_ventas → dim_producto  
- ✅ fact_ventas → dim_usuario
- ✅ fact_ventas → dim_sitio_web
- ✅ fact_ventas → dim_fecha
- ✅ fact_ventas → dim_promocion
- ✅ fact_ventas → dim_canal
- ✅ fact_ventas → dim_direccion
- ✅ fact_ventas → dim_envio
- ✅ fact_ventas → dim_impuestos
- ✅ fact_ventas → dim_pago
- ✅ fact_ventas → dim_orden
- ✅ fact_ventas → dim_line_item

---

## 🛠️ SCRIPTS DE MANTENIMIENTO

1. **build_all_dimensions.py** - Construye las 13 dimensiones
2. **build_fact_ventas.py** - Construye la tabla de hechos
3. **setup_database.py** - Carga incremental a PostgreSQL  
4. **orquestador_maestro.py** - Pipeline completo automatizado

---

**📋 Este diccionario refleja el modelo IMPLEMENTADO y FUNCIONAL del Data Warehouse PuntaFina.**