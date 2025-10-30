# Diccionario de campos (borrador)

- **fact_ventas_linea** (grano: pedido × línea × SKU × unidad): id_fact, order_id, line_item_id, id_producto, id_cliente, id_fecha, id_usuario, id_canal, id_sitio_web, id_precio_lista, id_promocion, id_pago, id_envio, cantidad, precio_unitario, subtotal, descuento_total, impuestos, total_pagado.

- **dim_producto**: id_producto, sku, nombre, descripcion, categoria, unidad_venta, estado_inventario, atributos_clave (talla, color, marca…).

- **dim_cliente**: id_cliente, nombre_usuario, correo, empresa, grupo_cliente, pais, ciudad.

- **dim_usuario**: id_usuario, nombre, unidad_negocio.

- **dim_sitio_web**: id_sitio_web, nombre, url.

- **dim_canal**: id_canal, nombre.

- **dim_precio_lista**: id_precio_lista, nombre_lista, moneda.

- **dim_promocion**: id_promocion, nombre, tipo_descuento, monto_descuento.

- **dim_pago**: id_pago, metodo_pago, estado_pago (estatus catálogo).

- **dim_fecha**: id_fecha, fecha, dia, mes, año, trimestre, nombre_dia.