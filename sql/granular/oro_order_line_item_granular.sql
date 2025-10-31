-- No cambiamos el nombre de la vista
DROP VIEW IF EXISTS oro_order_line_item_granular;

CREATE VIEW oro_order_line_item_granular AS
SELECT
    /* ===========================
       ALIASES NORMALIZADOS (ETL)
       =========================== */
    oli.id                                         AS line_item_id,      -- PK de la línea
    oli.order_id,                                                     -- FK a orden
    oli.product_id,                                                   -- FK a producto
    COALESCE(oli.product_sku, oli.product_name)    AS sku,             -- SKU visible
    COALESCE(oli.product_unit_code, oli.product_unit_id) AS product_unit, -- unidad de producto
    oli.quantity,                                                    -- cantidad de la línea
    COALESCE(oli.value, NULL)::numeric               AS price,          -- precio unitario
    NULL::numeric                                   AS row_total,      -- no presente en este dump
    NULL::numeric                                   AS discount_amount,-- no presente en este dump
    NULL::numeric                                   AS tax_amount,     -- no presente en este dump
    COALESCE(o.currency, oli.currency)              AS currency,       -- moneda de la orden/línea

    /* ===========================
       COLUMNAS ORIGINALES + ETIQUETAS
       =========================== */
    oli.id,                                                           -- id original
    oli.product_unit_id,
    COALESCE(
        CASE
            WHEN pu.code IS NOT NULL
            THEN 'Unidad ' || pu.code || ' (prec ' || pu.default_precision::text || ')'
        END,
        'Sin unidad de producto'
    )                                                AS product_unit_label,

    oli.product_id                                   AS product_id_fk,
    COALESCE(p.name, 'Sin producto')                 AS product_name_fk,

    oli.parent_product_id,
    COALESCE(pp.name, 'Sin producto padre')          AS parent_product_name,

    oli.order_id                                     AS order_id_fk,
    COALESCE(o.identifier, 'Sin orden')              AS order_identifier,

    -- Resto de columnas originales (sin alterar su semántica)
    oli.product_sku,
    oli.product_name,
    oli.product_variant_fields,
    oli.free_form_product,
    oli.product_unit_code,
    oli.value                                        AS value_raw,
    oli.currency                                     AS currency_raw,
    oli.price_type,
    oli.ship_by,
    oli.from_external_source,
    oli.comment,
    oli.shipping_method,
    oli.shipping_method_type,
    oli.shipping_estimate_amount,
    oli.checksum,
    oli.serialized_data

FROM public.oro_order_line_item oli
LEFT JOIN public.oro_product_unit pu
  ON pu.code = oli.product_unit_id
LEFT JOIN public.oro_product p
  ON p.id = oli.product_id
LEFT JOIN public.oro_product pp
  ON pp.id = oli.parent_product_id
LEFT JOIN public.oro_order o
  ON o.id = oli.order_id
ORDER BY oli.id;

-- Quick check
SELECT * FROM oro_order_line_item_granular;
