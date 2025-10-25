DROP VIEW IF EXISTS vw_oro_price_product_granular;

CREATE VIEW vw_oro_price_product_granular AS
SELECT
    /* === mismas columnas de la tabla base, más product_name pegado a product_id === */
    pp.id,  -- uuid

    COALESCE('Regla #' || pr.id::text, 'Sin regla')::varchar(255)              AS price_rule_id,
    COALESCE(NULLIF(pp.unit_code, ''), 'Sin unidad')::varchar(255)             AS unit_code,

    pp.product_id,  -- se conserva el entero original
    COALESCE(NULLIF(p.name, ''), 'Sin nombre')::varchar(255)                   AS product_name, 

    COALESCE(NULLIF(pl.name, ''), 'Sin lista de precios')::varchar(255)        AS price_list_id,
    COALESCE(NULLIF(pp.product_sku, ''), 'Sin SKU')::varchar(255)              AS product_sku,

    pp.quantity,
    pp.value,
    pp.currency,
    pp.version,

    COALESCE(pp.serialized_data, '{}'::jsonb)                                  AS serialized_data
FROM public.oro_price_product pp
LEFT JOIN public.oro_price_rule  pr ON pr.id  = pp.price_rule_id
LEFT JOIN public.oro_product     p  ON p.id   = pp.product_id
LEFT JOIN public.oro_price_list  pl ON pl.id  = pp.price_list_id
LEFT JOIN public.oro_product_unit pu ON pu.code = pp.unit_code; 