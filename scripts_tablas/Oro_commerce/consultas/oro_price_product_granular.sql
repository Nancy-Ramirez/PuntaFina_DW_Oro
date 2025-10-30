DROP VIEW IF EXISTS oro_price_product_granular;

CREATE VIEW oro_price_product_granular AS
SELECT
    /* === Identificador de la fila (uuid) === */
    pp.id,

    /* === Regla de precio (FK) + texto === */
    pp.price_rule_id,
    COALESCE('Regla #' || pr.id::text, 'Sin regla')::varchar(255) AS price_rule_txt,

    /* === Unidad (FK por código) + texto === */
    pp.unit_code,
    COALESCE(NULLIF(pp.unit_code, ''), 'Sin unidad')::varchar(255) AS unit_name,

    /* === Producto (FK) + texto === */
    pp.product_id,
    COALESCE(NULLIF(p.name, ''), 'Sin nombre')::varchar(255) AS product_name,

    /* === Lista de precios (FK) + texto === */
    pp.price_list_id,
    COALESCE(NULLIF(pl.name, ''), 'Sin lista de precios')::varchar(255) AS price_list_name,

    /* === Otros campos nativos === */
    COALESCE(NULLIF(pp.product_sku, ''), 'Sin SKU')::varchar(255) AS product_sku,
    pp.quantity,
    pp.value,
    pp.currency,
    pp.version,

    /* JSON siempre no-nulo */
    COALESCE(pp.serialized_data, '{}'::jsonb) AS serialized_data

FROM public.oro_price_product pp
LEFT JOIN public.oro_price_rule  pr ON pr.id  = pp.price_rule_id
LEFT JOIN public.oro_product     p  ON p.id   = pp.product_id
LEFT JOIN public.oro_price_list  pl ON pl.id  = pp.price_list_id
LEFT JOIN public.oro_product_unit pu ON pu.code = pp.unit_code;  -- opcional, por si luego quieres precisión por unidad

SELECT * FROM oro_price_product_granular;