DROP VIEW IF EXISTS oro_shipping_product_opts_granular;

CREATE VIEW oro_shipping_product_opts_granular AS
SELECT
    /* ===== PK ===== */
    sp.id,

    /* ===== FK #1: freight_class_code ===== */
    sp.freight_class_code,
    COALESCE(fc.code, 'Sin clase de flete')::varchar(255) AS freight_class_name,

    /* ===== FK #2: product_id ===== */
    sp.product_id,
    COALESCE(NULLIF(p.name, ''), 'Sin producto')::varchar(255) AS product_name,
    COALESCE(NULLIF(p.sku,  ''), 'Sin SKU')::varchar(255)      AS product_sku,

    /* ===== FK #3: product_unit_code ===== */
    sp.product_unit_code,
    COALESCE(pu.code, 'Sin unidad')::varchar(255)          AS product_unit_name,

    /* ===== FK #4: dimensions_unit_code ===== */
    sp.dimensions_unit_code,
    COALESCE(lu.code, 'Sin unidad de longitud')::varchar(255) AS dimensions_unit_name,

    /* ===== FK #5: weight_unit_code ===== */
    sp.weight_unit_code,
    COALESCE(wu.code, 'Sin unidad de peso')::varchar(255)  AS weight_unit_name,

    /* ===== Otros campos ===== */
    sp.weight_value,
    sp.dimensions_length,
    sp.dimensions_width,
    sp.dimensions_height

FROM public.oro_shipping_product_opts sp
LEFT JOIN public.oro_shipping_freight_class fc ON fc.code = sp.freight_class_code
LEFT JOIN public.oro_product              p  ON p.id    = sp.product_id
LEFT JOIN public.oro_product_unit         pu ON pu.code = sp.product_unit_code
LEFT JOIN public.oro_shipping_length_unit lu ON lu.code = sp.dimensions_unit_code
LEFT JOIN public.oro_shipping_weight_unit wu ON wu.code = sp.weight_unit_code;


SELECT * FROM oro_shipping_product_opts_granular;
