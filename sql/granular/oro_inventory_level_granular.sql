CREATE OR REPLACE VIEW oro_inventory_level_granular AS
SELECT
    /* ===== PK ===== */
    il.id,

    /* ===== FK + legible: Organización ===== */
    il.organization_id,
    COALESCE(NULLIF(org.name, ''), 'Sin organización')::varchar(255) AS organization_name,

    /* ===== FK + legible: Producto ===== */
    il.product_id,
    -- Etiqueta combinando SKU + Nombre
    (
      COALESCE(NULLIF(p.sku, ''), 'SKU desconocido') || ' - ' ||
      COALESCE(NULLIF(p.name, ''), 'Producto sin nombre')
    )::varchar(510) AS product_label,
    -- Extras útiles del producto (opcionales, por si los quieres en reportes)
    COALESCE(NULLIF(p.sku,  ''), 'SKU desconocido')::varchar(255)  AS product_sku,
    COALESCE(NULLIF(p.name, ''), 'Producto sin nombre')::varchar(255) AS product_name,

    /* ===== FK + legible: Unidad / precisión ===== */
    il.product_unit_precision_id,
    COALESCE(NULLIF(up.unit_code, ''), 'Sin unidad')::varchar(50) AS unit_code,
    COALESCE(up.unit_precision, 0) AS unit_precision,
    (
      'Precisión ' || COALESCE(up.unit_precision, 0)::text
    )::varchar(32) AS unit_precision_txt,
    -- Si te sirve para análisis/conversiones:
    COALESCE(up.conversion_rate, 1)::numeric(20,10) AS conversion_rate,
    COALESCE(up.sell, false) AS unit_sellable,

    /* ===== Métrica ===== */
    il.quantity,

    /* ===== JSONB original + legible ===== */
    il.serialized_data,
    COALESCE(il.serialized_data::text, 'Sin datos')::text AS serialized_data_txt

FROM public.oro_inventory_level il
LEFT JOIN public.oro_organization         org ON org.id = il.organization_id
LEFT JOIN public.oro_product              p   ON p.id   = il.product_id
LEFT JOIN public.oro_product_unit_precision up ON up.id = il.product_unit_precision_id;


SELECT * FROM oro_inventory_level_granular;