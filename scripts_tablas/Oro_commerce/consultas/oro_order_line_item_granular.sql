DROP VIEW IF EXISTS oro_order_line_item_granular;

CREATE VIEW oro_order_line_item_granular AS
SELECT
    -- === columnas originales + etiquetas a la par de cada FK ===
    oli.id,

    oli.product_unit_id,
    COALESCE(
      CASE
        WHEN pu.code IS NOT NULL
          THEN 'Unidad ' || pu.code || ' (prec ' || pu.default_precision::text || ')'
      END,
      'Sin unidad de producto'
    ) AS product_unit_label,

    oli.product_id,
    COALESCE(p.name, 'Sin producto') AS product_name_fk,

    oli.parent_product_id,
    COALESCE(pp.name, 'Sin producto padre') AS parent_product_name,

    oli.order_id,
    COALESCE(o.identifier, 'Sin orden') AS order_identifier,

    -- === resto de columnas (sin tocar tipos/valores originales) ===
    oli.product_sku,
    oli.product_name,
    oli.product_variant_fields,
    oli.free_form_product,
    oli.quantity,
    oli.product_unit_code,
    oli.value,
    oli.currency,
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

SELECT * FROM oro_order_line_item_granular LIMIT 10;
