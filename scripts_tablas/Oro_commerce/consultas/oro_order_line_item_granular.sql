CREATE OR REPLACE VIEW oro_order_line_item_granular AS
SELECT
    oli.id,
    COALESCE(pu.code, 'Sin unidad de producto') AS product_unit_id,
    COALESCE(p.name, 'Sin producto') AS product_id,
    COALESCE(pp.name, 'Sin producto padre') AS parent_product_id,
    COALESCE(o.id::text, 'Sin orden') AS order_id,  -- se usa id de la orden como texto
    COALESCE(oli.product_sku, 'Sin SKU') AS product_sku,
    COALESCE(oli.product_name, 'Sin nombre de producto') AS product_name,
    COALESCE(oli.product_variant_fields, 'Sin variante') AS product_variant_fields,
    COALESCE(oli.free_form_product, 'Sin producto libre') AS free_form_product,
    oli.quantity,
    COALESCE(oli.product_unit_code, 'Sin código de unidad') AS product_unit_code,
    COALESCE(oli.value, 0) AS value,
    COALESCE(oli.currency, 'Sin moneda') AS currency,
    oli.price_type,
    COALESCE(oli.ship_by, NULL) AS ship_by,
    oli.from_external_source,
    COALESCE(oli.comment, 'Sin comentario') AS comment,
    COALESCE(oli.shipping_method, 'Sin método de envío') AS shipping_method,
    COALESCE(oli.shipping_method_type, 'Sin tipo de envío') AS shipping_method_type,
    COALESCE(oli.shipping_estimate_amount, 0) AS shipping_estimate_amount,
    oli.checksum,
    COALESCE(oli.serialized_data::text, '{}') AS serialized_data
FROM public.oro_order_line_item oli
LEFT JOIN public.oro_product_unit pu
    ON oli.product_unit_id = pu.code
LEFT JOIN public.oro_product p
    ON oli.product_id = p.id
LEFT JOIN public.oro_product pp
    ON oli.parent_product_id = pp.id
LEFT JOIN public.oro_order o
    ON oli.order_id = o.id
ORDER BY oli.id;

SELECT * FROM oro_order_line_item_granular LIMIT 10;
SELECT * FROM oro_order_line_item LIMIT 10;
