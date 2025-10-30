DROP VIEW IF EXISTS oro_promotion_applied_discount_granular;

CREATE VIEW oro_promotion_applied_discount_granular AS
SELECT
    /* === columnas originales === */
    d.id,
    d.line_item_id,
    d.applied_promotion_id,
    d.amount,
    COALESCE(NULLIF(d.currency, ''), 'Sin moneda')::varchar(10) AS currency,
    d.created_at,
    d.updated_at,
    COALESCE(d.serialized_data, '{}'::jsonb) AS serialized_data,

    /* === columnas legibles al lado de los FK === */

    /* Línea de pedido (order line item) */
    COALESCE(
        CASE
            WHEN li.id IS NOT NULL THEN
                'Orden #'  || COALESCE(li.order_id::text, '?') || ' · ' ||
                'SKU '     || COALESCE(NULLIF(li.product_sku,   ''), 'Sin SKU') || ' · ' ||
                COALESCE(NULLIF(li.product_name, ''), 'Sin producto')
            ELSE 'Sin línea'
        END,
        'Sin línea'
    )::varchar(255) AS line_item_txt,

    /* Promoción aplicada */
    COALESCE('Promoción #' || ap.id::text, 'Sin promoción')::varchar(255) AS applied_promotion_txt

FROM public.oro_promotion_applied_discount d
LEFT JOIN public.oro_order_line_item        li ON li.id = d.line_item_id
LEFT JOIN public.oro_promotion_applied      ap ON ap.id = d.applied_promotion_id;


SELECT * FROM oro_promotion_applied_discount_granular;
