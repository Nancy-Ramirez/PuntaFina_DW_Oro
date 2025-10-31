CREATE OR REPLACE VIEW oro_sale_quote_prod_request_granular AS
SELECT
  /* ===== PK ===== */
  r.id,

  /* ===== FK: Línea de producto ===== */
  r.quote_product_id,
  COALESCE(('QP #' || r.quote_product_id::text), 'Sin línea')::varchar(64) AS quote_product_txt,

  /* ===== FK: Item del RFP ===== */
  r.request_product_item_id,
  CASE WHEN r.request_product_item_id IS NULL
       THEN 'Sin RFP item'
       ELSE ('RFP Item #' || r.request_product_item_id::text)
  END::varchar(64) AS request_product_item_txt,

  /* ===== FK: Unidad ===== */
  r.product_unit_id,
  COALESCE(NULLIF(u.code, ''), 'Sin unidad')::varchar(255) AS product_unit_code,
  COALESCE(u.default_precision, 0) AS unit_default_precision,

  /* ===== Unidad (texto de la fila) ===== */
  COALESCE(NULLIF(r.product_unit_code, ''), 'Sin unidad')::varchar(255) AS product_unit_code_line,

  /* ===== Cantidad y precio ===== */
  COALESCE(r.quantity, 0)::double precision AS quantity,
  COALESCE(r.value, 0)::numeric(19,4)      AS unit_price_value,
  COALESCE(NULLIF(r.currency, ''), 'Sin moneda')::varchar(255) AS currency,

  /* ===== JSONB ===== */
  r.serialized_data,
  COALESCE(r.serialized_data::text, 'Sin datos')::text AS serialized_data_txt

FROM public.oro_sale_quote_prod_request r
LEFT JOIN public.oro_product_unit u ON u.code = r.product_unit_id;


SELECT * FROM oro_sale_quote_prod_request_granular;