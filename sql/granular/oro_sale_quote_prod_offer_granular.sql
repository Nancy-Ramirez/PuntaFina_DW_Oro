CREATE OR REPLACE VIEW oro_sale_quote_prod_offer_granular AS
SELECT
  /* ===== PK ===== */
  o.id,

  /* ===== FK: Línea de producto ===== */
  o.quote_product_id,
  COALESCE(('QP #' || o.quote_product_id::text), 'Sin línea')::varchar(64) AS quote_product_txt,

  /* ===== FK: Unidad ===== */
  o.product_unit_id,
  COALESCE(NULLIF(u.code, ''), 'Sin unidad')::varchar(255) AS product_unit_code,
  COALESCE(u.default_precision, 0) AS unit_default_precision,

  /* ===== Unidad (texto de la fila) ===== */
  COALESCE(NULLIF(o.product_unit_code, ''), 'Sin unidad')::varchar(255) AS product_unit_code_line,

  /* ===== Cantidad y precio ===== */
  COALESCE(o.quantity, 0)::double precision AS quantity,
  COALESCE(o.value, 0)::numeric(19,4)      AS unit_price_value,
  COALESCE(NULLIF(o.currency, ''), 'Sin moneda')::varchar(255) AS currency,

  COALESCE(o.price_type, 0)::smallint AS price_type,
  ('Tipo ' || COALESCE(o.price_type,0)::text)::varchar(32) AS price_type_txt,

  /* ===== JSONB ===== */
  o.serialized_data,
  COALESCE(o.serialized_data::text, 'Sin datos')::text AS serialized_data_txt

FROM public.oro_sale_quote_prod_offer o
LEFT JOIN public.oro_product_unit u ON u.code = o.product_unit_id;


SELECT * FROM oro_sale_quote_prod_offer_granular;