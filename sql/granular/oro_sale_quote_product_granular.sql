CREATE OR REPLACE VIEW oro_sale_quote_product_granular AS
SELECT
  /* ===== PK ===== */
  qp.id,

  /* ===== FK: Cotización ===== */
  qp.quote_id,
  COALESCE(('Quote #' || qp.quote_id::text), 'Sin cotización')::varchar(255) AS quote_txt,

  /* ===== Producto principal ===== */
  qp.product_id,
  COALESCE(NULLIF(p.sku, ''), 'SKU desconocido')::varchar(255)  AS product_sku,
  COALESCE(NULLIF(p.name,''), 'Producto sin nombre')::varchar(255) AS product_name,
  (COALESCE(NULLIF(p.sku,''),'SKU desconocido') || ' - ' ||
   COALESCE(NULLIF(p.name,''),'Producto sin nombre'))::varchar(512) AS product_label,

  /* ===== Producto de reemplazo ===== */
  qp.product_replacement_id,
  COALESCE(NULLIF(pr.sku, ''), 'SKU desconocido')::varchar(255)  AS product_replacement_sku,
  COALESCE(NULLIF(pr.name,''), 'Reemplazo sin nombre')::varchar(255) AS product_replacement_name,
  (COALESCE(NULLIF(pr.sku,''),'SKU desconocido') || ' - ' ||
   COALESCE(NULLIF(pr.name,''),'Reemplazo sin nombre'))::varchar(512) AS product_replacement_label,

  /* ===== Datos libres / tipo ===== */
  COALESCE(qp.type, 0) AS type,
  ('Tipo ' || COALESCE(qp.type,0)::text)::varchar(32) AS type_txt,

  COALESCE(NULLIF(qp.product_sku, ''), 'Sin SKU en línea')::varchar(255) AS product_sku_line,
  COALESCE(NULLIF(qp.product_replacement_sku, ''), 'Sin SKU reemplazo en línea')::varchar(255) AS product_replacement_sku_line,

  COALESCE(NULLIF(qp.free_form_product, ''), 'Sin producto libre')::varchar(255) AS free_form_product,
  COALESCE(NULLIF(qp.free_form_product_replacement, ''), 'Sin reemplazo libre')::varchar(255) AS free_form_product_replacement,

  COALESCE(NULLIF(qp.comment, ''), 'Sin comentario interno')::text AS comment,
  COALESCE(NULLIF(qp.comment_customer, ''), 'Sin comentario cliente')::text AS comment_customer,

  /* JSONB */
  qp.serialized_data,
  COALESCE(qp.serialized_data::text, 'Sin datos')::text AS serialized_data_txt

FROM public.oro_sale_quote_product qp
LEFT JOIN public.oro_product p  ON p.id  = qp.product_id
LEFT JOIN public.oro_product pr ON pr.id = qp.product_replacement_id;


SELECT * FROM oro_sale_quote_product_granular;