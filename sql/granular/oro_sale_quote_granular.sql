CREATE OR REPLACE VIEW oro_sale_quote_granular AS
SELECT
  /* ===== PK ===== */
  q.id,

  /* ===== FKs + legibles ===== */
  q.customer_user_id,
  COALESCE(NULLIF(cu.email, ''), 'Sin cliente (usuario)')::varchar(255) AS customer_user_email,

  q.customer_id,
  COALESCE(NULLIF(cus.name, ''), 'Sin cliente')::varchar(255) AS customer_name,

  q.organization_id,
  COALESCE(NULLIF(org.name, ''), 'Sin organización')::varchar(255) AS organization_name,

  q.user_owner_id,
  COALESCE(NULLIF(u.username, ''), 'Sin propietario')::varchar(255) AS user_owner,

  q.website_id,
  COALESCE(NULLIF(w.name, ''), 'Sin sitio web')::varchar(255) AS website_name,

  q.request_id,
  CASE WHEN q.request_id IS NULL THEN 'Sin request' ELSE ('Request #' || q.request_id::text) END::varchar(255) AS request_txt,

  q.payment_term_7c4f1e8e_id,
  COALESCE(NULLIF(pt.label, ''), 'Sin término de pago')::varchar(255) AS payment_term_label,

  q.customer_status_id,
  COALESCE(NULLIF(cs.name, ''), 'Sin estado cliente')::varchar(255) AS customer_status_name,

  q.internal_status_id,
  COALESCE(NULLIF(is1.name, ''), 'Sin estado interno')::varchar(255) AS internal_status_name,

  q.opportunity_id,
  COALESCE(NULLIF(opp.name, ''), 'Sin oportunidad')::varchar(255) AS opportunity_name,

  /* ===== Atributos propios ===== */
  COALESCE(NULLIF(q.qid, ''), 'Sin QID')::varchar(255) AS qid,
  COALESCE(NULLIF(q.po_number, ''), 'Sin PO')::varchar(255) AS po_number,
  q.ship_until,
  q.created_at,
  q.updated_at,
  q.valid_until,

  /* Booleans + legibles */
  COALESCE(q.expired, false) AS expired,
  CASE WHEN COALESCE(q.expired,false) THEN 'Expirada' ELSE 'Vigente' END::varchar(16) AS expired_txt,

  COALESCE(q.prices_changed, false) AS prices_changed,
  CASE WHEN COALESCE(q.prices_changed,false) THEN 'Cambió precios' ELSE 'Sin cambios de precio' END::varchar(32) AS prices_changed_txt,

  COALESCE(q.shipping_method_locked, false) AS shipping_method_locked,
  CASE WHEN COALESCE(q.shipping_method_locked,false) THEN 'Método bloqueado' ELSE 'Método editable' END::varchar(24) AS shipping_method_locked_txt,

  COALESCE(q.allow_unlisted_shipping_method, false) AS allow_unlisted_shipping_method,
  CASE WHEN COALESCE(q.allow_unlisted_shipping_method,false) THEN 'Permite no listados' ELSE 'Sólo listados' END::varchar(24) AS allow_unlisted_shipping_method_txt,

  /* Envío */
  q.shipping_address_id,
  CASE WHEN q.shipping_address_id IS NULL THEN 'Sin dirección de envío' ELSE ('Dirección #' || q.shipping_address_id::text) END::varchar(255) AS shipping_address_txt,
  COALESCE(NULLIF(q.shipping_method, ''), 'Sin método de envío')::varchar(255) AS shipping_method,
  COALESCE(NULLIF(q.shipping_method_type, ''), 'Sin tipo de envío')::varchar(255) AS shipping_method_type,
  COALESCE(q.estimated_shipping_cost_amount, 0)::numeric(19,4) AS estimated_shipping_cost_amount,
  COALESCE(q.override_shipping_cost_amount, 0)::numeric(19,4) AS override_shipping_cost_amount,

  /* Moneda */
  COALESCE(NULLIF(q.currency, ''), 'Sin moneda')::varchar(3) AS currency,

  /* Activity/Contact center */
  q.ac_last_contact_date,
  q.ac_last_contact_date_in,
  q.ac_last_contact_date_out,
  COALESCE(q.ac_contact_count, 0)     AS ac_contact_count,
  COALESCE(q.ac_contact_count_in, 0)  AS ac_contact_count_in,
  COALESCE(q.ac_contact_count_out, 0) AS ac_contact_count_out,

  /* JSONB */
  q.serialized_data,
  COALESCE(q.serialized_data::text, 'Sin datos')::text AS serialized_data_txt

FROM public.oro_sale_quote q
LEFT JOIN public.oro_customer_user               cu  ON cu.id  = q.customer_user_id
LEFT JOIN public.oro_customer                    cus ON cus.id = q.customer_id
LEFT JOIN public.oro_organization                org ON org.id = q.organization_id
LEFT JOIN public.oro_user                        u   ON u.id   = q.user_owner_id
LEFT JOIN public.oro_payment_term                pt  ON pt.id  = q.payment_term_7c4f1e8e_id
LEFT JOIN public.oro_enum_quote_customer_status  cs  ON cs.id  = q.customer_status_id
LEFT JOIN public.oro_enum_quote_internal_status  is1 ON is1.id = q.internal_status_id
LEFT JOIN public.oro_website                     w   ON w.id   = q.website_id
LEFT JOIN public.orocrm_sales_opportunity        opp ON opp.id = q.opportunity_id;


SELECT * FROM oro_sale_quote_granular;