DROP VIEW IF EXISTS oro_order_granular;

CREATE VIEW oro_order_granular AS
SELECT
    /* ===== PK ===== */
    o.id,

    /* ===== FK #1: organization_id ===== */
    o.organization_id,
    COALESCE(org.name, 'Sin organización')::varchar(255) AS organization_name,

    /* ===== FK #2: user_owner_id ===== */
    o.user_owner_id,
    COALESCE(NULLIF(u.username,''), NULLIF(u.email,''), 'Sin propietario')::varchar(255) AS user_owner_txt,

    /* ===== FK #3: shipping_address_id ===== */
    o.shipping_address_id,
    COALESCE(
      NULLIF(sa.label,''),
      NULLIF(sa.street,'') || ' ' || COALESCE(NULLIF(sa.street2,''),'') || ', ' ||
      COALESCE(NULLIF(sa.city,''),'') || ' ' || COALESCE(NULLIF(sa.postal_code,''),''),
      'Sin dirección de envío'
    )::varchar(255) AS shipping_address_txt,

    /* ===== FK #4: billing_address_id ===== */
    o.billing_address_id,
    COALESCE(
      NULLIF(ba.label,''),
      NULLIF(ba.street,'') || ' ' || COALESCE(NULLIF(ba.street2,''),'') || ', ' ||
      COALESCE(NULLIF(ba.city,''),'') || ' ' || COALESCE(NULLIF(ba.postal_code,''),''),
      'Sin dirección de cobro'
    )::varchar(255) AS billing_address_txt,

    /* ===== FK #5: website_id ===== */
    o.website_id,
    COALESCE(w.name, 'Sin sitio')::varchar(255) AS website_name,

    /* ===== FK #6: parent_id (self) ===== */
    o.parent_id,
    COALESCE(
      NULLIF(op.identifier,''),
      CASE WHEN op.id IS NOT NULL THEN 'Pedido ' || op.id::text ELSE 'Sin pedido padre' END,
      'Sin pedido padre'
    )::varchar(255) AS parent_order_txt,

    /* ===== FK #7: customer_id ===== */
    o.customer_id,
    COALESCE(cus.name, 'Sin cliente')::varchar(255) AS customer_name,

    /* ===== FK #8: customer_user_id ===== */
    o.customer_user_id,
    COALESCE(cu.email, 'Sin usuario cliente')::varchar(255) AS customer_user_email,

    /* ===== FK #9: internal_status_id (enum) ===== */
    o.internal_status_id,
    COALESCE(st.name, 'Sin estado interno')::varchar(255) AS internal_status_name,

    /* ===== FK #10: payment_term_7c4f1e8e_id ===== */
    o.payment_term_7c4f1e8e_id,
    COALESCE(pt.label, 'Sin término de pago')::varchar(255) AS payment_term_label,

    /* ===== Resto de columnas nativas ===== */
    o.uuid,
    o.identifier,
    o.created_at,
    o.updated_at,
    o.po_number,
    o.customer_notes,
    o.ship_until,
    o.currency,
    o.shipping_method,
    o.shipping_method_type,
    o.subtotal_value,
    o.subtotal_currency,
    o.base_subtotal_value,
    o.subtotal_with_discounts,
    o.total_value,
    o.total_currency,
    o.base_total_value,
    o.estimated_shipping_cost_amount,
    o.override_shipping_cost_amount,
    o.total_discounts_amount,
    o.source_entity_class,
    o.source_entity_id,
    o.source_entity_identifier,
    o.disablepromotions,
    COALESCE(o.serialized_data, '{}'::jsonb) AS serialized_data,
    o.ac_last_contact_date,
    o.ac_last_contact_date_in,
    o.ac_last_contact_date_out,
    o.ac_contact_count,
    o.ac_contact_count_in,
    o.ac_contact_count_out

FROM public.oro_order o
LEFT JOIN public.oro_organization                org ON org.id = o.organization_id
LEFT JOIN public.oro_user                         u  ON  u.id  = o.user_owner_id
LEFT JOIN public.oro_order_address               sa  ON sa.id  = o.shipping_address_id
LEFT JOIN public.oro_order_address               ba  ON ba.id  = o.billing_address_id
LEFT JOIN public.oro_website                      w  ON  w.id  = o.website_id
LEFT JOIN public.oro_order                       op  ON op.id  = o.parent_id
LEFT JOIN public.oro_customer                    cus ON cus.id = o.customer_id
LEFT JOIN public.oro_customer_user               cu  ON cu.id  = o.customer_user_id
LEFT JOIN public.oro_enum_order_internal_status  st  ON st.id  = o.internal_status_id
LEFT JOIN public.oro_payment_term                pt  ON pt.id  = o.payment_term_7c4f1e8e_id;

SELECT * FROM oro_order_granular;
