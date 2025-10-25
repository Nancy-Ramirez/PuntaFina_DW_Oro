DROP VIEW IF EXISTS vw_oro_customer_granular;

CREATE VIEW vw_oro_customer_granular AS
SELECT
    /* === columnas originales + su detalle a la par === */
    c.id,

    c.parent_id,
    COALESCE(p.name, 'Sin cliente padre')              AS parent_name,

    c.group_id,
    COALESCE(g.name, 'Sin grupo')                      AS group_name,

    c.owner_id,
    COALESCE(u.username, 'Sin propietario')            AS owner_username,

    c.organization_id,
    COALESCE(o.name, 'Sin organización')               AS organization_name,

    c.internal_rating_id,
    COALESCE(r.name, 'Sin calificación interna')       AS internal_rating_name,

    c.name,
    c.created_at,
    c.updated_at,

    c.payment_term_7c4f1e8e_id,
    COALESCE(pt.label, 'Sin término de pago')          AS payment_term_label,

    c.taxcode_id,
    COALESCE(t.code, 'Sin código fiscal')              AS taxcode_code,

    c.previous_account_id,
    COALESCE(a.name, 'Sin cuenta previa')              AS previous_account_name,

    c.lifetime,

    c.datachannel_id,
    COALESCE(ch.name, 'Sin canal')                     AS datachannel_name,

    COALESCE(c.serialized_data, '{}'::jsonb)           AS serialized_data
FROM oro_customer c
LEFT JOIN oro_customer            p   ON p.id  = c.parent_id
LEFT JOIN oro_customer_group      g   ON g.id  = c.group_id
LEFT JOIN oro_user                u   ON u.id  = c.owner_id
LEFT JOIN oro_organization        o   ON o.id  = c.organization_id
LEFT JOIN oro_enum_acc_internal_rating r ON r.id = c.internal_rating_id
LEFT JOIN oro_payment_term        pt  ON pt.id = c.payment_term_7c4f1e8e_id
LEFT JOIN oro_tax_customer_tax_code t ON t.id  = c.taxcode_id
LEFT JOIN orocrm_account          a   ON a.id  = c.previous_account_id
LEFT JOIN orocrm_channel          ch  ON ch.id = c.datachannel_id;

SELECT * FROM vw_oro_customer_granular;
