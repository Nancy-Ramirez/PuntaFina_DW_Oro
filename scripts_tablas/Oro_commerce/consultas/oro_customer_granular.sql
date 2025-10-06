CREATE OR REPLACE VIEW vw_oro_customer AS
SELECT 
    c.id,
    COALESCE(p.name, 'Sin cliente padre') AS parent_id,
    COALESCE(g.name, 'Sin grupo') AS group_id,
    COALESCE(u.username, 'Sin propietario') AS owner_id,
    COALESCE(o.name, 'Sin organización') AS organization_id,
    COALESCE(r.name, 'Sin calificación interna') AS internal_rating_id,
    c.name,
    c.created_at,
    c.updated_at,
    COALESCE(pt.label, 'Sin término de pago') AS payment_term_7c4f1e8e_id,
    COALESCE(t.code, 'Sin código fiscal') AS taxcode_id,
    COALESCE(a.name, 'Sin cuenta previa') AS previous_account_id,
    c.lifetime,
    COALESCE(ch.name, 'Sin canal') AS datachannel_id,
    c.serialized_data
FROM oro_customer c
    LEFT JOIN oro_customer p ON c.parent_id = p.id
    LEFT JOIN oro_customer_group g ON c.group_id = g.id
    LEFT JOIN oro_user u ON c.owner_id = u.id
    LEFT JOIN oro_organization o ON c.organization_id = o.id
    LEFT JOIN oro_enum_acc_internal_rating r ON c.internal_rating_id = r.id
    LEFT JOIN oro_payment_term pt ON c.payment_term_7c4f1e8e_id = pt.id
    LEFT JOIN oro_tax_customer_tax_code t ON c.taxcode_id = t.id
    LEFT JOIN orocrm_account a ON c.previous_account_id = a.id
    LEFT JOIN orocrm_channel ch ON c.datachannel_id = ch.id;

SELECT * FROM vw_oro_customer LIMIT 10;
SELECT * FROM oro_customer LIMIT 10;
