CREATE OR REPLACE VIEW oro_promotion_granular AS
SELECT
    /* ===== PK ===== */
    p.id,

    /* ===== FK + legible: Organización ===== */
    p.organization_id,
    COALESCE(NULLIF(org.name, ''), 'Sin organización')::varchar(255) AS organization_name,

    /* ===== FK + legible: Configuración de descuento ===== */
    p.discount_config_id,
    CASE
        WHEN p.discount_config_id IS NOT NULL THEN ('Config #' || p.discount_config_id::text)
        ELSE 'Sin config de descuento'
    END::varchar(255) AS discount_config_txt,

    /* ===== FK + legible: Regla ===== */
    p.rule_id,
    CASE
        WHEN p.rule_id IS NOT NULL THEN ('Regla #' || p.rule_id::text)
        ELSE 'Sin regla'
    END::varchar(255) AS rule_txt,

    /* ===== FK + legible: Segmento de productos ===== */
    p.products_segment_id,
    CASE
        WHEN p.products_segment_id IS NOT NULL THEN ('Segmento #' || p.products_segment_id::text)
        ELSE 'Sin segmento'
    END::varchar(255) AS products_segment_txt,

    /* ===== FK + legible: Propietario (usuario interno) ===== */
    p.user_owner_id,
    COALESCE(NULLIF(u.username, ''), 'Sin propietario')::varchar(255) AS user_owner,

    /* ===== Booleano + legible ===== */
    COALESCE(p.use_coupons, false) AS use_coupons,
    CASE WHEN COALESCE(p.use_coupons, false)
         THEN 'Usa cupones' ELSE 'Sin cupones' END::varchar(20) AS use_coupons_txt,

    /* ===== Fechas ===== */
    p.created_at,
    p.updated_at,

    /* ===== JSONB original + legible ===== */
    p.serialized_data,
    COALESCE(p.serialized_data::text, 'Sin datos')::text AS serialized_data_txt

FROM public.oro_promotion p
LEFT JOIN public.oro_organization org
       ON org.id = p.organization_id
LEFT JOIN public.oro_user u
       ON u.id = p.user_owner_id;

SELECT * FROM oro_promotion_granular;
