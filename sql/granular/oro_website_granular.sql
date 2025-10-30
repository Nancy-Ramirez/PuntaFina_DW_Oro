DROP VIEW IF EXISTS oro_website_granular;

CREATE VIEW oro_website_granular AS
SELECT
    /* PK */
    w.id,

    /* FK #1: organization_id */
    w.organization_id,
    COALESCE(org.name, 'Sin organización')::varchar(255) AS organization_name,

    /* FK #2: business_unit_owner_id */
    w.business_unit_owner_id,
    COALESCE(bu.name, 'Sin unidad de negocio')::varchar(255) AS business_unit_owner_name,

    /* Campos propios */
    w.name AS website_name,
    w.created_at,
    w.updated_at,
    w.is_default,

    /* FK #3: guest_role_id */
    w.guest_role_id,
    COALESCE(guest.label,
             NULLIF(guest.role, ''),
             'Sin rol invitado')::varchar(255) AS guest_role_label,

    /* FK #4: default_role_id */
    w.default_role_id,
    COALESCE(def.label,
             NULLIF(def.role, ''),
             'Sin rol predeterminado')::varchar(255) AS default_role_label,

    /* JSON nunca nulo para reportes */
    COALESCE(w.serialized_data, '{}'::jsonb) AS serialized_data

FROM public.oro_website w
LEFT JOIN public.oro_organization         org   ON org.id = w.organization_id
LEFT JOIN public.oro_business_unit        bu    ON bu.id  = w.business_unit_owner_id
LEFT JOIN public.oro_customer_user_role   guest ON guest.id = w.guest_role_id
LEFT JOIN public.oro_customer_user_role   def   ON def.id   = w.default_role_id;

SELECT * FROM oro_website_granular;
