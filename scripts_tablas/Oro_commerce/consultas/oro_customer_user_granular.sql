CREATE OR REPLACE VIEW oro_customer_user_granular AS
SELECT
    u.id,
    COALESCE(ou.username, 'Sin propietario') AS owner_id,
    COALESCE(org.name, 'Sin organización') AS organization_id,
    COALESCE(c.name, 'Sin cliente') AS customer_id,
    COALESCE(w.name, 'Sin sitio web') AS website_id,
    COALESCE(s.name, 'Sin estado de auth') AS auth_status_id,
    u.username,
    u.email,
    u.email_lowercase,
    COALESCE(u.name_prefix, 'Sin prefijo') AS name_prefix,
    COALESCE(u.first_name, 'Sin nombre') AS first_name,
    COALESCE(u.middle_name, 'Sin segundo nombre') AS middle_name,
    COALESCE(u.last_name, 'Sin apellido') AS last_name,
    COALESCE(u.name_suffix, 'Sin sufijo') AS name_suffix,
    u.birthday,
    u.enabled,
    u.confirmed,
    u.is_guest,
    u.salt,
    u.password,
    COALESCE(u.confirmation_token, 'Sin token') AS confirmation_token,
    u.password_requested,
    u.password_changed,
    u.last_login,
    u.login_count,
    u.created_at,
    u.updated_at,
    u.cookies_accepted,
    COALESCE(u.serialized_data::text, '{}') AS serialized_data,
    u.ac_last_contact_date,
    u.ac_last_contact_date_in,
    u.ac_last_contact_date_out,
    COALESCE(u.ac_contact_count, 0) AS ac_contact_count,
    COALESCE(u.ac_contact_count_in, 0) AS ac_contact_count_in,
    COALESCE(u.ac_contact_count_out, 0) AS ac_contact_count_out
FROM public.oro_customer_user u
LEFT JOIN public.oro_user ou
    ON u.owner_id = ou.id
LEFT JOIN public.oro_organization org
    ON u.organization_id = org.id
LEFT JOIN public.oro_customer c
    ON u.customer_id = c.id
LEFT JOIN public.oro_website w
    ON u.website_id = w.id
LEFT JOIN public.oro_enum_cu_auth_status s
    ON u.auth_status_id = s.id
ORDER BY u.id;


SELECT * FROM oro_customer_user_granular LIMIT 15;
SELECT * FROM oro_customer_user LIMIT 15;