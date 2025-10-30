DROP VIEW IF EXISTS oro_user_granular;

CREATE VIEW oro_user_granular AS 
SELECT
    /* ===== PK ===== */
    u.id,

    /* ===== FK #1: business_unit_owner_id ===== */
    u.business_unit_owner_id,
    COALESCE(bu.name, 'Sin unidad de negocio')::varchar(255) AS business_unit_owner_name,

    /* ===== FK #2: avatar_id ===== */
    u.avatar_id,
    COALESCE(af.original_filename, 'Sin avatar')::varchar(255)   AS avatar_filename,

    /* ===== FK #3: organization_id ===== */
    u.organization_id,
    COALESCE(org.name, 'Sin organización')::varchar(255)         AS organization_name,

    /* ===== FK #4: auth_status_id (enum por texto) ===== */
    u.auth_status_id,
    COALESCE(eas.name, 'Sin estado')::varchar(255)               AS auth_status_name,

    /* ===== resto de columnas originales (sin cambios) ===== */
    u.username,
    u.username_lowercase,
    u.email,
    u.email_lowercase,
    u.phone,
    u.name_prefix,
    u.first_name,
    u.middle_name,
    u.last_name,
    u.name_suffix,
    u.birthday,
    u.enabled,
    u.salt,
    u.password,
    u.confirmation_token,
    u.password_requested,
    u.last_login,
    u.login_count,
    u.createdat,
    u.updatedat,
    u.title,
    u.password_changed,
    u.googleid,
    u.serialized_data

FROM public.oro_user u
LEFT JOIN public.oro_business_unit      bu  ON bu.id  = u.business_unit_owner_id
LEFT JOIN public.oro_attachment_file    af  ON af.id  = u.avatar_id
LEFT JOIN public.oro_organization       org ON org.id = u.organization_id
LEFT JOIN public.oro_enum_auth_status   eas ON eas.id = u.auth_status_id;

SELECT * FROM oro_user_granular ORDER BY id;