CREATE OR REPLACE VIEW orocrm_channel_granular AS
SELECT
    /* ===== PK ===== */
    ch.id,

    /* ===== FK + legible: Organización ===== */
    ch.organization_owner_id,
    COALESCE(NULLIF(org.name, ''), 'Sin organización')::varchar(255) AS organization_name,

    /* ===== FK + legible: Fuente de datos (oro_integration_channel) ===== */
    ch.data_source_id,
    COALESCE(NULLIF(ic.name, ''), 'Sin fuente de datos')::varchar(255)  AS data_source_name,
    COALESCE(NULLIF(ic.type, ''), 'Sin tipo de integración')::varchar(255) AS data_source_type,
    COALESCE(ic.enabled, false) AS data_source_enabled,
    CASE WHEN COALESCE(ic.enabled, false)
         THEN 'Integración habilitada' ELSE 'Integración deshabilitada' END::varchar(32) AS data_source_enabled_txt,

    /* ===== Atributos propios ===== */
    COALESCE(NULLIF(ch.name, ''), 'Sin nombre')::varchar(255) AS name,

    COALESCE(ch.status, false) AS status,
    CASE WHEN COALESCE(ch.status, false) THEN 'Activo' ELSE 'Inactivo' END::varchar(20) AS status_txt,

    COALESCE(NULLIF(ch.channel_type, ''), 'Sin tipo de canal')::varchar(255) AS channel_type,

    /* JSON original + legible */
    ch.data,
    COALESCE(ch.data::text, 'Sin datos')::text AS data_txt,

    COALESCE(NULLIF(ch.customer_identity, ''), 'Sin identidad de cliente')::varchar(255) AS customer_identity,

    /* Tiempos (respetamos los tipos originales) */
    ch.createdat,
    ch.updatedat

FROM public.orocrm_channel ch
LEFT JOIN public.oro_organization       org ON org.id = ch.organization_owner_id
LEFT JOIN public.oro_integration_channel ic  ON ic.id  = ch.data_source_id;

SELECT * FROM orocrm_channel_granular;