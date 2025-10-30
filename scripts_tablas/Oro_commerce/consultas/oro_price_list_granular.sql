CREATE OR REPLACE VIEW oro_price_list_granular AS
SELECT
    /* ===== Clave primaria ===== */
    pl.id,

    /* ===== FK + legible ===== */
    pl.organization_id,
    COALESCE(NULLIF(org.name, ''), 'Sin organización')::varchar(255) AS organization_name,

    /* ===== Atributos propios ===== */
    COALESCE(NULLIF(pl.name, ''), 'Sin nombre')::varchar(255)                AS name,

    /* Booleans (con versión legible) */
    COALESCE(pl.active, false)                                               AS active,
    CASE WHEN COALESCE(pl.active, false)  THEN 'Activo'     ELSE 'Inactivo'   END::varchar(20) AS active_txt,

    COALESCE(pl.actual, false)                                               AS actual,
    CASE WHEN COALESCE(pl.actual, false)  THEN 'Vigente'    ELSE 'No vigente' END::varchar(20) AS actual_txt,

    /* Texto largo con fallback */
    COALESCE(NULLIF(pl.product_assignment_rule, ''), 'Sin regla de asignación')::text AS product_assignment_rule,

    /* Programación (bool + legible) */
    COALESCE(pl.contain_schedule, false)                                     AS contain_schedule,
    CASE WHEN COALESCE(pl.contain_schedule, false) THEN 'Con horario' ELSE 'Sin horario' END::varchar(20) AS contain_schedule_txt,

    /* Tiempos */
    pl.created_at,
    pl.updated_at,

    /* JSONB original y legible (extra, opcional) */
    pl.serialized_data,
    COALESCE(pl.serialized_data::text, 'Sin datos')::text                    AS serialized_data_txt

FROM public.oro_price_list pl
LEFT JOIN public.oro_organization org
       ON org.id = pl.organization_id;


SELECT * FROM oro_price_list_granular;