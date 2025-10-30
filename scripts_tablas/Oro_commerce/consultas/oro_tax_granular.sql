CREATE OR REPLACE VIEW oro_tax_granular AS
SELECT
    /* ===== PK ===== */
    t.id,

    /* ===== Código ===== */
    t.code,
    COALESCE(NULLIF(t.code, ''), 'Sin código')::varchar(255) AS code_txt,

    /* ===== Descripción ===== */
    t.description,
    COALESCE(NULLIF(t.description, ''), 'Sin descripción')::text AS description_txt,

    /* ===== Tasa ===== */
    t.rate,  -- ej. 0.07 significa 7%
    ROUND( (t.rate * 100)::numeric, 4 ) AS rate_pct,                          -- 7.0000
    (ROUND( (t.rate * 100)::numeric, 2 )::text || '%')::varchar(32) AS rate_pct_txt, -- '7%','7.50%', etc.

    /* ===== Tiempos ===== */
    t.created_at,
    t.updated_at

FROM public.oro_tax t;

SELECT * FROM oro_tax_granular;