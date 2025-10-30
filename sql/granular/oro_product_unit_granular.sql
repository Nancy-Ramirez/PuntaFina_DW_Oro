CREATE OR REPLACE VIEW oro_product_unit_granular AS
SELECT
    /* ===== Clave primaria ===== */
    u.code,
    COALESCE(NULLIF(u.code, ''), 'Sin código')::varchar(255) AS code_txt,
    UPPER(COALESCE(NULLIF(u.code, ''), 'SIN CÓDIGO'))::varchar(255) AS code_upper,

    /* ===== Precisión por defecto ===== */
    u.default_precision,
    ('Precisión ' || COALESCE(u.default_precision, 0)::text)::varchar(32) AS default_precision_txt

FROM public.oro_product_unit u;

SELECT * FROM oro_product_unit_granular;