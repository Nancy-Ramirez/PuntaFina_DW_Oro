CREATE OR REPLACE VIEW oro_address_granular AS
SELECT
    /* ===== PK ===== */
    a.id,

    /* ===== País (FK) ===== */
    a.country_code,
    COALESCE(NULLIF(c.name, ''), 'Sin país')::varchar(255) AS country_name,

    /* ===== Región/Estado (FK) ===== */
    a.region_code,
    COALESCE(NULLIF(r.name, ''), 'Sin región')::varchar(255) AS region_name,

    /* Etiqueta de región preferida: nombre por FK; si no, el texto libre de la dirección */
    COALESCE(
      NULLIF(r.name, ''),
      NULLIF(a.region_text, ''),
      'Sin región'
    )::varchar(255) AS region_label,

    /* ===== Etiquetas y organización ===== */
    COALESCE(NULLIF(a.label, ''), 'Sin etiqueta')::varchar(255)          AS label,
    COALESCE(NULLIF(a.organization, ''), 'Sin organización')::varchar(255) AS organization,

    /* ===== Vía pública ===== */
    COALESCE(NULLIF(a.street,  ''), 'Sin calle')::varchar(500)  AS street,
    COALESCE(NULLIF(a.street2, ''), 'Sin complemento')::varchar(500) AS street2,

    /* ===== Ciudad y CP ===== */
    COALESCE(NULLIF(a.city,        ''), 'Sin ciudad')::varchar(255)       AS city,
    COALESCE(NULLIF(a.postal_code, ''), 'Sin código postal')::varchar(255) AS postal_code,

    /* ===== Nombre de persona (componentes originales) ===== */
    a.name_prefix,
    a.first_name,
    a.middle_name,
    a.last_name,
    a.name_suffix,

    /* ===== Nombre completo (legible) ===== */
    COALESCE(
      NULLIF(
        CONCAT_WS(
          ' ',
          NULLIF(a.name_prefix, ''),
          NULLIF(a.first_name, ''),
          NULLIF(a.middle_name, ''),
          NULLIF(a.last_name, ''),
          NULLIF(a.name_suffix, '')
        ),
        ''
      ),
      'Sin nombre'
    )::varchar(255) AS full_name_txt,

    /* ===== Dirección completa (legible) ===== */
    COALESCE(
      NULLIF(
        CONCAT_WS(
          ', ',
          NULLIF(a.street, ''),
          NULLIF(a.street2, ''),
          NULLIF(a.city, ''),
          -- Preferimos nombre de región por FK; si no, el texto libre:
          NULLIF(COALESCE(NULLIF(r.name, ''), NULLIF(a.region_text, '')), ''),
          NULLIF(a.postal_code, ''),
          NULLIF(a.country_code, '')
        ),
        ''
      ),
      'Sin dirección'
    )::varchar(1024) AS full_address_txt,

    /* ===== Tiempos ===== */
    a.created,
    a.updated,

    /* ===== JSONB ===== */
    a.serialized_data,
    COALESCE(a.serialized_data::text, 'Sin datos')::text AS serialized_data_txt

FROM public.oro_address a
LEFT JOIN public.oro_dictionary_country c
       ON c.iso2_code = a.country_code
LEFT JOIN public.oro_dictionary_region  r
       ON r.combined_code = a.region_code;


SELECT * FROM oro_address_granular;