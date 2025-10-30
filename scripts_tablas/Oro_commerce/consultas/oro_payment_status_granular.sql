CREATE OR REPLACE VIEW oro_payment_status_granular AS
SELECT
    /* ===== PK ===== */
    ps.id,

    /* ===== Clase de entidad + legibles ===== */
    COALESCE(NULLIF(ps.entity_class, ''), 'Sin clase de entidad')::varchar(255) AS entity_class,
    ps.entity_identifier,
    CASE
        WHEN ps.entity_class = 'Oro\\Bundle\\OrderBundle\\Entity\\Order' THEN
            COALESCE(NULLIF(o.identifier, ''), 'Pedido ' || ps.entity_identifier::text)
        ELSE
            COALESCE(ps.entity_identifier::text, 'Sin entidad')
    END::varchar(255) AS entity_identifier_txt,

    /* ===== Estado de pago ===== */
    ps.payment_status,
    COALESCE(NULLIF(ps.payment_status, ''), 'Sin estado de pago')::varchar(255) AS payment_status_txt

FROM public.oro_payment_status ps
/* Si la entidad es una Orden, intentamos enriquecer el identificador */
LEFT JOIN public.oro_order o
       ON o.id = ps.entity_identifier
      AND ps.entity_class = 'Oro\\Bundle\\OrderBundle\\Entity\\Order';

SELECT * FROM oro_payment_status_granular;