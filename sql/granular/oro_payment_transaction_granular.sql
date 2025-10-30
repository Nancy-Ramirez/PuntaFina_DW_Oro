DROP VIEW IF EXISTS oro_payment_transaction_granular;
CREATE OR REPLACE VIEW oro_payment_transaction_granular AS
SELECT
  /* ===== Identificador base ===== */
  t.id,

  /* ===== Origen (FK a self) ===== */
  t.source_payment_transaction                          AS source_payment_transaction_id,
  COALESCE('Txn ' || src.id::text, 'Sin origen')::varchar(255) AS source_payment_transaction_txt,

  /* ===== Dueño frontend (FK a oro_customer_user) ===== */
  t.frontend_owner_id,
  COALESCE(
    NULLIF(cu_tx.email, ''),
    NULLIF(cu_ord.email, ''),
    'Sin dueño frontend'
  )::varchar(255) AS frontend_owner,

  /* ===== Organización (FK a oro_organization) ===== */
  t.organization_id,
  COALESCE(
    NULLIF(org_tx.name, ''),
    NULLIF(org_ord.name, ''),
    'Sin organización'
  )::varchar(255) AS organization_name,

  /* ===== Propietario interno (FK a oro_user) ===== */
  t.user_owner_id,
  COALESCE(NULLIF(u_tx.username, ''), 'Sin propietario')::varchar(255) AS user_owner,

  /* ===== Entidad referenciada ===== */
  COALESCE(NULLIF(t.entity_class, ''), 'Sin clase de entidad')::varchar(255) AS entity_class,
  t.entity_identifier,
  CASE
    WHEN t.entity_class = 'Oro\Bundle\OrderBundle\Entity\Order' THEN
      COALESCE(NULLIF(ord.identifier, ''), 'Pedido ' || COALESCE(t.entity_identifier::text, ''))
    ELSE
      COALESCE(t.entity_identifier::text, 'Sin entidad')
  END::varchar(255) AS entity_identifier_txt,

  /* ===== Método / acción / referencia ===== */
  COALESCE(NULLIF(t.payment_method, ''), 'Sin método de pago')::varchar(255) AS payment_method,
  COALESCE(NULLIF(t.action,         ''), 'Sin acción')::varchar(255)         AS action,
  COALESCE(NULLIF(t.reference,      ''), 'Sin referencia')::varchar(255)     AS reference,

  /* ===== Campos nativos ===== */
  t.amount,
  COALESCE(NULLIF(t.currency, ''), 'Sin moneda')::varchar(10) AS currency,
  t.active,
  t.successful,

  /* ===== Payloads ===== */
  COALESCE(NULLIF(t.request,             ''), 'Sin request')::text             AS request,
  COALESCE(NULLIF(t.response,            ''), 'Sin response')::text            AS response,
  COALESCE(NULLIF(t.transaction_options, ''), 'Sin opciones')::text            AS transaction_options,

  /* ===== Tiempos ===== */
  t.created_at,
  t.updated_at

FROM oro_payment_transaction t
/* Origen (self-join) */
LEFT JOIN oro_payment_transaction src
       ON src.id = t.source_payment_transaction
LEFT JOIN oro_order ord
       ON ord.id = t.entity_identifier
      AND t.entity_class = 'Oro\\Bundle\\OrderBundle\\Entity\\Order'

/* Dueño frontend: por transacción y por orden */
LEFT JOIN oro_customer_user cu_tx  ON cu_tx.id  = t.frontend_owner_id
LEFT JOIN oro_customer_user cu_ord ON cu_ord.id = ord.customer_user_id

/* Organización: por transacción y por orden */
LEFT JOIN oro_organization org_tx  ON org_tx.id  = t.organization_id
LEFT JOIN oro_organization org_ord ON org_ord.id = ord.organization_id

/* Propietario (usuario interno) guardado en la transacción */
LEFT JOIN oro_user u_tx ON u_tx.id = t.user_owner_id;

SELECT * FROM oro_payment_transaction_granular;