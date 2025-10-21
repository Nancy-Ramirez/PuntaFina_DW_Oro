
CREATE OR REPLACE VIEW vw_orocrm_channel AS
SELECT
    ch.id,
    COALESCE(o.name, 'Sin organización') AS organization_owner_id,
    COALESCE(ic.name, 'Sin fuente de datos') AS data_source_id,
    ch.name,
    CASE 
        WHEN ch.status = TRUE THEN 'Activo'
        ELSE 'Inactivo'
    END AS status,
    ch.channel_type,
    COALESCE(ch.customer_identity, 'Sin identidad de cliente') AS customer_identity,
    ch.createdat,
    ch.updatedat,
    ch.data
FROM orocrm_channel ch
    LEFT JOIN oro_organization o ON ch.organization_owner_id = o.id
    LEFT JOIN oro_integration_channel ic ON ch.data_source_id = ic.id;


SELECT * FROM vw_orocrm_channel LIMIT 10;
SELECT * FROM orocrm_channel LIMIT 10;
