CREATE OR REPLACE VIEW oro_catalog_category_granular AS
SELECT
    /* ===== PK ===== */
    c.id,

    /* ===== Padre (self FK) ===== */
    c.parent_id,
    COALESCE(NULLIF(p.title, ''), 'Sin padre')::varchar(255) AS parent_title,

    /* ===== Opciones por defecto del producto ===== */
    c.default_product_options_id,
    CASE
        WHEN c.default_product_options_id IS NOT NULL
            THEN ('Opciones por defecto #' || c.default_product_options_id::text)
        ELSE 'Sin opciones por defecto'
    END::varchar(255) AS default_product_options_txt,

    /* ===== Organización ===== */
    c.organization_id,
    COALESCE(NULLIF(org.name, ''), 'Sin organización')::varchar(255) AS organization_name,

    /* ===== Árbol ===== */
    c.tree_left,
    c.tree_level,
    c.tree_right,
    c.tree_root,

    /* ===== Fechas ===== */
    c.created_at,
    c.updated_at,

    /* ===== Campos de nombre/ruta ===== */
    COALESCE(NULLIF(c.materialized_path, ''), 'Sin ruta')::varchar(255) AS materialized_path,
    COALESCE(NULLIF(c.title,            ''), 'Sin título')::varchar(255) AS title,

    /* ===== Imágenes (FK a attachment) ===== */
    c.largeimage_id,
    CASE WHEN c.largeimage_id IS NULL
         THEN 'Sin imagen grande' ELSE ('Archivo #' || c.largeimage_id::text) END::varchar(255) AS largeimage_txt,

    c.smallimage_id,
    CASE WHEN c.smallimage_id IS NULL
         THEN 'Sin imagen pequeña' ELSE ('Archivo #' || c.smallimage_id::text) END::varchar(255) AS smallimage_txt,

    /* ===== Fallbacks de inventario (FK a oro_entity_fallback_value) ===== */
    c.manageinventory_id,
    CASE WHEN c.manageinventory_id IS NULL
         THEN 'Sin gestión inventario' ELSE ('EFV #' || c.manageinventory_id::text) END::varchar(255) AS manageinventory_txt,

    c.highlightlowinventory_id,
    CASE WHEN c.highlightlowinventory_id IS NULL
         THEN 'Sin resaltado bajo inventario' ELSE ('EFV #' || c.highlightlowinventory_id::text) END::varchar(255) AS highlightlowinventory_txt,

    c.inventorythreshold_id,
    CASE WHEN c.inventorythreshold_id IS NULL
         THEN 'Sin umbral inventario' ELSE ('EFV #' || c.inventorythreshold_id::text) END::varchar(255) AS inventorythreshold_txt,

    c.lowinventorythreshold_id,
    CASE WHEN c.lowinventorythreshold_id IS NULL
         THEN 'Sin umbral bajo inventario' ELSE ('EFV #' || c.lowinventorythreshold_id::text) END::varchar(255) AS lowinventorythreshold_txt,

    c.minimumquantitytoorder_id,
    CASE WHEN c.minimumquantitytoorder_id IS NULL
         THEN 'Sin mínimo por pedido' ELSE ('EFV #' || c.minimumquantitytoorder_id::text) END::varchar(255) AS minimumquantitytoorder_txt,

    c.maximumquantitytoorder_id,
    CASE WHEN c.maximumquantitytoorder_id IS NULL
         THEN 'Sin máximo por pedido' ELSE ('EFV #' || c.maximumquantitytoorder_id::text) END::varchar(255) AS maximumquantitytoorder_txt,

    c.decrementquantity_id,
    CASE WHEN c.decrementquantity_id IS NULL
         THEN 'Sin decremento cantidad' ELSE ('EFV #' || c.decrementquantity_id::text) END::varchar(255) AS decrementquantity_txt,

    c.backorder_id,
    CASE WHEN c.backorder_id IS NULL
         THEN 'Sin backorder' ELSE ('EFV #' || c.backorder_id::text) END::varchar(255) AS backorder_txt,

    c.isupcoming_id,
    CASE WHEN c.isupcoming_id IS NULL
         THEN 'Sin próximo' ELSE ('EFV #' || c.isupcoming_id::text) END::varchar(255) AS isupcoming_txt,

    /* ===== Disponibilidad ===== */
    c.availability_date,

    /* ===== JSONB ===== */
    c.serialized_data,
    COALESCE(c.serialized_data::text, 'Sin datos')::text AS serialized_data_txt

FROM public.oro_catalog_category c
LEFT JOIN public.oro_catalog_category p ON p.id = c.parent_id
LEFT JOIN public.oro_organization org   ON org.id = c.organization_id;


SELECT * FROM oro_catalog_category_granular;