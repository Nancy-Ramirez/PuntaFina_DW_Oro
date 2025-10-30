DROP VIEW IF EXISTS oro_product_granular;

CREATE VIEW vw_oro_product_granular AS
SELECT
    /* === columnas originales, en el mismo orden === */
    p.id,

    p.organization_id,
    COALESCE(org.name, 'Sin organización')::varchar(255)                          AS organization_name,

    p.business_unit_owner_id,
    COALESCE(bu.name, 'Sin unidad de negocio')::varchar(255)                      AS business_unit_owner_name,

    p.primary_unit_precision_id,
    COALESCE(
      CASE WHEN pup.id IS NOT NULL THEN
           'Unidad ' || COALESCE(pup.unit_code, '?')
           || ' (prec ' || COALESCE(pup.unit_precision::text, '?') || ')'
           || ' · rate ' || COALESCE(pup.conversion_rate::text, '?')
           || ' · ' || CASE WHEN pup.sell THEN 'vendible' ELSE 'no vendible' END
      END,
      'Sin precisión principal'
    )::varchar(255)                                                                AS primary_unit_precision_label,

    p.brand_id,
    COALESCE(br.default_title, 'Sin marca')::varchar(255)                          AS brand_name,

    p.inventory_status_id,
    COALESCE(inv.name, 'Sin estado de inventario')::varchar(255)                   AS inventory_status_name,

    p.attribute_family_id,
    COALESCE(af.code, 'Sin familia de atributos')::varchar(255)                    AS attribute_family_code,

    p.sku,
    p.sku_uppercase,
    p.name,
    p.name_uppercase,
    p.created_at,
    p.updated_at,
    p.variant_fields,
    p.status,
    p.type,
    p.is_featured,
    p.is_new_arrival,

    p.pagetemplate_id,
    COALESCE(
      CASE WHEN p.pagetemplate_id IS NOT NULL THEN 'EFV #' || p.pagetemplate_id::text END,
      'Sin plantilla'
    )::varchar(255)                                                                AS pagetemplate_label,

    p.category_id,
    COALESCE(cat.title, 'Sin categoría')::varchar(255)                             AS category_title,

    p.category_sort_order,

    p.taxcode_id,
    COALESCE(tax.code, 'Sin código fiscal')::varchar(255)                          AS taxcode_code,

    p.manageinventory_id,
    COALESCE(
      CASE WHEN p.manageinventory_id IS NOT NULL THEN 'EFV #' || p.manageinventory_id::text END,
      'Sin manage inventory'
    )::varchar(255)                                                                AS manageinventory_label,

    p.highlightlowinventory_id,
    COALESCE(
      CASE WHEN p.highlightlowinventory_id IS NOT NULL THEN 'EFV #' || p.highlightlowinventory_id::text END,
      'Sin highlight low inventory'
    )::varchar(255)                                                                AS highlight_low_inventory_label,

    p.inventorythreshold_id,
    COALESCE(
      CASE WHEN p.inventorythreshold_id IS NOT NULL THEN 'EFV #' || p.inventorythreshold_id::text END,
      'Sin umbral de inventario'
    )::varchar(255)                                                                AS inventory_threshold_label,

    p.lowinventorythreshold_id,
    COALESCE(
      CASE WHEN p.lowinventorythreshold_id IS NOT NULL THEN 'EFV #' || p.lowinventorythreshold_id::text END,
      'Sin umbral bajo'
    )::varchar(255)                                                                AS low_inventory_threshold_label,

    p.minimumquantitytoorder_id,
    COALESCE(
      CASE WHEN p.minimumquantitytoorder_id IS NOT NULL THEN 'EFV #' || p.minimumquantitytoorder_id::text END,
      'Sin cantidad mínima'
    )::varchar(255)                                                                AS min_qty_to_order_label,

    p.maximumquantitytoorder_id,
    COALESCE(
      CASE WHEN p.maximumquantitytoorder_id IS NOT NULL THEN 'EFV #' || p.maximumquantitytoorder_id::text END,
      'Sin cantidad máxima'
    )::varchar(255)                                                                AS max_qty_to_order_label,

    p.decrementquantity_id,
    COALESCE(
      CASE WHEN p.decrementquantity_id IS NOT NULL THEN 'EFV #' || p.decrementquantity_id::text END,
      'Sin decremento'
    )::varchar(255)                                                                AS decrement_qty_label,

    p.backorder_id,
    COALESCE(
      CASE WHEN p.backorder_id IS NOT NULL THEN 'EFV #' || p.backorder_id::text END,
      'Sin backorder'
    )::varchar(255)                                                                AS backorder_label,

    p.isupcoming_id,
    COALESCE(
      CASE WHEN p.isupcoming_id IS NOT NULL THEN 'EFV #' || p.isupcoming_id::text END,
      'Sin upcoming'
    )::varchar(255)                                                                AS is_upcoming_label,

    p.availability_date,
    COALESCE(p.serialized_data, '{}'::jsonb)                                       AS serialized_data

FROM public.oro_product p
LEFT JOIN public.oro_organization               org ON org.id  = p.organization_id
LEFT JOIN public.oro_business_unit              bu  ON bu.id   = p.business_unit_owner_id
LEFT JOIN public.oro_product_unit_precision     pup ON pup.id  = p.primary_unit_precision_id
LEFT JOIN public.oro_brand                      br  ON br.id   = p.brand_id
LEFT JOIN public.oro_enum_prod_inventory_status inv ON inv.id  = p.inventory_status_id
LEFT JOIN public.oro_attribute_family           af  ON af.id   = p.attribute_family_id
LEFT JOIN public.oro_catalog_category           cat ON cat.id  = p.category_id
LEFT JOIN public.oro_tax_product_tax_code       tax ON tax.id  = p.taxcode_id;

SELECT * FROM oro_product_granular;