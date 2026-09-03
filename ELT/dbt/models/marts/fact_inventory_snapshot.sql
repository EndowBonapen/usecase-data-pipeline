-- Grain: 1 SKU x warehouse x asOf. product_id comes from a simulated
-- crosswalk, not a real source reference (see models/intermediate/).
select
    i.inventory_id,
    cw.product_id,
    i.warehouse_id,
    date(i.as_of) as snapshot_date_key,
    i.as_of,
    i.sku,
    i.stock_status,
    i.on_hand,
    i.reserved,
    i.available,
    i.backordered,
    i.reorder_point,
    i.needs_reorder
from {{ ref('stg_inventory') }} as i
left join {{ ref('int_inventory_product_crosswalk') }} as cw
    on cw.inventory_id = i.inventory_id
