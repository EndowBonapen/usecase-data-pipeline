select
    id as inventory_id,
    sku,
    product.id as product_id,
    product.name as product_name,
    product.category as product_category,
    warehouse.id as warehouse_id,
    warehouse.name as warehouse_name,
    stockStatus as stock_status,
    onHand as on_hand,
    reserved,
    available,
    backordered,
    reorderPoint as reorder_point,
    needsReorder as needs_reorder,
    incomingQuantity as incoming_quantity,
    nextRestockAt as next_restock_at,
    asOf as as_of
from {{ source('raw', 'inventory') }}
