select
    id as inventory_id,
    incoming_item.purchaseOrderId as purchase_order_id,
    incoming_item.quantity as quantity,
    incoming_item.expectedAt as expected_at
from {{ source('raw', 'inventory') }},
unnest(incoming) as incoming_item
