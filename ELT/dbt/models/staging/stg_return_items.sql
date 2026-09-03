select
    id as return_id,
    item.orderLineId as order_line_id,
    item.productId as product_id,
    item.name as product_name,
    item.orderedQuantity as ordered_quantity,
    item.returnQuantity as return_quantity,
    cast(item.unitPriceMinor as numeric) / 100 as unit_price_amount,
    cast(item.lineRefundMinor as numeric) / 100 as line_refund_amount
from {{ source('raw', 'returns') }},
unnest(items) as item
