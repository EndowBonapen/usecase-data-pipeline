select
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price_amount,
    line_total_amount
from {{ ref('stg_order_items') }}
