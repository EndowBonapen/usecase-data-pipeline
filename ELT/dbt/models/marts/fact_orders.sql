select
    order_id,
    customer_id,
    order_number,
    status,
    currency,
    date(placed_at) as placed_date_key,
    placed_at,
    shipped_at,
    total_amount
from {{ ref('stg_orders') }}
