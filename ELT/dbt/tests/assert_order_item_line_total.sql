-- Same check as ETL's validate_order_item_totals(), now in SQL.
select
    order_item_id,
    quantity,
    unit_price_amount,
    line_total_amount
from {{ ref('fact_order_items') }}
where abs(line_total_amount - (quantity * unit_price_amount)) > 0.01
