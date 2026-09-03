-- dbt singular test: passes when this returns 0 rows.
-- Same check as ETL's transform/clean.py validate_order_totals(), now in SQL.
select
    o.order_id,
    o.total_amount as order_total,
    sum(oi.line_total_amount) as items_total
from {{ ref('fact_orders') }} as o
join {{ ref('fact_order_items') }} as oi
    on oi.order_id = o.order_id
group by o.order_id, o.total_amount
having abs(o.total_amount - sum(oi.line_total_amount)) > 0.01
