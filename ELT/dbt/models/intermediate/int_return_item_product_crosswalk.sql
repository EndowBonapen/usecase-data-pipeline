-- SIMULATED crosswalk — see int_transaction_customer_crosswalk.sql for why.
-- return_items.product_id is satellite-generated, independent of
-- relational_seed's real product catalog.
with products as (
    select
        product_id,
        row_number() over (order by product_id) - 1 as idx
    from {{ ref('stg_products') }}
),
product_count as (
    select count(*) as n from {{ ref('stg_products') }}
)

select
    ri.return_id,
    ri.order_line_id,
    p.product_id
from {{ ref('stg_return_items') }} as ri
cross join product_count
join products as p
    on p.idx = mod(abs(farm_fingerprint(concat(ri.return_id, ri.order_line_id))), product_count.n)
