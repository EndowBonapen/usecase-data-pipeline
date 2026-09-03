-- SIMULATED crosswalk — see int_transaction_customer_crosswalk.sql for why.
-- inventory.product{} is independently generated, not linked to
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
    i.inventory_id,
    p.product_id
from {{ ref('stg_inventory') }} as i
cross join product_count
join products as p
    on p.idx = mod(abs(farm_fingerprint(i.inventory_id)), product_count.n)
