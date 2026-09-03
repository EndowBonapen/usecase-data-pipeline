-- SIMULATED crosswalk — see int_transaction_customer_crosswalk.sql for why.
-- returns.order_id was checked against relational_seed's real orders
-- (0 matches, different ID formats entirely) — confirmed independent.
with customers as (
    select
        customer_id,
        row_number() over (order by customer_id) - 1 as idx
    from {{ ref('stg_customers') }}
),
customer_count as (
    select count(*) as n from {{ ref('stg_customers') }}
)

select
    r.return_id,
    c.customer_id
from {{ ref('stg_returns') }} as r
cross join customer_count
join customers as c
    on c.idx = mod(abs(farm_fingerprint(r.return_id)), customer_count.n)
