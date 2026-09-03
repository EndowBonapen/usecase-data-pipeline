-- SIMULATED crosswalk. Empirically confirmed (see docs) that transactions
-- carry no customer reference at all in the source data, and their IDs share
-- no pool with relational_seed's customers. Real MDM would match on a real
-- signal (email, name, timestamp proximity); there is none here to match on.
-- Each transaction is deterministically (stable hash, not random) assigned
-- to a customer so downstream models have something to join on — this
-- demonstrates the crosswalk *pattern*, not a solved identity match.
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
    t.transaction_id,
    c.customer_id
from {{ ref('stg_transactions') }} as t
cross join customer_count
join customers as c
    on c.idx = mod(abs(farm_fingerprint(t.transaction_id)), customer_count.n)
