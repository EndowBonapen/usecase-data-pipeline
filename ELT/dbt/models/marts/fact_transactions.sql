-- customer_id comes from a simulated crosswalk, not a real source reference
-- (see models/intermediate/int_transaction_customer_crosswalk.sql)
select
    t.transaction_id,
    cw.customer_id,
    date(t.occurred_at) as occurred_date_key,
    t.occurred_at,
    t.amount,
    t.currency,
    t.transaction_type,
    t.status,
    t.merchant,
    t.reference
from {{ ref('stg_transactions') }} as t
left join {{ ref('int_transaction_customer_crosswalk') }} as cw
    on cw.transaction_id = t.transaction_id
