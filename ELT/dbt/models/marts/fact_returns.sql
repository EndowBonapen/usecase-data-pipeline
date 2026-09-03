-- Grain: 1 row per return line item. customer_id/product_id come from
-- simulated crosswalks, not real source references (see models/intermediate/).
select
    r.return_id,
    ri.order_line_id,
    cw_c.customer_id,
    cw_p.product_id,
    date(r.created_at) as created_date_key,
    r.created_at,
    r.status,
    r.reason,
    ri.return_quantity,
    ri.line_refund_amount as refund_amount
from {{ ref('stg_returns') }} as r
join {{ ref('stg_return_items') }} as ri
    on ri.return_id = r.return_id
left join {{ ref('int_return_customer_crosswalk') }} as cw_c
    on cw_c.return_id = r.return_id
left join {{ ref('int_return_item_product_crosswalk') }} as cw_p
    on cw_p.return_id = ri.return_id
    and cw_p.order_line_id = ri.order_line_id
