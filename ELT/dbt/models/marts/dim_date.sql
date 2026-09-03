-- Static spine wide enough to cover the mock data's timestamps (including
-- forward-looking ones like inventory.nextRestockAt). No dbt_utils package
-- installed, so this is plain generate_date_array rather than date_spine().
select
    date_day as date_key,
    extract(year from date_day) as year,
    extract(month from date_day) as month,
    extract(day from date_day) as day,
    extract(dayofweek from date_day) in (1, 7) as is_weekend
from unnest(generate_date_array('2025-01-01', '2027-12-31')) as date_day
