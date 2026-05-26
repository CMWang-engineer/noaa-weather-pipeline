with base as (
    select * from {{ ref('stg_noaa') }}
)

select
    COUNTRY,
    YEAR,
    MONTH,
    round(avg(TEMP_C), 2)   as avg_temp_c,
    round(avg(MAX_C), 2)    as avg_max_c,
    round(avg(MIN_C), 2)    as avg_min_c,
    round(max(MAX_C), 2)    as highest_temp_c,
    round(min(MIN_C), 2)    as lowest_temp_c,
    count(*)                as record_count
from base
group by COUNTRY, YEAR, MONTH
order by COUNTRY, YEAR, MONTH
