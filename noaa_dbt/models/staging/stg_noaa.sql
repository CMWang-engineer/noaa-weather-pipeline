with source as (
    select * from {{ source('noaa_weather', 'noaa_partitioned') }}
),

cleaned as (
    select
        STATION,
        DATE,
        LATITUDE,
        LONGITUDE,
        ELEVATION,
        NAME,
        TRIM(RIGHT(COUNTRY, 2)) as COUNTRY,
        MONTH,
        EXTRACT(YEAR FROM DATE) as YEAR,
        TEMP_C,
        MAX_C,
        MIN_C,
        case when PRCP = 99.99 then null else PRCP end as PRCP,
        case when WDSP = 999.9 then null else WDSP end as WDSP

    from source
    where TEMP_C is not null
)

select * from cleaned
