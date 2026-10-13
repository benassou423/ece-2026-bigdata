{{ config(severity='warn') }}

-- The 3 first digits of a ZIP code identify a sectional center of the USPS network, located in one state:
-- the test returns the users whose ZIP code is outside the prefix ranges of their state
select
    u.user_id,
    u.state,
    u.zip_code
from {{ ref('stg_users') }} u
where not exists (
    select 1
    from {{ ref('zip_prefixes') }} z
    where z.state = u.state
        and try_cast(left(u.zip_code, 3) as integer) between z.prefix_min and z.prefix_max
)
