-- One row per calendar month seen in originations or performance (portable across warehouses).
with months as (
    select distinct origination_month as month_start from {{ ref('fct_loan_origination') }}
    union
    select distinct as_of_month from {{ ref('stg_loan_performance') }}
)
select
    month_start,
    extract(year from month_start) as year,
    extract(quarter from month_start) as quarter,
    extract(month from month_start) as month,
    cast(extract(year from month_start) as varchar) || '-Q' || cast(extract(quarter from month_start) as varchar) as year_quarter
from months
