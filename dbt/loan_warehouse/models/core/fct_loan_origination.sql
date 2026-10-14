-- Grain: one row per loan, attributes as of origination.
select
    l.loan_id,
    l.borrower_id,
    l.dealer_id,
    l.vin,
    l.origination_date,
    {{ dbt.date_trunc('month', 'l.origination_date') }} as origination_month,
    cast(extract(year from l.origination_date) as varchar) || '-Q'
        || cast(extract(quarter from l.origination_date) as varchar) as vintage_quarter,
    l.amount_financed,
    l.apr,
    l.term_months,
    case when l.term_months <= 48 then '<= 48' when l.term_months <= 60 then '60' else '72+' end as term_band,
    l.monthly_payment,
    v.vehicle_value,
    v.vehicle_condition,
    l.amount_financed / v.vehicle_value as ltv,
    {{ ltv_band('l.amount_financed / v.vehicle_value') }} as ltv_band,
    b.credit_score,
    b.credit_tier,
    case when b.annual_income > 0 then (l.monthly_payment * 12) / b.annual_income end as payment_to_income
from {{ ref('stg_loans') }} l
join {{ ref('stg_vehicles') }} v using (vin)
join {{ ref('dim_borrower') }} b using (borrower_id)
