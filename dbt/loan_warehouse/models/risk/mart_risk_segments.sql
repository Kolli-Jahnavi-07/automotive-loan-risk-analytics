-- Loss performance by credit tier, LTV band, and term, on loans seasoned 12+ months.
with as_of as (
    select max(as_of_month) as as_of_month from {{ ref('fct_loan_month') }}
),
loan_outcomes as (
    select
        loan_id,
        max(case when days_past_due >= 60 or status = 'charged_off' then 1 else 0 end) as ever_60_plus,
        max(case when status = 'charged_off' then 1 else 0 end) as charged_off,
        sum(net_loss) as net_loss
    from {{ ref('fct_loan_month') }}
    group by loan_id
)
select
    o.credit_tier,
    o.ltv_band,
    o.term_band,
    count(*) as loans,
    sum(o.amount_financed) as original_amount,
    avg(o.apr) as avg_apr,
    avg(o.credit_score) as avg_credit_score,
    avg(lo.ever_60_plus) as ever_60_plus_rate,
    avg(lo.charged_off) as charge_off_rate,
    sum(lo.net_loss) / sum(o.amount_financed) as net_loss_rate
from {{ ref('fct_loan_origination') }} o
join loan_outcomes lo using (loan_id)
cross join as_of a
where {{ dbt.datediff('o.origination_month', 'a.as_of_month', 'month') }} >= 12
group by o.credit_tier, o.ltv_band, o.term_band
