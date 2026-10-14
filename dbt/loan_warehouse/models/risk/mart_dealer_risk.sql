-- Early-default rate by dealer: share of loans that hit 60+ days past due within
-- their first 12 months on book. Dealers well above the portfolio rate get flagged
-- for review (possible fraud, weak underwriting, or a bad lending partner).
with as_of as (
    select max(as_of_month) as as_of_month from {{ ref('fct_loan_month') }}
),
seasoned as (
    select o.loan_id, o.dealer_id
    from {{ ref('fct_loan_origination') }} o
    cross join as_of a
    where {{ dbt.datediff('o.origination_month', 'a.as_of_month', 'month') }} >= 12
),
early as (
    select
        s.dealer_id,
        s.loan_id,
        max(case when m.months_on_book <= 12 and (m.days_past_due >= 60 or m.status = 'charged_off') then 1 else 0 end) as early_default
    from seasoned s
    join {{ ref('fct_loan_month') }} m using (loan_id)
    group by s.dealer_id, s.loan_id
),
by_dealer as (
    select dealer_id, count(*) as seasoned_loans, avg(early_default) as early_default_rate
    from early
    group by dealer_id
),
portfolio as (
    select avg(early_default) as portfolio_rate from early
)
select
    d.dealer_id,
    dd.dealer_name,
    dd.dealer_state,
    dd.dealer_type,
    d.seasoned_loans,
    d.early_default_rate,
    p.portfolio_rate,
    d.early_default_rate / nullif(p.portfolio_rate, 0) as relative_risk,
    -- how many standard errors above the portfolio rate; use to prioritize reviews
    (d.early_default_rate - p.portfolio_rate)
        / sqrt(p.portfolio_rate * (1 - p.portfolio_rate) / d.seasoned_loans) as z_score,
    (d.seasoned_loans >= 30 and d.early_default_rate >= 1.75 * p.portfolio_rate) as flagged_for_review
from by_dealer d
join {{ ref('dim_dealer') }} dd using (dealer_id)
cross join portfolio p
