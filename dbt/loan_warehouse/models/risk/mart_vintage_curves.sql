-- Cumulative net loss by origination quarter and months on book.
-- Each vintage is cut at the months-on-book its youngest loan has fully observed,
-- so partially seasoned months never drag a curve down.
with vintages as (
    select
        vintage_quarter,
        count(*) as loans,
        sum(amount_financed) as original_amount,
        max(origination_month) as last_origination_month
    from {{ ref('fct_loan_origination') }}
    group by vintage_quarter
),
as_of as (
    select max(as_of_month) as as_of_month from {{ ref('fct_loan_month') }}
),
losses as (
    select o.vintage_quarter, m.months_on_book, sum(m.net_loss) as net_loss, sum(m.charge_off_amount) as gross_loss
    from {{ ref('fct_loan_month') }} m
    join {{ ref('fct_loan_origination') }} o using (loan_id)
    group by o.vintage_quarter, m.months_on_book
),
curve as (
    select
        l.vintage_quarter,
        l.months_on_book,
        sum(l.net_loss) over (partition by l.vintage_quarter order by l.months_on_book rows unbounded preceding) as cum_net_loss,
        sum(l.gross_loss) over (partition by l.vintage_quarter order by l.months_on_book rows unbounded preceding) as cum_gross_loss
    from losses l
)
select
    c.vintage_quarter,
    c.months_on_book,
    v.loans,
    v.original_amount,
    c.cum_gross_loss / v.original_amount as cum_gross_loss_rate,
    c.cum_net_loss / v.original_amount as cum_net_loss_rate
from curve c
join vintages v using (vintage_quarter)
cross join as_of a
where c.months_on_book <= {{ dbt.datediff('v.last_origination_month', 'a.as_of_month', 'month') }}
