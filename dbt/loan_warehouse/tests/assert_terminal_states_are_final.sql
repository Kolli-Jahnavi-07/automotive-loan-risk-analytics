-- Once a loan is charged off or paid off, no later snapshot may exist for it.
with terminal as (
    select loan_id, min(as_of_month) as closed_month
    from {{ ref('fct_loan_month') }}
    where status in ('charged_off', 'paid_off')
    group by loan_id
)
select m.loan_id, m.as_of_month, t.closed_month
from {{ ref('fct_loan_month') }} m
join terminal t using (loan_id)
where m.as_of_month > t.closed_month
