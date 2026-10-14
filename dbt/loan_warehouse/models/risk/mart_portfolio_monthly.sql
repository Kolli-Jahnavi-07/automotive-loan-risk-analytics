-- Portfolio health by month: size, delinquency mix by balance, and credit losses.
with perf as (
    select
        as_of_month,
        count(case when status not in ('paid_off', 'charged_off') then 1 end) as active_loans,
        sum(case when status not in ('paid_off', 'charged_off') then ending_balance else 0 end) as outstanding_balance,
        sum(case when days_past_due >= 30 and status not in ('paid_off', 'charged_off') then ending_balance else 0 end) as balance_30_plus,
        sum(case when days_past_due >= 60 and status not in ('paid_off', 'charged_off') then ending_balance else 0 end) as balance_60_plus,
        sum(case when days_past_due >= 90 and status not in ('paid_off', 'charged_off') then ending_balance else 0 end) as balance_90_plus,
        count(case when status = 'charged_off' then 1 end) as charge_offs,
        sum(charge_off_amount) as gross_charge_off,
        sum(recovery_amount) as recoveries,
        sum(net_loss) as net_loss
    from {{ ref('fct_loan_month') }}
    group by as_of_month
),
orig as (
    select origination_month, count(*) as new_loans, sum(amount_financed) as new_volume
    from {{ ref('fct_loan_origination') }}
    group by origination_month
)
select
    p.as_of_month,
    coalesce(o.new_loans, 0) as new_loans,
    coalesce(o.new_volume, 0) as new_volume,
    p.active_loans,
    p.outstanding_balance,
    p.balance_30_plus / nullif(p.outstanding_balance, 0) as dq_30_plus_rate,
    p.balance_60_plus / nullif(p.outstanding_balance, 0) as dq_60_plus_rate,
    p.balance_90_plus / nullif(p.outstanding_balance, 0) as dq_90_plus_rate,
    p.charge_offs,
    p.gross_charge_off,
    p.recoveries,
    p.net_loss,
    -- annualized net charge-off rate on the balance at risk that month
    12 * p.net_loss / nullif(p.outstanding_balance + p.gross_charge_off, 0) as annualized_net_loss_rate
from perf p
left join orig o on o.origination_month = p.as_of_month
