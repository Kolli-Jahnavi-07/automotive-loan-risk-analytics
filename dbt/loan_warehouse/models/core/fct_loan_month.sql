{{
    config(
        materialized = 'incremental',
        unique_key = ['loan_id', 'as_of_month'],
        incremental_strategy = 'delete+insert'
    )
}}
-- Grain: one row per loan per performance month.
-- Incremental: each run reprocesses the last few months so late servicing corrections land.
select
    p.loan_id,
    p.as_of_month,
    {{ dbt.datediff('o.origination_month', 'p.as_of_month', 'month') }} as months_on_book,
    p.scheduled_payment,
    p.payment_received,
    p.ending_balance,
    p.days_past_due,
    p.status,
    {{ dpd_bucket('p.days_past_due', 'p.status') }} as dpd_bucket,
    p.charge_off_amount,
    p.recovery_amount,
    p.charge_off_amount - p.recovery_amount as net_loss
from {{ ref('stg_loan_performance') }} p
join {{ ref('fct_loan_origination') }} o using (loan_id)
{% if is_incremental() %}
where p.as_of_month >= (
    select {{ dbt.dateadd('month', -1 * var('late_arrival_months'), 'max(as_of_month)') }} from {{ this }}
)
{% endif %}
