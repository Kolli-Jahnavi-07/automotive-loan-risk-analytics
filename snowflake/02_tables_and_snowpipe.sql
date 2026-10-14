-- Silver tables and Snowpipe auto-ingest. Each new Parquet file the Spark job lands
-- in S3 triggers an S3 event -> SQS -> Snowpipe load, so silver stays current without
-- a scheduled COPY.

use schema LOAN_RISK.SILVER;

create table if not exists LOANS (
  loan_id string, borrower_id string, dealer_id string, vin string,
  origination_date date, amount_financed number(12,2), apr number(6,4),
  term_months int, monthly_payment number(10,2)
);

create table if not exists LOAN_PERFORMANCE (
  loan_id string, as_of_month date, scheduled_payment number(10,2),
  payment_received number(12,2), ending_balance number(12,2), days_past_due int,
  status string, charge_off_amount number(12,2), recovery_amount number(12,2),
  ingested_at timestamp_ntz, perf_year int
);

create table if not exists BORROWERS (borrower_id string, credit_score int, annual_income number(12,2), state string, employment_years number(4,1));
create table if not exists DEALERS   (dealer_id string, dealer_name string, state string, dealer_type string);
create table if not exists VEHICLES  (vin string, make string, model string, model_year int, condition string, vehicle_value number(12,2));

create pipe if not exists LOAN_PERFORMANCE_PIPE auto_ingest = true as
  copy into LOAN_PERFORMANCE
  from @SILVER_STAGE/loan_performance/
  match_by_column_name = case_insensitive
  file_format = (format_name = PARQUET_FMT);

create pipe if not exists LOANS_PIPE auto_ingest = true as
  copy into LOANS
  from @SILVER_STAGE/loans/
  match_by_column_name = case_insensitive
  file_format = (format_name = PARQUET_FMT);

-- Dimensions are small full refreshes; the Airflow DAG runs these COPY statements directly.
-- copy into BORROWERS from @SILVER_STAGE/borrowers/ match_by_column_name = case_insensitive;
-- copy into DEALERS   from @SILVER_STAGE/dealers/   match_by_column_name = case_insensitive;
-- copy into VEHICLES  from @SILVER_STAGE/vehicles/  match_by_column_name = case_insensitive;

-- Check pipe health
-- select system$pipe_status('LOAN_RISK.SILVER.LOAN_PERFORMANCE_PIPE');
