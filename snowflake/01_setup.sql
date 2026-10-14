-- Production layout for the loan risk warehouse in Snowflake.
-- Run once as SYSADMIN / SECURITYADMIN. Not executed in CI (no Snowflake account there);
-- locally the same dbt models run on DuckDB.

create warehouse if not exists LOAD_WH      warehouse_size = xsmall auto_suspend = 60 auto_resume = true;
create warehouse if not exists TRANSFORM_WH warehouse_size = small  auto_suspend = 60 auto_resume = true;

create database if not exists LOAN_RISK;
create schema if not exists LOAN_RISK.SILVER;     -- Parquet from the Spark job, loaded by Snowpipe
create schema if not exists LOAN_RISK.ANALYTICS;  -- dbt staging / core / risk models

create role if not exists LOADER;
create role if not exists TRANSFORMER;
create role if not exists ANALYST;

grant usage on warehouse LOAD_WH to role LOADER;
grant usage on warehouse TRANSFORM_WH to role TRANSFORMER;
grant usage on database LOAN_RISK to role LOADER;
grant usage on database LOAN_RISK to role TRANSFORMER;
grant usage on database LOAN_RISK to role ANALYST;
grant all on schema LOAN_RISK.SILVER to role LOADER;
grant usage, select on all tables in schema LOAN_RISK.SILVER to role TRANSFORMER;  -- dbt reads silver
grant all on schema LOAN_RISK.ANALYTICS to role TRANSFORMER;
grant usage on schema LOAN_RISK.ANALYTICS to role ANALYST;
grant select on future tables in schema LOAN_RISK.ANALYTICS to role ANALYST;

create file format if not exists LOAN_RISK.SILVER.PARQUET_FMT type = parquet;

-- The Spark job writes silver Parquet to s3://<bucket>/loan-risk/silver/<table>/
create stage if not exists LOAN_RISK.SILVER.SILVER_STAGE
  url = 's3://<bucket>/loan-risk/silver/'
  storage_integration = S3_INT
  file_format = LOAN_RISK.SILVER.PARQUET_FMT;
