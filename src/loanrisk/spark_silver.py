"""Bronze -> silver with PySpark.

Reads the raw extracts, enforces types, standardizes values, removes duplicates,
and quarantines records that fail validation (with the reason attached) instead of
silently dropping them. Writes Parquet for the warehouse layer and a JSON run report
with row counts per table, so every run is auditable.

Rules applied
-------------
borrowers         trim + upper-case state, credit score must be 300-850
loans             parse ISO and US (MM/DD/YYYY) dates, drop exact duplicates,
                  quarantine non-positive amounts and unknown borrowers/dealers
loan_performance  keep the latest ingested version of each (loan_id, as_of_month),
                  quarantine snapshots for loans that do not exist (orphans)
"""

from __future__ import annotations

import argparse
import json
import os
import time

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

LOAN_SCHEMA = T.StructType(
    [
        T.StructField("loan_id", T.StringType()),
        T.StructField("borrower_id", T.StringType()),
        T.StructField("dealer_id", T.StringType()),
        T.StructField("vin", T.StringType()),
        T.StructField("origination_date", T.StringType()),
        T.StructField("amount_financed", T.DoubleType()),
        T.StructField("apr", T.DoubleType()),
        T.StructField("term_months", T.IntegerType()),
        T.StructField("monthly_payment", T.DoubleType()),
    ]
)

BORROWER_SCHEMA = T.StructType(
    [
        T.StructField("borrower_id", T.StringType()),
        T.StructField("credit_score", T.IntegerType()),
        T.StructField("annual_income", T.DoubleType()),
        T.StructField("state", T.StringType()),
        T.StructField("employment_years", T.DoubleType()),
    ]
)

PERF_SCHEMA = T.StructType(
    [
        T.StructField("loan_id", T.StringType()),
        T.StructField("as_of_month", T.StringType()),
        T.StructField("scheduled_payment", T.DoubleType()),
        T.StructField("payment_received", T.DoubleType()),
        T.StructField("ending_balance", T.DoubleType()),
        T.StructField("days_past_due", T.IntegerType()),
        T.StructField("status", T.StringType()),
        T.StructField("charge_off_amount", T.DoubleType()),
        T.StructField("recovery_amount", T.DoubleType()),
        T.StructField("ingested_at", T.StringType()),
    ]
)


def spark_session(app: str = "loan-risk-silver") -> SparkSession:
    return (
        SparkSession.builder.master(os.environ.get("SPARK_MASTER", "local[*]"))
        .appName(app)
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8"))
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Pure transformations (unit-tested in tests/test_spark_silver.py)
# ---------------------------------------------------------------------------

def clean_borrowers(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    df = df.withColumn("state", F.upper(F.trim("state")))
    bad = F.col("credit_score").isNull() | ~F.col("credit_score").between(300, 850) | F.col("borrower_id").isNull()
    quarantined = df.filter(bad).withColumn("reject_reason", F.lit("invalid_credit_score_or_id"))
    good = df.filter(~bad).dropDuplicates(["borrower_id"])
    return good, quarantined


def parse_origination_date(col: str) -> F.Column:
    """Accept ISO dates and the US format one upstream system sends; anything else becomes NULL."""
    return F.coalesce(
        F.try_to_timestamp(F.col(col), F.lit("yyyy-MM-dd")),
        F.try_to_timestamp(F.col(col), F.lit("MM/dd/yyyy")),
    ).cast("date")


def clean_loans(df: DataFrame, borrowers: DataFrame, dealers: DataFrame) -> tuple[DataFrame, DataFrame]:
    df = df.dropDuplicates().withColumn("origination_date", parse_origination_date("origination_date"))

    known_b = borrowers.select("borrower_id").withColumn("_b", F.lit(True))
    known_d = dealers.select("dealer_id").withColumn("_d", F.lit(True))
    df = df.join(known_b, "borrower_id", "left").join(known_d, "dealer_id", "left")

    reason = (
        F.when(F.col("origination_date").isNull(), "unparseable_origination_date")
        .when(F.col("amount_financed") <= 0, "non_positive_amount")
        .when(F.col("apr").isNull() | (F.col("apr") <= 0) | (F.col("apr") > 0.36), "apr_out_of_range")
        .when(F.col("_b").isNull(), "unknown_borrower")
        .when(F.col("_d").isNull(), "unknown_dealer")
    )
    df = df.withColumn("reject_reason", reason).drop("_b", "_d")
    quarantined = df.filter(F.col("reject_reason").isNotNull())
    good = df.filter(F.col("reject_reason").isNull()).drop("reject_reason")
    # a loan id must be unique after cleaning
    w = Window.partitionBy("loan_id").orderBy(F.col("origination_date").desc())
    good = good.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    return good, quarantined


def latest_performance(
    df: DataFrame, loans: DataFrame, rejected_loans: DataFrame | None = None
) -> tuple[DataFrame, DataFrame]:
    """Keep the newest version of each monthly snapshot and route snapshots without a valid loan.

    Snapshots for loans that were themselves quarantined are tagged ``parent_loan_rejected``
    so they can be replayed once the loan is fixed; snapshots for loan ids that never
    existed are tagged ``orphan_loan_id``.
    """
    df = (
        df.withColumn("as_of_month", F.to_date("as_of_month"))
        .withColumn("ingested_at", F.to_timestamp("ingested_at"))
    )
    w = Window.partitionBy("loan_id", "as_of_month").orderBy(F.col("ingested_at").desc())
    df = df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

    known = loans.select("loan_id").withColumn("_known", F.lit(True))
    df = df.join(known, "loan_id", "left")
    if rejected_loans is not None:
        rej = rejected_loans.select("loan_id").distinct().withColumn("_rejected", F.lit(True))
        df = df.join(rej, "loan_id", "left")
    else:
        df = df.withColumn("_rejected", F.lit(None).cast("boolean"))
    reason = F.when(F.col("_rejected"), "parent_loan_rejected").otherwise("orphan_loan_id")
    quarantined = (
        df.filter(F.col("_known").isNull()).withColumn("reject_reason", reason).drop("_known", "_rejected")
    )
    good = df.filter(F.col("_known")).drop("_known", "_rejected")
    return good, quarantined


# ---------------------------------------------------------------------------
# Job
# ---------------------------------------------------------------------------

def run(bronze: str, silver: str, quarantine: str, metrics_path: str) -> dict:
    spark = spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    t0 = time.time()
    read_csv = lambda name, schema=None: (  # noqa: E731
        spark.read.option("header", True).schema(schema).csv(f"{bronze}/{name}")
        if schema
        else spark.read.option("header", True).option("inferSchema", True).csv(f"{bronze}/{name}")
    )

    borrowers_raw = read_csv("borrowers.csv", BORROWER_SCHEMA)
    dealers = read_csv("dealers.csv").dropDuplicates(["dealer_id"])
    vehicles = read_csv("vehicles.csv").dropDuplicates(["vin"])
    loans_raw = read_csv("loans.csv", LOAN_SCHEMA)
    perf_raw = spark.read.schema(PERF_SCHEMA).json(f"{bronze}/loan_performance.jsonl")

    borrowers, q_b = clean_borrowers(borrowers_raw)
    loans, q_l = clean_loans(loans_raw, borrowers, dealers)
    loans = loans.cache()
    perf, q_p = latest_performance(perf_raw, loans, q_l)

    out = {
        "borrowers": borrowers,
        "dealers": dealers,
        "vehicles": vehicles,
        "loans": loans,
        "loan_performance": perf,
    }
    metrics: dict = {"tables": {}}
    for name, df in out.items():
        writer = df.write.mode("overwrite")
        if name == "loan_performance":
            df = df.withColumn("perf_year", F.year("as_of_month"))
            writer = df.write.mode("overwrite").partitionBy("perf_year")
        writer.parquet(f"{silver}/{name}")
        metrics["tables"][name] = {"rows_written": spark.read.parquet(f"{silver}/{name}").count()}

    raw_counts = {
        "borrowers": borrowers_raw.count(),
        "dealers": None,
        "vehicles": None,
        "loans": loans_raw.count(),
        "loan_performance": perf_raw.count(),
    }
    for name, q in {"borrowers": q_b, "loans": q_l, "loan_performance": q_p}.items():
        q.write.mode("overwrite").parquet(f"{quarantine}/{name}")
        reasons = {r["reject_reason"]: r["n"] for r in q.groupBy("reject_reason").agg(F.count("*").alias("n")).collect()}
        metrics["tables"][name]["quarantined"] = reasons
    for name, n in raw_counts.items():
        if n is not None:
            t = metrics["tables"][name]
            t["rows_read"] = n
            t["duplicates_removed"] = n - t["rows_written"] - sum(t["quarantined"].values())

    metrics["seconds"] = round(time.time() - t0, 1)
    os.makedirs(os.path.dirname(metrics_path), exist_ok=True)
    with open(metrics_path, "w") as f:
        json.dump(metrics, f, indent=2)
    spark.stop()
    return metrics


def main() -> None:
    p = argparse.ArgumentParser(description="Clean bronze extracts into silver Parquet")
    p.add_argument("--bronze", default="data/bronze")
    p.add_argument("--silver", default="data/silver")
    p.add_argument("--quarantine", default="data/quarantine")
    p.add_argument("--metrics", default="data/metrics/silver_run.json")
    a = p.parse_args()
    m = run(a.bronze, a.silver, a.quarantine, a.metrics)
    print(json.dumps(m, indent=2))


if __name__ == "__main__":
    main()
