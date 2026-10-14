import datetime as dt

from pyspark.sql import Row

from loanrisk.spark_silver import clean_borrowers, clean_loans, latest_performance


def _loans(spark, rows):
    cols = ["loan_id", "borrower_id", "dealer_id", "vin", "origination_date", "amount_financed", "apr", "term_months", "monthly_payment"]
    return spark.createDataFrame([Row(**dict(zip(cols, r))) for r in rows])


def test_borrower_state_is_standardized(spark):
    df = spark.createDataFrame([Row(borrower_id="B1", credit_score=700, annual_income=1.0, state=" tx ", employment_years=2.0)])
    good, bad = clean_borrowers(df)
    assert good.collect()[0]["state"] == "TX"
    assert bad.count() == 0


def test_borrower_with_impossible_score_is_quarantined(spark):
    df = spark.createDataFrame([Row(borrower_id="B1", credit_score=990, annual_income=1.0, state="TX", employment_years=2.0)])
    good, bad = clean_borrowers(df)
    assert good.count() == 0
    assert bad.collect()[0]["reject_reason"] == "invalid_credit_score_or_id"


def test_loans_parse_both_date_formats_and_drop_exact_duplicates(spark):
    borrowers = spark.createDataFrame([Row(borrower_id="B1")])
    dealers = spark.createDataFrame([Row(dealer_id="D1")])
    loans = _loans(
        spark,
        [
            ("L1", "B1", "D1", "V1", "2024-03-05", 20000.0, 0.08, 60, 405.0),
            ("L1", "B1", "D1", "V1", "2024-03-05", 20000.0, 0.08, 60, 405.0),  # exact duplicate
            ("L2", "B1", "D1", "V2", "03/07/2024", 15000.0, 0.09, 48, 373.0),  # US date format
        ],
    )
    good, bad = clean_loans(loans, borrowers, dealers)
    got = {r["loan_id"]: r["origination_date"] for r in good.collect()}
    assert got == {"L1": dt.date(2024, 3, 5), "L2": dt.date(2024, 3, 7)}
    assert bad.count() == 0


def test_loans_are_quarantined_with_a_reason(spark):
    borrowers = spark.createDataFrame([Row(borrower_id="B1")])
    dealers = spark.createDataFrame([Row(dealer_id="D1")])
    loans = _loans(
        spark,
        [
            ("L1", "B1", "D1", "V1", "2024-03-05", -20000.0, 0.08, 60, 405.0),
            ("L2", "B9", "D1", "V2", "2024-03-05", 20000.0, 0.08, 60, 405.0),
            ("L3", "B1", "D1", "V3", "not a date", 20000.0, 0.08, 60, 405.0),
            ("L4", "B1", "D1", "V4", "2024-03-05", 20000.0, 0.80, 60, 405.0),
        ],
    )
    good, bad = clean_loans(loans, borrowers, dealers)
    reasons = {r["loan_id"]: r["reject_reason"] for r in bad.collect()}
    assert good.count() == 0
    assert reasons == {
        "L1": "non_positive_amount",
        "L2": "unknown_borrower",
        "L3": "unparseable_origination_date",
        "L4": "apr_out_of_range",
    }


def test_latest_performance_keeps_newest_version_and_routes_orphans(spark):
    perf = spark.createDataFrame(
        [
            Row(loan_id="L1", as_of_month="2024-05-01", days_past_due=0, ingested_at="2024-05-01T00:00:00Z"),
            Row(loan_id="L1", as_of_month="2024-05-01", days_past_due=30, ingested_at="2024-05-04T00:00:00Z"),
            Row(loan_id="L2", as_of_month="2024-05-01", days_past_due=0, ingested_at="2024-05-01T00:00:00Z"),
            Row(loan_id="L9", as_of_month="2024-05-01", days_past_due=0, ingested_at="2024-05-01T00:00:00Z"),
        ]
    )
    loans = spark.createDataFrame([Row(loan_id="L1")])
    rejected = spark.createDataFrame([Row(loan_id="L2")])
    good, bad = latest_performance(perf, loans, rejected)
    rows = good.collect()
    assert len(rows) == 1 and rows[0]["days_past_due"] == 30  # the correction wins
    assert {r["loan_id"]: r["reject_reason"] for r in bad.collect()} == {
        "L2": "parent_loan_rejected",
        "L9": "orphan_loan_id",
    }
