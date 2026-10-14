import csv
import json

import numpy as np

from loanrisk.generate import GenConfig, amortized_payment, credit_tier, generate


def test_amortized_payment_matches_known_value():
    # $30,000 at 6% APR for 60 months is $579.98/month
    p = amortized_payment(np.array([30_000.0]), np.array([0.06]), np.array([60]))
    assert round(float(p[0]), 2) == 579.98


def test_zero_rate_payment_is_straight_line():
    p = amortized_payment(np.array([12_000.0]), np.array([0.0]), np.array([48]))
    assert float(p[0]) == 250.0


def test_credit_tiers():
    tiers = credit_tier(np.array([800, 700, 640, 550]))
    assert tiers.tolist() == ["super_prime", "prime", "near_prime", "subprime"]


def test_generation_is_deterministic(tmp_path):
    a, b = tmp_path / "a" / "bronze", tmp_path / "b" / "bronze"
    generate(GenConfig(loans=300, seed=11, out_dir=str(a)))
    generate(GenConfig(loans=300, seed=11, out_dir=str(b)))
    assert (a / "loans.csv").read_text() == (b / "loans.csv").read_text()


def test_loans_reach_a_terminal_state_only_once(tmp_path):
    out = tmp_path / "bronze"
    generate(GenConfig(loans=500, seed=3, out_dir=str(out), mess=False))
    closed = {}
    with open(out / "loan_performance.jsonl") as f:
        for line in f:
            r = json.loads(line)
            if r["status"] in ("paid_off", "charged_off"):
                assert r["loan_id"] not in closed, "loan closed twice"
                closed[r["loan_id"]] = r["as_of_month"]
    assert closed, "expected some loans to close"


def test_clean_mode_has_no_injected_mess(tmp_path):
    out = tmp_path / "bronze"
    generate(GenConfig(loans=400, seed=5, out_dir=str(out), mess=False))
    with open(out / "loans.csv") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 400
    assert all(float(r["amount_financed"]) > 0 for r in rows)
    assert all("/" not in r["origination_date"] for r in rows)


def test_ground_truth_written_outside_bronze(tmp_path):
    out = tmp_path / "data" / "bronze"
    generate(GenConfig(loans=300, seed=1, out_dir=str(out)))
    truth = json.loads((tmp_path / "data" / "ground_truth" / "high_risk_dealers.json").read_text())
    assert truth and all(d.startswith("D") for d in truth)
