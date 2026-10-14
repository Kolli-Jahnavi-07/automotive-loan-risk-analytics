"""Synthetic auto-loan data generator.

Produces the raw ("bronze") extracts a lender's systems would hand a data team:

* borrowers.csv, dealers.csv, vehicles.csv, loans.csv  - loan origination system extracts
* loan_performance.jsonl                                - monthly servicing snapshots

Performance is simulated with a monthly roll-rate model: each active loan can stay
current, roll into deeper delinquency, cure, prepay, or charge off. Roll rates scale
with credit score, loan-to-value, and term, so the downstream risk marts show the
patterns you would expect in a real auto portfolio (subprime and high-LTV loans
default more; longer terms season worse).

The generator also injects a small amount of realistic mess (duplicates, mixed date
formats, inconsistent casing, invalid amounts, late-arriving corrections, orphan
records) so the cleaning layer has real work to do.

All data is synthetic. Nothing here describes real people or real loans.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta

import numpy as np

STATES = ["TX", "CA", "FL", "GA", "OH", "MI", "NC", "AZ", "IL", "PA", "NY", "TN", "MA", "WA", "CO"]
MAKES = {
    "Chevrolet": ["Equinox", "Silverado 1500", "Malibu", "Traverse", "Blazer"],
    "GMC": ["Sierra 1500", "Terrain", "Acadia", "Yukon"],
    "Buick": ["Encore GX", "Enclave", "Envision"],
    "Cadillac": ["XT5", "Escalade", "CT5"],
    "Toyota": ["Camry", "RAV4", "Corolla", "Tacoma"],
    "Ford": ["F-150", "Escape", "Explorer"],
    "Honda": ["Civic", "CR-V", "Accord"],
}
TERMS = np.array([36, 48, 60, 72, 84])
TERM_P = np.array([0.08, 0.14, 0.30, 0.36, 0.12])

FIRST_ORIG = date(2022, 1, 1)
LAST_ORIG = date(2026, 3, 1)
AS_OF = date(2026, 9, 1)  # last performance month (month start)


@dataclass
class GenConfig:
    loans: int = 20_000
    seed: int = 7
    out_dir: str = "data/bronze"
    mess: bool = True


def month_add(d: date, n: int) -> date:
    y, m = divmod(d.month - 1 + n, 12)
    return date(d.year + y, m + 1, 1)


def months_between(a: date, b: date) -> int:
    return (b.year - a.year) * 12 + (b.month - a.month)


def amortized_payment(principal: np.ndarray, apr: np.ndarray, term: np.ndarray) -> np.ndarray:
    r = apr / 12.0
    safe_r = np.where(r > 0, r, 1.0)  # avoid 0/0 in the branch np.where discards
    return np.where(r > 0, principal * safe_r / (1 - (1 + safe_r) ** (-term)), principal / term)


def credit_tier(score: np.ndarray) -> np.ndarray:
    return np.select(
        [score >= 720, score >= 660, score >= 620],
        ["super_prime", "prime", "near_prime"],
        default="subprime",
    )


def generate(cfg: GenConfig) -> dict[str, int]:
    rng = np.random.default_rng(cfg.seed)
    os.makedirs(cfg.out_dir, exist_ok=True)
    n = cfg.loans

    # ---------- borrowers (some borrowers have more than one loan) ----------
    n_borrowers = max(1, int(n * 0.94))
    b_ids = np.array([f"B{100000 + i}" for i in range(n_borrowers)])
    score = np.clip(rng.normal(688, 72, n_borrowers), 500, 850).round().astype(int)
    income = np.clip(rng.lognormal(np.log(68_000), 0.45, n_borrowers), 18_000, 450_000).round(-2)
    b_state = rng.choice(STATES, n_borrowers)
    emp_years = np.clip(rng.gamma(2.2, 3.0, n_borrowers), 0, 40).round(1)

    # ---------- dealers ----------
    n_dealers = max(5, n // 120)
    d_ids = np.array([f"D{1000 + i}" for i in range(n_dealers)])
    d_state = rng.choice(STATES, n_dealers)
    d_type = rng.choice(["franchise", "independent"], n_dealers, p=[0.7, 0.3])
    # A few dealers send much riskier paper (weak verification or outright fraud).
    # They are not labeled in the extracts; the dealer-risk mart has to find them.
    n_bad = max(1, round(n_dealers * 0.03))
    bad_dealers = rng.choice(n_dealers, size=n_bad, replace=False)
    dealer_risk = np.ones(n_dealers)
    dealer_risk[bad_dealers] = 2.6

    # ---------- loans + vehicles ----------
    loan_ids = np.array([f"L{2000000 + i}" for i in range(n)])
    borrower_idx = np.concatenate([np.arange(n_borrowers), rng.integers(0, n_borrowers, n - n_borrowers)])
    rng.shuffle(borrower_idx)
    dealer_idx = rng.integers(0, n_dealers, n)

    span = months_between(FIRST_ORIG, LAST_ORIG)
    orig_month_offset = rng.integers(0, span + 1, n)
    orig_day = rng.integers(1, 29, n)
    orig_dates = [month_add(FIRST_ORIG, int(o)).replace(day=int(d)) for o, d in zip(orig_month_offset, orig_day)]

    is_new = rng.random(n) < 0.45
    make_names = list(MAKES)
    make = rng.choice(make_names, n, p=[0.22, 0.14, 0.08, 0.06, 0.2, 0.18, 0.12])
    model = np.array([rng.choice(MAKES[m]) for m in make])
    orig_year = np.array([d.year for d in orig_dates])
    model_year = np.where(is_new, orig_year + rng.integers(0, 2, n), orig_year - rng.integers(1, 8, n))
    vehicle_value = np.where(is_new, rng.normal(44_000, 11_000, n), rng.normal(26_000, 8_500, n))
    vehicle_value = np.clip(vehicle_value, 7_500, 110_000).round(-1)
    vins = np.array([f"VIN{rng.integers(10**9, 10**10)}" for _ in range(n)])

    s = score[borrower_idx]
    tier = credit_tier(s)
    down_pct = np.clip(rng.beta(2.0, 9.0, n) - (s < 620) * 0.03, 0.0, 0.5)
    fees = rng.normal(1_800, 400, n).clip(500, 4_000)
    amount = (vehicle_value * (1 - down_pct) + fees).round(2)
    ltv = (amount / vehicle_value).round(4)
    term = rng.choice(TERMS, n, p=TERM_P)
    base_apr = np.select(
        [tier == "super_prime", tier == "prime", tier == "near_prime"],
        [rng.uniform(0.049, 0.071, n), rng.uniform(0.071, 0.104, n), rng.uniform(0.11, 0.152, n)],
        default=rng.uniform(0.152, 0.229, n),
    )
    apr = (base_apr + (~is_new) * 0.015 + (term >= 72) * 0.006).round(4)
    payment = amortized_payment(amount, apr, term).round(2)

    # ---------- monthly performance simulation ----------
    risk = dealer_risk[dealer_idx] * np.exp(-(s - 690) / 75.0) * (ltv / 1.0) ** 1.6 * np.where(term >= 72, 1.25, 1.0) * np.where(is_new, 0.9, 1.15)
    p_roll_30 = np.clip(0.012 * risk, 0.001, 0.25)
    p_prepay = np.clip(0.011 * np.exp((s - 690) / 200.0), 0.003, 0.03)

    first_perf = np.array([months_between(FIRST_ORIG, month_add(d.replace(day=1), 1)) for d in orig_dates])
    last_perf = months_between(FIRST_ORIG, AS_OF)

    # state: 0 current, 1 dpd30, 2 dpd60, 3 dpd90, 4 charged_off, 5 paid_off
    state = np.zeros(n, dtype=int)
    balance = amount.copy()
    months_on_book = np.zeros(n, dtype=int)
    perf_rows: list[dict] = []
    r = apr / 12.0

    for t in range(int(first_perf.min()), last_perf + 1):
        active = (first_perf <= t) & (state < 4)
        if not active.any():
            continue
        idx = np.where(active)[0]
        u = rng.random(idx.size)
        st = state[idx]
        new = st.copy()

        # current -> prepay / roll to 30
        cur = st == 0
        new[cur & (u < p_prepay[idx])] = 5
        new[cur & (u >= p_prepay[idx]) & (u < p_prepay[idx] + p_roll_30[idx])] = 1
        # 30 -> cure / 60
        m30 = st == 1
        new[m30 & (u < 0.52)] = 0
        new[m30 & (u >= 0.52) & (u < 0.52 + 0.33)] = 2
        # 60 -> cure / 90
        m60 = st == 2
        new[m60 & (u < 0.24)] = 0
        new[m60 & (u >= 0.24) & (u < 0.24 + 0.52)] = 3
        # 90 -> cure / charge-off
        m90 = st == 3
        new[m90 & (u < 0.10)] = 0
        new[m90 & (u >= 0.10) & (u < 0.10 + 0.72)] = 4

        month = month_add(FIRST_ORIG, t)
        for j, k in enumerate(idx):
            prev, nxt = st[j], new[j]
            months_on_book[k] += 1
            sched = float(payment[k])
            open_bal = float(balance[k])
            interest = open_bal * float(r[k])
            received = 0.0
            recovery = 0.0
            charge_off = 0.0
            if nxt == 5:  # prepaid in full
                received = round(open_bal + interest, 2)
                balance[k] = 0.0
                status = "paid_off"
            elif nxt == 4:
                charge_off = round(open_bal, 2)
                recovery = round(open_bal * float(rng.uniform(0.32, 0.58)), 2)
                balance[k] = 0.0
                status = "charged_off"
            elif nxt == 0:
                missed = prev  # catch up on missed payments when curing
                received = round(sched * (1 + missed), 2)
                principal = max(0.0, received - interest * (1 + missed))
                balance[k] = max(0.0, open_bal - principal)
                if balance[k] < 1 or months_on_book[k] >= term[k] + missed:
                    balance[k] = 0.0
                    nxt = 5
                    status = "paid_off"
                else:
                    status = "current"
            else:
                status = {1: "dpd_30", 2: "dpd_60", 3: "dpd_90"}[nxt]
            new[j] = nxt
            perf_rows.append(
                {
                    "loan_id": loan_ids[k],
                    "as_of_month": month.isoformat(),
                    "scheduled_payment": round(sched, 2),
                    "payment_received": received,
                    "ending_balance": round(float(balance[k]), 2),
                    "days_past_due": {0: 0, 1: 30, 2: 60, 3: 90}.get(int(nxt), 0),
                    "status": status,
                    "charge_off_amount": charge_off,
                    "recovery_amount": recovery,
                    "ingested_at": datetime(month.year, month.month, 1).isoformat() + "Z",
                }
            )
        state[idx] = new

    # ---------- write bronze extracts ----------
    def write_csv(name: str, header: list[str], rows: list[list]) -> int:
        with open(os.path.join(cfg.out_dir, name), "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(header)
            w.writerows(rows)
        return len(rows)

    borrower_rows = []
    for i in range(n_borrowers):
        st_val = b_state[i]
        inc = income[i]
        if cfg.mess:
            if rng.random() < 0.04:
                st_val = f" {st_val.lower()} "  # inconsistent casing / whitespace
            if rng.random() < 0.01:
                inc = ""  # missing income
        borrower_rows.append([b_ids[i], int(score[i]), inc, st_val, emp_years[i]])

    dealer_rows = [[d_ids[i], f"Dealer {1000 + i}", d_state[i], d_type[i]] for i in range(n_dealers)]
    vehicle_rows = [
        [vins[i], make[i], model[i], int(model_year[i]), "new" if is_new[i] else "used", vehicle_value[i]] for i in range(n)
    ]

    loan_rows = []
    for i in range(n):
        od = orig_dates[i]
        od_str = od.isoformat()
        amt = amount[i]
        if cfg.mess:
            if rng.random() < 0.03:
                od_str = od.strftime("%m/%d/%Y")  # second source system uses US dates
            if rng.random() < 0.002:
                amt = -amt  # sign error from an upstream bug
        loan_rows.append(
            [loan_ids[i], b_ids[borrower_idx[i]], d_ids[dealer_idx[i]], vins[i], od_str, amt, apr[i], int(term[i]), payment[i]]
        )
    if cfg.mess:  # exact duplicate rows from a re-sent extract
        dup = rng.choice(len(loan_rows), size=max(1, len(loan_rows) // 200), replace=False)
        loan_rows.extend(loan_rows[i] for i in dup)

    counts = {
        "borrowers": write_csv("borrowers.csv", ["borrower_id", "credit_score", "annual_income", "state", "employment_years"], borrower_rows),
        "dealers": write_csv("dealers.csv", ["dealer_id", "dealer_name", "state", "dealer_type"], dealer_rows),
        "vehicles": write_csv("vehicles.csv", ["vin", "make", "model", "model_year", "condition", "vehicle_value"], vehicle_rows),
        "loans": write_csv(
            "loans.csv",
            ["loan_id", "borrower_id", "dealer_id", "vin", "origination_date", "amount_financed", "apr", "term_months", "monthly_payment"],
            loan_rows,
        ),
    }

    if cfg.mess:
        # late-arriving corrections: a re-sent snapshot with a newer ingested_at must win
        n_corr = max(1, len(perf_rows) // 300)
        for i in rng.choice(len(perf_rows), size=n_corr, replace=False):
            row = dict(perf_rows[i])
            ts = datetime.fromisoformat(row["ingested_at"].rstrip("Z")) + timedelta(days=3)
            row["ingested_at"] = ts.isoformat() + "Z"
            perf_rows.append(row)
        # orphan snapshots for loans the origination system never sent
        for i in range(max(1, len(perf_rows) // 2000)):
            perf_rows.append({**perf_rows[i], "loan_id": f"L9{i:06d}"})

    with open(os.path.join(cfg.out_dir, "loan_performance.jsonl"), "w") as f:
        for row in perf_rows:
            f.write(json.dumps(row) + "\n")
    counts["loan_performance"] = len(perf_rows)

    # Answer key for validation only (kept outside the bronze extracts).
    truth_dir = os.path.join(os.path.dirname(cfg.out_dir.rstrip("/")) or ".", "ground_truth")
    os.makedirs(truth_dir, exist_ok=True)
    with open(os.path.join(truth_dir, "high_risk_dealers.json"), "w") as f:
        json.dump(sorted(d_ids[bad_dealers].tolist()), f, indent=2)
    return counts


def main() -> None:
    p = argparse.ArgumentParser(description="Generate synthetic auto-loan bronze data")
    p.add_argument("--loans", type=int, default=20_000)
    p.add_argument("--seed", type=int, default=7)
    p.add_argument("--out", default="data/bronze")
    p.add_argument("--clean", action="store_true", help="skip injecting messy records")
    a = p.parse_args()
    counts = generate(GenConfig(loans=a.loans, seed=a.seed, out_dir=a.out, mess=not a.clean))
    for k, v in counts.items():
        print(f"{k:>18}: {v:,} rows")


if __name__ == "__main__":
    main()
