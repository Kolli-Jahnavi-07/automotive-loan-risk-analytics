"""Build docs/results.md and the README charts from the warehouse marts.

Everything in docs/ is produced by this script from a real pipeline run, so the
numbers in the README always match what the code does.
"""

from __future__ import annotations

import argparse
import json
import os

import duckdb
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

# Validated categorical palette (light surface) - assigned in fixed order.
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4"]
SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_2 = "#52514e"
GRID = "#e4e3df"


def style_axes(ax, title: str, ylabel: str) -> None:
    ax.set_facecolor(SURFACE)
    ax.set_title(title, loc="left", fontsize=13, color=INK, pad=12, fontweight="bold")
    ax.set_ylabel(ylabel, color=INK_2, fontsize=10)
    ax.tick_params(colors=INK_2, labelsize=9, length=0)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(GRID)


def pct(x: float, digits: int = 1) -> str:
    return f"{x * 100:.{digits}f}%"


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="data/warehouse.duckdb")
    p.add_argument("--metrics", default="data/metrics/silver_run.json")
    p.add_argument("--truth", default="data/ground_truth/high_risk_dealers.json")
    p.add_argument("--out", default="docs")
    a = p.parse_args()

    os.makedirs(f"{a.out}/img", exist_ok=True)
    con = duckdb.connect(a.db, read_only=True)
    q = lambda sql: con.sql(sql).fetchall()  # noqa: E731

    # ---------- chart 1: vintage loss curves (one vintage per year) ----------
    vintages = [r[0] for r in q("select distinct vintage_quarter from risk.mart_vintage_curves where vintage_quarter like '%-Q1' order by 1")]
    fig, ax = plt.subplots(figsize=(8, 4.4), dpi=150, facecolor=SURFACE)
    for color, v in zip(SERIES, vintages):
        pts = q(f"select months_on_book, cum_net_loss_rate from risk.mart_vintage_curves where vintage_quarter = '{v}' order by 1")
        ax.plot([x for x, _ in pts], [y * 100 for _, y in pts], color=color, linewidth=2, label=v, solid_capstyle="round")
    style_axes(ax, "Cumulative net loss by vintage", "Net loss, % of original balance")
    ax.set_xlabel("Months on book", color=INK_2, fontsize=10)
    ax.legend(frameon=False, fontsize=9, labelcolor=INK, loc="upper left")
    fig.tight_layout()
    fig.savefig(f"{a.out}/img/vintage_curves.png", facecolor=SURFACE)
    plt.close(fig)

    # ---------- chart 2: charge-off rate by credit tier ----------
    tiers = q(
        """
        select credit_tier, sum(charge_off_rate * loans) / sum(loans) as co
        from risk.mart_risk_segments group by 1
        order by case credit_tier when 'super_prime' then 1 when 'prime' then 2 when 'near_prime' then 3 else 4 end
        """
    )
    labels = [t.replace("_", " ").title() for t, _ in tiers]
    vals = [v * 100 for _, v in tiers]
    fig, ax = plt.subplots(figsize=(8, 4), dpi=150, facecolor=SURFACE)
    bars = ax.bar(labels, vals, color=SERIES[0], width=0.55)
    for b, v in zip(bars, vals):
        ax.text(b.get_x() + b.get_width() / 2, v + 0.5, f"{v:.1f}%", ha="center", va="bottom", fontsize=10, color=INK)
    style_axes(ax, "Charge-off rate by credit tier (loans seasoned 12+ months)", "Loans charged off, %")
    fig.tight_layout()
    fig.savefig(f"{a.out}/img/chargeoff_by_tier.png", facecolor=SURFACE)
    plt.close(fig)

    # ---------- chart 3: 30+ day delinquency over time ----------
    dq = q("select as_of_month, dq_30_plus_rate from risk.mart_portfolio_monthly where active_loans >= 500 order by 1")
    fig, ax = plt.subplots(figsize=(8, 3.8), dpi=150, facecolor=SURFACE)
    ax.plot([d for d, _ in dq], [v * 100 for _, v in dq], color=SERIES[0], linewidth=2)
    style_axes(ax, "30+ day delinquency, share of outstanding balance", "Balance 30+ days past due, %")
    fig.tight_layout()
    fig.savefig(f"{a.out}/img/delinquency_trend.png", facecolor=SURFACE)
    plt.close(fig)

    # ---------- results.md ----------
    metrics = json.load(open(a.metrics))
    truth = set(json.load(open(a.truth))) if os.path.exists(a.truth) else set()
    flagged = {r[0] for r in q("select dealer_id from risk.mart_dealer_risk where flagged_for_review")}
    latest = q("select as_of_month, active_loans, outstanding_balance, dq_30_plus_rate, dq_60_plus_rate, annualized_net_loss_rate from risk.mart_portfolio_monthly order by 1 desc limit 1")[0]
    seg = q(
        """
        select credit_tier, sum(loans), sum(charge_off_rate * loans) / sum(loans), sum(net_loss_rate * original_amount) / sum(original_amount), sum(avg_apr * loans) / sum(loans)
        from risk.mart_risk_segments group by 1
        order by case credit_tier when 'super_prime' then 1 when 'prime' then 2 when 'near_prime' then 3 else 4 end
        """
    )
    ltv = q("select ltv_band, sum(loans), sum(charge_off_rate * loans) / sum(loans) from risk.mart_risk_segments group by 1 order by case ltv_band when '< 80%' then 1 when '80-100%' then 2 when '100-120%' then 3 else 4 end")
    top = q("select dealer_id, seasoned_loans, early_default_rate, relative_risk, z_score from risk.mart_dealer_risk where flagged_for_review order by z_score desc")

    t = metrics["tables"]
    lines = [
        "# Results from the latest run",
        "",
        "Generated by `scripts/report.py` from a full pipeline run on synthetic data. Re-run `make all` to reproduce.",
        "",
        "## Data cleaning (Spark silver job)",
        "",
        "| Table | Rows read | Rows written | Duplicates removed | Quarantined |",
        "|---|---:|---:|---:|---|",
    ]
    for name in ("borrowers", "loans", "loan_performance"):
        m = t[name]
        qtxt = ", ".join(f"{k}: {v:,}" for k, v in sorted(m["quarantined"].items())) or "none"
        lines.append(f"| {name} | {m['rows_read']:,} | {m['rows_written']:,} | {m['duplicates_removed']:,} | {qtxt} |")
    lines += [
        "",
        f"Spark job runtime: {metrics['seconds']} s in Spark local mode.",
        "",
        f"## Portfolio as of {latest[0]:%B %Y}",
        "",
        "| Active loans | Outstanding balance | 30+ DQ (balance) | 60+ DQ (balance) | Annualized net loss |",
        "|---:|---:|---:|---:|---:|",
        f"| {latest[1]:,} | ${latest[2] / 1e6:,.1f}M | {pct(latest[3])} | {pct(latest[4])} | {pct(latest[5])} |",
        "",
        "## Loss by credit tier (loans seasoned 12+ months)",
        "",
        "| Credit tier | Loans | Avg APR | Charge-off rate | Net loss rate |",
        "|---|---:|---:|---:|---:|",
    ]
    for tier, n, co, nl, apr in seg:
        lines.append(f"| {tier.replace('_', ' ')} | {n:,} | {pct(apr)} | {pct(co)} | {pct(nl)} |")
    lines += ["", "## Charge-off rate by loan-to-value", "", "| LTV band | Loans | Charge-off rate |", "|---|---:|---:|"]
    for band, n, co in ltv:
        lines.append(f"| {band} | {n:,} | {pct(co)} |")
    lines += [
        "",
        "## Dealer early-default review",
        "",
        f"The generator secretly assigns higher-risk lending to {len(truth)} dealers (answer key kept outside the pipeline). "
        f"The dealer-risk mart flagged {len(flagged)} dealers and caught **{len(truth & flagged)} of {len(truth)}**, "
        f"with {len(flagged - truth)} false alarms from ordinary dealers having an unlucky year. "
        "Flags are a review queue, not an automatic action; `z_score` orders the queue.",
        "",
        "| Dealer | Seasoned loans | Early-default rate | Relative risk | z-score | Seeded high-risk? |",
        "|---|---:|---:|---:|---:|---|",
    ]
    for d, n, edr, rr, z in top:
        lines.append(f"| {d} | {n} | {pct(edr)} | {rr:.2f}x | {z:.1f} | {'yes' if d in truth else 'no'} |")
    with open(f"{a.out}/results.md", "w") as f:
        f.write("\n".join(lines) + "\n")
    print(f"wrote {a.out}/results.md and 3 charts")


if __name__ == "__main__":
    main()
