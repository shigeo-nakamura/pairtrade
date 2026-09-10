#!/usr/bin/env python3
"""Engine B Step 0: is the US perp already absorbing the KRX session?

bot-strategy#988. Consumes the JSONL written by `engine_b_step0_extract.py`
and answers, per price type, the two questions the kill rules are written on:

    r_KR      = ln(mid_KR(t1) / mid_KR(t0))     KRX-session move of the KR name
    r_US_conc = ln(mid_US(t1) / mid_US(t0))     the US perp's move over the same hours
    fwd       = ln(mid_US(t2) / mid_US(t1))     what Engine B would try to capture
    eps       = r_KR - beta * r_US_conc         KR move the US perp has not already made

Kill rules (frozen in the issue before the data was looked at):

    K0-a: sd(fwd) < 14 bps                       -> kill (the move to capture is
                                                    under twice the 7 bps round trip)
    K0-b: R^2 >= 0.8 and sd(eps) < 14 bps        -> kill (already absorbed, and the
                                                    residual has no tradable width)

`corr(eps, fwd)` is reported but is explicitly *not* a go/kill input: at this
sample size the sign of a correlation is noise, while the two dispersions are
what converge fast enough to act on.

Every dispersion is reported with an exact (normal-theory) chi-square interval
rather than a bare point estimate. With a handful of sessions the estimate of a
standard deviation is itself uncertain by a factor of several, so a kill call is
only safe when the whole interval sits on one side of the threshold — and a
"no kill" is only safe when the whole interval sits on the other.

Each statistic uses every day that can support it: `fwd` needs only the US
symbol at t1 and t2, while the regression needs both symbols at t0 and t1, so a
day with a hole in one leg still contributes to the other. The per-statistic n
is reported alongside each number.

Note on beta: the issue text fits `r_US_conc ~ r_KR` for R^2/beta but then
writes `eps = r_KR - beta * r_US_conc`, which is the residual of the *other*
regression. R^2 is identical either way; only the slope differs. Both slopes and
both residual dispersions are reported, and a K0-b kill is only called clear-cut
when the two conventions agree.
"""

from __future__ import annotations

import argparse
import json
import math
from typing import Optional, Sequence

BPS = 1e-4
KILL_SD_BPS = 14.0
KILL_R2 = 0.8
POINTS = ("t0", "t1", "t2")
CI_ALPHA = 0.10  # two-sided 90% interval


def mean(values: Sequence[float]) -> float:
    return sum(values) / len(values)


def stdev(values: Sequence[float], ddof: int = 1, centred: bool = True) -> Optional[float]:
    """Standard deviation with an explicit degrees-of-freedom loss.

    Regression residuals lose one df per fitted parameter, so their dispersion
    must be divided by n-2, not n-1, and they are already centred by the fit, so
    their mean is not re-estimated.
    """
    n = len(values)
    if n - ddof < 1:
        return None
    mu = mean(values) if centred else 0.0
    return math.sqrt(sum((v - mu) ** 2 for v in values) / (n - ddof))


def _lower_gamma_regularised(a: float, x: float) -> float:
    """P(a, x): regularised lower incomplete gamma, series/continued fraction."""
    if x <= 0.0:
        return 0.0
    log_gamma_a = math.lgamma(a)
    if x < a + 1.0:
        term = 1.0 / a
        total = term
        n = a
        for _ in range(1000):
            n += 1.0
            term *= x / n
            total += term
            if abs(term) < abs(total) * 1e-14:
                break
        return total * math.exp(-x + a * math.log(x) - log_gamma_a)
    # Continued fraction for Q(a, x) = 1 - P(a, x).
    tiny = 1e-300
    b = x + 1.0 - a
    c = 1.0 / tiny
    d = 1.0 / b
    h = d
    for i in range(1, 1000):
        an = -i * (i - a)
        b += 2.0
        d = an * d + b
        if abs(d) < tiny:
            d = tiny
        c = b + an / c
        if abs(c) < tiny:
            c = tiny
        d = 1.0 / d
        delta = d * c
        h *= delta
        if abs(delta - 1.0) < 1e-14:
            break
    q = math.exp(-x + a * math.log(x) - log_gamma_a) * h
    return 1.0 - q


def chi2_quantile(p: float, df: int) -> float:
    """Inverse chi-square CDF by bisection (df is tiny here, precision is ample)."""
    if not 0.0 < p < 1.0 or df < 1:
        raise ValueError("chi2_quantile needs 0<p<1 and df>=1")
    lo, hi = 0.0, max(10.0, float(df) * 4.0)
    while _lower_gamma_regularised(df / 2.0, hi / 2.0) < p and hi < 1e9:
        hi *= 2.0
    for _ in range(200):
        mid = 0.5 * (lo + hi)
        if _lower_gamma_regularised(df / 2.0, mid / 2.0) < p:
            lo = mid
        else:
            hi = mid
    return 0.5 * (lo + hi)


def sd_ci(sd: Optional[float], df: int, alpha: float = CI_ALPHA) -> Optional[tuple]:
    """Exact normal-theory confidence interval for a standard deviation.

    (df * sd^2) / sigma^2 ~ chi^2(df), so the interval is sd rescaled by
    sqrt(df / chi2 quantiles). Unlike a bootstrap it stays informative at n=3,
    where resampling degenerates to a lower bound of zero.
    """
    if sd is None or df < 1:
        return None
    hi_q = chi2_quantile(1.0 - alpha / 2.0, df)
    lo_q = chi2_quantile(alpha / 2.0, df)
    return sd * math.sqrt(df / hi_q), sd * math.sqrt(df / lo_q)


def ols(y: Sequence[float], x: Sequence[float]) -> Optional[dict]:
    """Least squares y = alpha + beta * x, with R^2. None when x has no spread."""
    if len(y) != len(x) or len(y) < 3:
        return None
    mx, my = mean(x), mean(y)
    sxx = sum((v - mx) ** 2 for v in x)
    if sxx <= 0.0:
        return None
    sxy = sum((a - mx) * (b - my) for a, b in zip(x, y))
    beta = sxy / sxx
    alpha = my - beta * mx
    syy = sum((v - my) ** 2 for v in y)
    resid = [b - (alpha + beta * a) for a, b in zip(x, y)]
    r2 = 1.0 - (sum(r * r for r in resid) / syy) if syy > 0 else None
    return {"alpha": alpha, "beta": beta, "r2": r2, "resid": resid}


def correlation(a: Sequence[float], b: Sequence[float]) -> Optional[float]:
    if len(a) != len(b) or len(a) < 3:
        return None
    ma, mb = mean(a), mean(b)
    saa = sum((v - ma) ** 2 for v in a)
    sbb = sum((v - mb) ** 2 for v in b)
    if saa <= 0 or sbb <= 0:
        return None
    return sum((x - ma) * (y - mb) for x, y in zip(a, b)) / math.sqrt(saa * sbb)


def load_prices(path: str) -> dict:
    """{(date, point, symbol, price_type): row} from the extractor's JSONL."""
    prices = {}
    with open(path) as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            if row.get("status") != "ok" or "price" not in row:
                continue
            key = (row["date"], row["point"], row["symbol"], row["price_type"])
            prices[key] = row
    return prices


def build_sessions(
    prices: dict, kr_symbol: str, us_symbol: str, price_type: str, max_lag_secs: float
) -> tuple:
    """Per-day returns (each may be None) plus why a leg was unusable."""
    dates = sorted({key[0] for key in prices})
    sessions, notes = [], []

    def usable(date, symbol, point):
        row = prices.get((date, point, symbol, price_type))
        if row is None:
            return None, "%s@%s missing" % (symbol, point)
        if abs(row["lag_secs"]) > max_lag_secs:
            return None, "%s@%s stale(%+.0fs)" % (symbol, point, row["lag_secs"])
        price = float(row["price"])
        if price <= 0:
            return None, "%s@%s non_positive" % (symbol, point)
        return price, None

    for date in dates:
        found, why = {}, []
        for symbol in (kr_symbol, us_symbol):
            for point in POINTS:
                price, reason = usable(date, symbol, point)
                if reason is not None:
                    why.append(reason)
                else:
                    found[(symbol, point)] = price

        def ret(symbol, first, last):
            a = found.get((symbol, first))
            b = found.get((symbol, last))
            return math.log(b / a) if a and b else None

        session = {
            "date": date,
            "r_kr": ret(kr_symbol, "t0", "t1"),
            "r_us_conc": ret(us_symbol, "t0", "t1"),
            "fwd": ret(us_symbol, "t1", "t2"),
        }
        if session["r_kr"] is None and session["r_us_conc"] is None and session["fwd"] is None:
            notes.append({"date": date, "usable": "none", "why": why})
            continue
        if why:
            notes.append({"date": date, "usable": "partial", "why": why})
        sessions.append(session)
    return sessions, notes


def analyse(sessions: Sequence[dict]) -> dict:
    fwd_days = [s for s in sessions if s["fwd"] is not None]
    reg_days = [
        s for s in sessions if s["r_kr"] is not None and s["r_us_conc"] is not None
    ]
    fwd = [s["fwd"] for s in fwd_days]
    r_kr = [s["r_kr"] for s in reg_days]
    r_us = [s["r_us_conc"] for s in reg_days]

    out = {
        "n_fwd": len(fwd_days),
        "n_regression": len(reg_days),
        "fwd_dates": [s["date"] for s in fwd_days],
        "regression_dates": [s["date"] for s in reg_days],
        "sd_fwd_bps": None,
        "sd_fwd_bps_ci90": None,
        "mean_fwd_bps": mean(fwd) / BPS if fwd else None,
        "sd_r_kr_bps": None,
        "sd_r_us_conc_bps": None,
        "r2": None,
        "beta_kr_on_us": None,
        "beta_us_on_kr": None,
        "sd_eps_bps": None,
        "sd_eps_bps_ci90": None,
        "sd_eps_issue_beta_bps": None,
        "corr_eps_fwd": None,
        "n_corr": 0,
        "corr_r_kr_r_us": correlation(r_kr, r_us),
        "fwd_bps": [f / BPS for f in fwd],
        "r_kr_bps": [r / BPS for r in r_kr],
        "r_us_conc_bps": [r / BPS for r in r_us],
    }

    sd_fwd = stdev(fwd)
    if sd_fwd is not None:
        out["sd_fwd_bps"] = sd_fwd / BPS
        ci = sd_ci(sd_fwd, len(fwd) - 1)
        out["sd_fwd_bps_ci90"] = [c / BPS for c in ci] if ci else None
    sd_kr = stdev(r_kr)
    sd_us = stdev(r_us)
    out["sd_r_kr_bps"] = sd_kr / BPS if sd_kr is not None else None
    out["sd_r_us_conc_bps"] = sd_us / BPS if sd_us is not None else None

    kr_on_us = ols(r_kr, r_us)  # residual = the eps the strategy would trade
    us_on_kr = ols(r_us, r_kr)  # slope = absorption, as the issue words it
    if kr_on_us is not None:
        eps = kr_on_us["resid"]
        df = len(eps) - 2  # two fitted parameters
        out["r2"] = kr_on_us["r2"]
        out["beta_kr_on_us"] = kr_on_us["beta"]
        sd_eps = stdev(eps, ddof=2, centred=False)
        out["sd_eps_bps"] = sd_eps / BPS if sd_eps is not None else None
        ci_eps = sd_ci(sd_eps, df)
        out["sd_eps_bps_ci90"] = [c / BPS for c in ci_eps] if ci_eps else None
        out["eps_bps"] = [e / BPS for e in eps]
        paired = [
            (e, s["fwd"])
            for e, s in zip(eps, reg_days)
            if s["fwd"] is not None
        ]
        out["n_corr"] = len(paired)
        if paired:
            out["corr_eps_fwd"] = correlation(
                [p[0] for p in paired], [p[1] for p in paired]
            )
    if us_on_kr is not None:
        out["beta_us_on_kr"] = us_on_kr["beta"]
        # The literal reading of the issue's formula: eps with the other slope.
        eps_issue = [
            s["r_kr"] - us_on_kr["beta"] * s["r_us_conc"] for s in reg_days
        ]
        sd_issue = stdev(eps_issue, ddof=2)
        out["sd_eps_issue_beta_bps"] = sd_issue / BPS if sd_issue is not None else None
    return out


def verdict(stats: dict) -> dict:
    """Apply the frozen kill rules, and say whether the sample can carry them."""
    sd_fwd = stats.get("sd_fwd_bps")
    sd_eps = stats.get("sd_eps_bps")
    sd_eps_alt = stats.get("sd_eps_issue_beta_bps")
    r2 = stats.get("r2")

    k0a = sd_fwd is not None and sd_fwd < KILL_SD_BPS
    k0b_primary = (
        r2 is not None and sd_eps is not None and r2 >= KILL_R2 and sd_eps < KILL_SD_BPS
    )
    k0b_alt = (
        r2 is not None
        and sd_eps_alt is not None
        and r2 >= KILL_R2
        and sd_eps_alt < KILL_SD_BPS
    )
    killed = k0a or k0b_primary or k0b_alt

    reasons = []
    if k0a:
        reasons.append("K0-a: sd(fwd)=%.1f bps < %.0f bps" % (sd_fwd, KILL_SD_BPS))
    if k0b_primary or k0b_alt:
        reasons.append(
            "K0-b: R^2=%.3f >= %.1f and sd(eps)=%.1f bps < %.0f bps"
            % (r2, KILL_R2, sd_eps if k0b_primary else sd_eps_alt, KILL_SD_BPS)
        )

    # A "no kill" is only meaningful when the uncertainty band clears the
    # threshold too: at n=3 a point estimate alone proves nothing.
    ci_fwd = stats.get("sd_fwd_bps_ci90")
    ci_eps = stats.get("sd_eps_bps_ci90")
    k0a_ci_clear = bool(ci_fwd) and ci_fwd[0] >= KILL_SD_BPS
    k0b_ci_clear = (r2 is not None and r2 < KILL_R2) or (
        bool(ci_eps) and ci_eps[0] >= KILL_SD_BPS
    )
    return {
        "killed": killed,
        "k0a": k0a,
        "k0b": k0b_primary or k0b_alt,
        "k0b_beta_variants_agree": k0b_primary == k0b_alt,
        "k0a_clear_of_threshold_at_ci_lower": k0a_ci_clear,
        "k0b_clear_of_threshold_at_ci_lower": k0b_ci_clear,
        "reasons": reasons,
        "decision": "KILL" if killed else "PROCEED to Step 1 (#989)",
    }


def fmt(value, digits=1, suffix=""):
    if value is None:
        return "n/a"
    return ("%%.%df%%s" % digits) % (value, suffix)


def fmt_ci(ci):
    if not ci:
        return ""
    return "  (90%% CI %.1f-%.1f)" % (ci[0], ci[1])


def report(label: str, stats: dict, decision: dict, notes: Sequence[dict]) -> str:
    lines = ["=== %s ===" % label]
    lines.append(
        "n(fwd) = %d [%s]" % (stats["n_fwd"], ", ".join(stats["fwd_dates"]))
    )
    lines.append(
        "n(regression) = %d [%s]"
        % (stats["n_regression"], ", ".join(stats["regression_dates"]))
    )
    for note in notes:
        lines.append("  %s (%s): %s" % (note["date"], note["usable"], "; ".join(note["why"])))
    lines.append(
        "sd(fwd)  = %s bps%s   [K0-a kills below %.0f]"
        % (fmt(stats["sd_fwd_bps"]), fmt_ci(stats["sd_fwd_bps_ci90"]), KILL_SD_BPS)
    )
    lines.append(
        "sd(eps)  = %s bps%s   [K0-b kills below %.0f, and only when R^2 >= %.1f]"
        % (
            fmt(stats["sd_eps_bps"]),
            fmt_ci(stats["sd_eps_bps_ci90"]),
            KILL_SD_BPS,
            KILL_R2,
        )
    )
    lines.append(
        "sd(eps) under the issue's literal beta = %s bps"
        % fmt(stats["sd_eps_issue_beta_bps"])
    )
    lines.append(
        "R^2 = %s   (beta r_KR~r_US = %s, beta r_US~r_KR = %s)"
        % (
            fmt(stats["r2"], 3),
            fmt(stats["beta_kr_on_us"], 3),
            fmt(stats["beta_us_on_kr"], 3),
        )
    )
    lines.append(
        "sd(r_KR) = %s bps, sd(r_US_conc) = %s bps, mean(fwd) = %s bps"
        % (
            fmt(stats["sd_r_kr_bps"]),
            fmt(stats["sd_r_us_conc_bps"]),
            fmt(stats["mean_fwd_bps"]),
        )
    )
    lines.append(
        "corr(eps, fwd) = %s over n=%d   (reference only, not a decision input)"
        % (fmt(stats["corr_eps_fwd"], 3), stats["n_corr"])
    )
    lines.append("decision: %s" % decision["decision"])
    for reason in decision["reasons"]:
        lines.append("  %s" % reason)
    if not decision["killed"]:
        if decision["k0a_clear_of_threshold_at_ci_lower"]:
            lines.append("  K0-a: even the CI lower bound clears the threshold")
        else:
            lines.append("  K0-a: NOT clear at the CI lower bound -- underpowered, needs more sessions")
        if decision["k0b_clear_of_threshold_at_ci_lower"]:
            lines.append("  K0-b: clear (R^2 below the gate, or the eps CI clears it)")
        else:
            lines.append("  K0-b: NOT clear at the CI lower bound -- underpowered, needs more sessions")
    if not decision["k0b_beta_variants_agree"]:
        lines.append("  NOTE: the two beta conventions disagree on K0-b; treat as unresolved")
    return "\n".join(lines)


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prices", required=True, help="JSONL from engine_b_step0_extract.py")
    parser.add_argument("--kr-symbol", default="SKHYNIXUSD")
    parser.add_argument("--us-symbol", default="SNDK")
    parser.add_argument(
        "--price-types",
        default="mid,mark,index",
        help="run the whole test once per price type (bot-strategy#873 robustness)",
    )
    parser.add_argument(
        "--max-lag-secs",
        type=float,
        default=120.0,
        help="drop a quote sitting further than this from the instant it stands for",
    )
    parser.add_argument("--json-out")
    args = parser.parse_args(argv)

    prices = load_prices(args.prices)
    results = {}
    chunks = []
    for price_type in [p for p in args.price_types.split(",") if p]:
        sessions, notes = build_sessions(
            prices, args.kr_symbol, args.us_symbol, price_type, args.max_lag_secs
        )
        label = "%s vs %s, price_type=%s" % (args.kr_symbol, args.us_symbol, price_type)
        if not sessions:
            chunks.append("=== %s ===\nno usable session (%d day(s) had no leg)" % (label, len(notes)))
            results[price_type] = {"n_fwd": 0, "n_regression": 0, "notes": notes}
            continue
        stats = analyse(sessions)
        decision = verdict(stats)
        chunks.append(report(label, stats, decision, notes))
        results[price_type] = {
            "stats": stats,
            "decision": decision,
            "notes": notes,
            "sessions": sessions,
        }

    print("\n\n".join(chunks))
    if args.json_out:
        with open(args.json_out, "w") as handle:
            json.dump(
                {
                    "kr_symbol": args.kr_symbol,
                    "us_symbol": args.us_symbol,
                    "max_lag_secs": args.max_lag_secs,
                    "kill_thresholds": {"sd_bps": KILL_SD_BPS, "r2": KILL_R2},
                    "by_price_type": results,
                },
                handle,
                indent=2,
                sort_keys=True,
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
