#!/usr/bin/env python3
"""Byte-exact Round 3 grid for Tokyo Extended (bot-strategy#492)."""

from __future__ import annotations

import argparse
import csv
import itertools
import multiprocessing
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
PROD_YAML = REPO / "configs/pairtrade/debot-pair-btceth-extended.yaml"
BINARY = REPO / "target/release/debot"
CONVERT_BINARY = REPO / "target/release/convert-data"


@dataclass(frozen=True)
class Cell:
    entry_z: float
    fc_secs: int
    exit_timeout_secs: int

    @property
    def slug(self) -> str:
        entry = str(self.entry_z).replace(".", "p")
        return f"ez{entry}_fc{self.fc_secs}_xt{self.exit_timeout_secs}"


def replace_top_level(config: str, key: str, value: str) -> str:
    pattern = rf"^{re.escape(key)}:\s*.*$"
    updated, count = re.subn(pattern, f"{key}: {value}", config, count=1,
                             flags=re.MULTILINE)
    if count != 1:
        raise ValueError(f"missing top-level config key: {key}")
    return updated


def make_config(template: str, cell: Cell) -> str:
    config = replace_top_level(template, "entry_z_score_base", str(cell.entry_z))
    config = replace_top_level(
        config, "entry_z_score_min", f"{max(1.0, cell.entry_z - 0.5):.1f}"
    )
    config = replace_top_level(
        config, "entry_z_score_max", f"{cell.entry_z + 0.5:.1f}"
    )
    config = replace_top_level(config, "force_close_time_secs", str(cell.fc_secs))
    config = replace_top_level(config, "shutdown_grace_secs", str(cell.fc_secs + 60))
    return replace_top_level(
        config, "exit_post_only_timeout_secs", str(cell.exit_timeout_secs)
    )


def build_data(data_dir: Path, out_dir: Path) -> Path:
    files = sorted(data_dir.glob("market_data_btceth_extended_*.jsonl"))
    if not files:
        raise SystemExit(f"ERROR: no Extended dumps in {data_dir}")

    shared = out_dir / "shared"
    shared.mkdir(parents=True, exist_ok=True)
    combined = shared / "combined.jsonl"
    live_bin = shared / "live.bin"

    with combined.open("wb") as dst:
        for path in files:
            with path.open("rb") as src:
                while chunk := src.read(1024 * 1024):
                    dst.write(chunk)

    subprocess.run(
        [str(CONVERT_BINARY), str(combined), str(live_bin), "0"],
        cwd=REPO,
        check=True,
    )
    return live_bin


def parse_log(log_path: Path, cutoff: datetime, fc_secs: int) -> dict[str, float]:
    sys.path.insert(0, str(REPO / "scripts"))
    from log_analyzer import calculate_pnl, compute_max_drawdown, compute_sharpe

    pnl, trade_pnls, _, hold_secs = calculate_pnl(
        str(log_path), cutoff, None, 2.5, 0.0
    )
    count = len(trade_pnls)
    if count == 0:
        return {
            "trades": 0,
            "pnl": 0.0,
            "win_pct": 0.0,
            "sharpe": 0.0,
            "maxdd": 0.0,
            "calmar": 0.0,
            "fc_pct": 0.0,
            "median_hold_h": 0.0,
        }

    maxdd = compute_max_drawdown(trade_pnls)
    holds = sorted(hold_secs)
    fc_count = sum(abs(hold - fc_secs) <= 60 for hold in holds)
    return {
        "trades": count,
        "pnl": float(pnl),
        "win_pct": 100 * sum(value > 0 for value in trade_pnls) / count,
        "sharpe": compute_sharpe(trade_pnls),
        "maxdd": maxdd,
        "calmar": float(pnl) / maxdd if maxdd > 0 else 0.0,
        "fc_pct": 100 * fc_count / count,
        "median_hold_h": holds[count // 2] / 3600,
    }


def run_cell(
    args: tuple[Cell, Path, Path, Path, datetime, Path | None],
) -> tuple[Cell, dict]:
    cell, out_dir, live_bin, events_dir, cutoff, warm_start_snapshot = args
    work = out_dir / cell.slug
    work.mkdir(parents=True, exist_ok=True)
    config_path = work / "config.yaml"
    log_path = work / "bt.log"
    config_path.write_text(make_config(PROD_YAML.read_text(), cell))

    env = os.environ.copy()
    env.update(
        {
            "BACKTEST_MODE": "true",
            "BACKTEST_FILE": str(live_bin),
            "BT_EVAL_TIMESTAMPS_FILE": str(events_dir / "eval_ts.txt"),
            "BT_RESTART_TIMESTAMPS_FILE": str(events_dir / "restart_ts.txt"),
            "DRY_RUN": "true",
            "ENABLE_DATA_DUMP": "false",
            "PAIRTRADE_CONFIG_PATH": str(config_path),
            "RUST_LOG": "warn,debot::pairtrade=info",
            "UNIVERSE_PAIRS": "BTC/ETH",
        }
    )
    if warm_start_snapshot is None:
        env.pop("BT_WARM_START_SNAPSHOT", None)
    else:
        env["BT_WARM_START_SNAPSHOT"] = str(warm_start_snapshot)
    with log_path.open("wb") as log:
        subprocess.run(
            [str(BINARY)],
            cwd=REPO,
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
            check=False,
        )
    return cell, parse_log(log_path, cutoff, cell.fc_secs)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--events-dir", type=Path, required=True)
    parser.add_argument("--cutoff", required=True, help="UTC P&L cutoff")
    parser.add_argument("--out", type=Path, default=Path("/tmp/round3_grid_extended"))
    parser.add_argument("--entry-z", default="1.8,2.0,2.2")
    parser.add_argument("--fc", default="7200,10800,14400")
    parser.add_argument("--exit-timeout", default="15,30,60")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--min-event-days", type=float, default=7.0)
    parser.add_argument(
        "--warm-start-snapshot",
        type=Path,
        help="v2 snapshot used to align initial beta/history with live state",
    )
    args = parser.parse_args()

    for path in (PROD_YAML, BINARY, CONVERT_BINARY):
        if not path.exists():
            raise SystemExit(f"ERROR: missing {path}")
    for name in ("eval_ts.txt", "restart_ts.txt"):
        path = args.events_dir / name
        if not path.exists() or (name == "eval_ts.txt" and path.stat().st_size == 0):
            raise SystemExit(f"ERROR: missing or empty byte-exact input {path}")
    if args.warm_start_snapshot is not None and not args.warm_start_snapshot.exists():
        raise SystemExit(
            f"ERROR: missing warm-start snapshot {args.warm_start_snapshot}"
        )

    cutoff = datetime.fromisoformat(args.cutoff.replace("Z", "+00:00"))
    if cutoff.tzinfo is None:
        cutoff = cutoff.replace(tzinfo=timezone.utc)

    eval_timestamps = [
        int(value) for value in (args.events_dir / "eval_ts.txt").read_text().split()
    ]
    event_start = datetime.fromtimestamp(min(eval_timestamps), timezone.utc)
    event_end = datetime.fromtimestamp(max(eval_timestamps), timezone.utc)
    event_days = (event_end - event_start).total_seconds() / 86400
    print(
        f"Byte-exact event coverage: {event_start.isoformat()} -> "
        f"{event_end.isoformat()} ({event_days:.2f} days, "
        f"{len(eval_timestamps)} evals)",
        file=sys.stderr,
    )
    if event_days < args.min_event_days:
        print(
            f"WARNING: event coverage is shorter than {args.min_event_days:g} days; "
            "treat this as a smoke readout, not a parameter winner.",
            file=sys.stderr,
        )
    if args.warm_start_snapshot is None:
        print(
            "WARNING: no warm-start snapshot supplied; initial beta/history will "
            "be rebuilt from the dump and early-window z-scores may differ from live",
            file=sys.stderr,
        )

    cells = [
        Cell(entry_z, fc_secs, exit_timeout)
        for entry_z, fc_secs, exit_timeout in itertools.product(
            (float(v) for v in args.entry_z.split(",")),
            (int(v) for v in args.fc.split(",")),
            (int(v) for v in args.exit_timeout.split(",")),
        )
    ]
    args.out.mkdir(parents=True, exist_ok=True)
    live_bin = build_data(args.data_dir, args.out)

    jobs = [
        (
            cell,
            args.out,
            live_bin,
            args.events_dir,
            cutoff,
            args.warm_start_snapshot,
        )
        for cell in cells
    ]
    with multiprocessing.Pool(args.workers) as pool:
        results = list(pool.imap_unordered(run_cell, jobs))

    results.sort(key=lambda item: (item[0].entry_z, item[0].fc_secs,
                                   item[0].exit_timeout_secs))
    csv_path = args.out / "results.csv"
    with csv_path.open("w", newline="") as output:
        writer = csv.writer(output)
        writer.writerow(
            ["entry_z", "fc_secs", "exit_timeout_secs", "trades", "pnl_25bp",
             "win_pct", "sharpe", "maxdd", "calmar", "fc_pct", "median_hold_h"]
        )
        for cell, metrics in results:
            writer.writerow(
                [cell.entry_z, cell.fc_secs, cell.exit_timeout_secs]
                + [metrics[key] for key in (
                    "trades", "pnl", "win_pct", "sharpe", "maxdd", "calmar",
                    "fc_pct", "median_hold_h"
                )]
            )

    profitable = [item for item in results if item[1]["pnl"] > 0]
    if profitable:
        ranked = sorted(
            results,
            key=lambda item: (item[1]["calmar"], item[1]["pnl"]),
            reverse=True,
        )
    else:
        ranked = sorted(results, key=lambda item: item[1]["pnl"], reverse=True)
    print("| entry_z | fc | exit timeout | trades | PnL @2.5bp | MaxDD | Calmar |")
    print("|---:|---:|---:|---:|---:|---:|---:|")
    for cell, metrics in ranked[:10]:
        print(
            f"| {cell.entry_z:.1f} | {cell.fc_secs} | {cell.exit_timeout_secs} "
            f"| {metrics['trades']:.0f} | ${metrics['pnl']:+.4f} "
            f"| ${metrics['maxdd']:.4f} | {metrics['calmar']:.3f} |"
        )
    print(f"\nResults: {csv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
