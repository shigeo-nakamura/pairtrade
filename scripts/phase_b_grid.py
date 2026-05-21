#!/usr/bin/env python3
"""
phase_b_grid.py — Phase B grid sweep runner (bot-strategy#70).

Enumerates the parameter grid from #70 (entry_z_base × force_close_time_secs ×
exit_z_score × stop_loss_z_score = 100 cells by default), runs one byte-exact
BT per cell, and aggregates results into a sortable markdown table.

Designed to be re-runnable: keeps per-cell BT logs under
`/tmp/phase_b_grid/<cell-id>/` so a partial run can resume without recomputing.

Usage
-----
    # Default 100-cell grid on R3+R4 combined data:
    scripts/phase_b_grid.py \\
        --data-dir /tmp/bt_rehedge_p2_extended/data \\
        --events-dir /tmp/bt_rehedge_p2_extended/events \\
        --snapshot /tmp/bt_r3r4/pairtrade_history_BTC_ETH.json \\
        --out /tmp/phase_b_grid

    # Smaller smoke sweep:
    scripts/phase_b_grid.py --entry-z 2.0,2.5 --fc 7200,10800 --exit-z 0.2 --sl-z 4.0 ...

After the run completes, prints the top-N cells by Calmar at 5 bp fee.

The cell-template YAML is built from the production debot-pair-btceth.yaml
with the four sweep dimensions overridden at top level. Per-strategy
overrides are removed so all variants share the swept config (gives the
cleanest comparison).
"""
from __future__ import annotations

import argparse
import itertools
import json
import multiprocessing
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
PROD_YAML = REPO / "configs/pairtrade/debot-pair-btceth.yaml"
BINARY = REPO / "target/release/debot"


@dataclass(frozen=True)
class Cell:
    entry_z: float
    fc: int
    exit_z: float
    sl_z: float

    @property
    def slug(self) -> str:
        return f"ez{self.entry_z}_fc{self.fc}_xz{self.exit_z}_slz{self.sl_z}".replace(".", "p")


def make_yaml(template: str, cell: Cell) -> str:
    """Override the four primary sweep knobs on the top-level + strip per-strategy
    fc overrides so the cell value applies uniformly across A/B/C."""
    out = template
    out = re.sub(r"^entry_z_score_base:\s*[0-9.]+", f"entry_z_score_base: {cell.entry_z}",
                 out, count=1, flags=re.MULTILINE)
    out = re.sub(r"^entry_z_score_min:\s*[0-9.]+",
                 f"entry_z_score_min: {max(1.0, cell.entry_z - 0.5):.1f}",
                 out, count=1, flags=re.MULTILINE)
    out = re.sub(r"^entry_z_score_max:\s*[0-9.]+",
                 f"entry_z_score_max: {cell.entry_z + 0.5:.1f}",
                 out, count=1, flags=re.MULTILINE)
    out = re.sub(r"^exit_z_score:\s*[0-9.]+", f"exit_z_score: {cell.exit_z}",
                 out, count=1, flags=re.MULTILINE)
    out = re.sub(r"^stop_loss_z_score:\s*[0-9.]+", f"stop_loss_z_score: {cell.sl_z}",
                 out, count=1, flags=re.MULTILINE)
    out = re.sub(r"^force_close_time_secs:\s*\d+", f"force_close_time_secs: {cell.fc}",
                 out, count=1, flags=re.MULTILINE)
    # Per-strategy override: rewrite all instances inside the strategies block
    out = re.sub(r"^    force_close_time_secs:\s*\d+",
                 f"    force_close_time_secs: {cell.fc}",
                 out, flags=re.MULTILINE)
    # shutdown_grace must be > fc + 60
    out = re.sub(r"^shutdown_grace_secs:.*$",
                 f"shutdown_grace_secs: {cell.fc + 60}",
                 out, flags=re.MULTILINE)
    return out


def run_one(cell: Cell, out_root: Path, data_dir: Path, events_dir: Path,
            snapshot: Path) -> tuple[Cell, dict]:
    """Run one BT cell. Reuses existing live.bin/combined.jsonl if present."""
    work = out_root / cell.slug
    work.mkdir(parents=True, exist_ok=True)
    # cache: shared live.bin across cells (data is the same)
    shared_bin = out_root / "shared" / "live.bin"
    if not shared_bin.exists():
        (out_root / "shared").mkdir(parents=True, exist_ok=True)
        combined = out_root / "shared/combined.jsonl"
        if not combined.exists():
            with open(combined, "wb") as f:
                for jsonl in sorted(data_dir.glob("market_data_btceth_*.jsonl")):
                    f.write(jsonl.read_bytes())
        subprocess.run(
            ["cargo", "run", "--release", "--bin", "convert-data", "--",
             str(combined), str(shared_bin), "0"],
            cwd=REPO, check=True, capture_output=True,
        )

    yaml_path = work / "cell.yaml"
    yaml_path.write_text(make_yaml(PROD_YAML.read_text(), cell))

    bt_log = work / "bt.log"
    if bt_log.exists() and bt_log.stat().st_size > 0:
        # Resume — already ran
        pass
    else:
        env = {
            "PATH": "/usr/bin:/usr/local/bin",
            "HOME": str(Path.home()),
            # libsigner.so lives outside the standard linker paths
            "LD_LIBRARY_PATH": "/home/shigeo/bot/lighter-go",
            "BT_WARM_START_SNAPSHOT": str(snapshot),
            "BT_EVAL_TIMESTAMPS_FILE": str(events_dir / "eval_ts.txt"),
            "BT_RESTART_TIMESTAMPS_FILE": str(events_dir / "restart_ts.txt"),
            "BACKTEST_MODE": "true",
            "BACKTEST_FILE": str(shared_bin),
            "DRY_RUN": "true",
            "ENABLE_DATA_DUMP": "false",
            "RUST_LOG": "warn,debot::pairtrade=info,debot=info",
            "UNIVERSE_PAIRS": "BTC/ETH",
            "PAIRTRADE_CONFIG_PATH": str(yaml_path),
        }
        with open(bt_log, "wb") as f:
            subprocess.run([str(BINARY)], stdout=f, stderr=subprocess.STDOUT,
                           env=env, cwd=REPO)
    return cell, parse_bt_log(bt_log)


def parse_bt_log(log_path: Path) -> dict:
    """Compute PnL stats at 0bp + 5bp by streaming entry/exit lines."""
    # Lazy import: log_analyzer is in scripts/
    sys.path.insert(0, str(REPO / "scripts"))
    from log_analyzer import calculate_pnl, compute_max_drawdown, compute_sharpe
    out = {}
    # Pick a permissive warmup_end (= unix epoch start)
    warmup_end = datetime(2026, 1, 1, tzinfo=timezone.utc)
    for fee_label, fee_val in [("0bp", 0.0), ("5bp", 5.0)]:
        pnl, tp, _, hs = calculate_pnl(str(log_path), warmup_end, None, fee_val, 0.0)
        n = len(tp)
        if n == 0:
            out[fee_label] = {"n": 0, "pnl": 0.0, "win_pct": 0.0,
                              "sharpe": 0.0, "maxdd": 0.0, "calmar": 0.0,
                              "fc_pct": 0.0, "median_hold_h": 0.0}
            continue
        wins = sum(1 for p in tp if p > 0)
        dd = compute_max_drawdown(tp)
        sh = compute_sharpe(tp)
        cm = float(pnl) / dd if dd > 0 else 0.0
        # FC% — count holds that ended very close to the fc value
        # (the cell's fc is encoded in slug; for simplicity, count any hold
        # within 60s of an approximate "max" hold)
        max_h = max(hs) if hs else 0
        fc_hits = sum(1 for h in hs if max_h > 0 and abs(h - max_h) < 60)
        out[fee_label] = {
            "n": n,
            "pnl": float(pnl),
            "win_pct": 100 * wins / n,
            "sharpe": sh,
            "maxdd": dd,
            "calmar": cm,
            "fc_pct": 100 * fc_hits / n,
            "median_hold_h": (sorted(hs)[len(hs) // 2] / 3600) if hs else 0.0,
        }
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--data-dir", type=Path, required=True,
                        help="Directory of market_data_btceth_*.jsonl files")
    parser.add_argument("--events-dir", type=Path, required=True,
                        help="Directory holding eval_ts.txt + restart_ts.txt for byte-exact replay")
    parser.add_argument("--snapshot", type=Path, required=True,
                        help="Warm-start snapshot path")
    parser.add_argument("--out", type=Path, default=Path("/tmp/phase_b_grid"))
    parser.add_argument("--entry-z", default="1.5,2.0,2.5,3.0,3.5",
                        help="Comma-separated entry_z_base values")
    parser.add_argument("--fc", default="3600,5400,7200,10800,14400",
                        help="Comma-separated force_close_time_secs values")
    parser.add_argument("--exit-z", default="0.2,0.3",
                        help="Comma-separated exit_z_score values")
    parser.add_argument("--sl-z", default="4.0,6.0",
                        help="Comma-separated stop_loss_z_score values")
    parser.add_argument("--workers", type=int, default=max(1, multiprocessing.cpu_count() // 2),
                        help="Parallel BT processes (default: cpu_count/2)")
    parser.add_argument("--top-n", type=int, default=20, help="Top-N cells in summary")
    args = parser.parse_args()

    if not BINARY.exists():
        print(f"ERROR: binary not built ({BINARY}). Run `cargo build --release` first.",
              file=sys.stderr)
        return 1

    entry_zs = [float(x) for x in args.entry_z.split(",")]
    fcs = [int(x) for x in args.fc.split(",")]
    exit_zs = [float(x) for x in args.exit_z.split(",")]
    sl_zs = [float(x) for x in args.sl_z.split(",")]

    cells = [Cell(ez, fc, xz, sl) for ez, fc, xz, sl
             in itertools.product(entry_zs, fcs, exit_zs, sl_zs)]
    print(f"# Phase B grid sweep — {len(cells)} cells across "
          f"{args.workers} workers", file=sys.stderr)

    args.out.mkdir(parents=True, exist_ok=True)

    # Sequential first run to populate shared bin, then parallelize remaining cells
    with multiprocessing.Pool(args.workers) as pool:
        results = []
        # Pre-warm the bin (run cell 0 alone first to build shared/live.bin)
        results.append(run_one(cells[0], args.out, args.data_dir,
                               args.events_dir, args.snapshot))
        print(f"  cell 1/{len(cells)} done ({cells[0].slug})", file=sys.stderr)
        # Now parallelize the rest
        remaining = cells[1:]
        for i, r in enumerate(pool.imap_unordered(
                _worker, [(c, args.out, args.data_dir, args.events_dir, args.snapshot)
                          for c in remaining]), start=2):
            results.append(r)
            print(f"  cell {i}/{len(cells)} done ({r[0].slug})", file=sys.stderr)

    # Save CSV
    csv_path = args.out / "results.csv"
    with open(csv_path, "w") as f:
        f.write("entry_z,fc,exit_z,sl_z,fee,n,pnl,win_pct,sharpe,maxdd,calmar,fc_pct,median_hold_h\n")
        for cell, metrics in results:
            for fee_label in ["0bp", "5bp"]:
                m = metrics[fee_label]
                f.write(f"{cell.entry_z},{cell.fc},{cell.exit_z},{cell.sl_z},"
                        f"{fee_label},{m['n']},{m['pnl']:.4f},{m['win_pct']:.2f},"
                        f"{m['sharpe']:.4f},{m['maxdd']:.2f},{m['calmar']:.4f},"
                        f"{m['fc_pct']:.2f},{m['median_hold_h']:.3f}\n")
    print(f"# results saved to {csv_path}", file=sys.stderr)

    # Sort & top-N at 5bp by Calmar (= the primary objective for #70)
    def sort_key(item):
        cell, metrics = item
        m = metrics["5bp"]
        return -m["calmar"] if m["calmar"] > 0 else 1e9 - m["pnl"]  # losers sort by least-negative PnL

    top_5bp = sorted(results, key=sort_key)[:args.top_n]
    print("\n## Top cells by Calmar @ fee=5bp\n")
    print("| entry_z | fc | exit_z | sl_z | n | PnL$ | win% | Sharpe | MaxDD | Calmar | FC% |")
    print("|---|---|---|---|---|---|---|---|---|---|---|")
    for cell, metrics in top_5bp:
        m = metrics["5bp"]
        print(f"| {cell.entry_z} | {cell.fc} | {cell.exit_z} | {cell.sl_z} "
              f"| {m['n']} | {m['pnl']:+.2f} | {m['win_pct']:.0f}% "
              f"| {m['sharpe']:.3f} | {m['maxdd']:.2f} | {m['calmar']:.3f} | {m['fc_pct']:.0f}% |")

    # And by Calmar @ 0bp (Frankfurt's effective fee)
    top_0bp = sorted(results, key=lambda i: -i[1]["0bp"]["calmar"])[:args.top_n]
    print("\n## Top cells by Calmar @ fee=0bp (Frankfurt)\n")
    print("| entry_z | fc | exit_z | sl_z | n | PnL$ | win% | Sharpe | MaxDD | Calmar | FC% |")
    print("|---|---|---|---|---|---|---|---|---|---|---|")
    for cell, metrics in top_0bp:
        m = metrics["0bp"]
        print(f"| {cell.entry_z} | {cell.fc} | {cell.exit_z} | {cell.sl_z} "
              f"| {m['n']} | {m['pnl']:+.2f} | {m['win_pct']:.0f}% "
              f"| {m['sharpe']:.3f} | {m['maxdd']:.2f} | {m['calmar']:.3f} | {m['fc_pct']:.0f}% |")
    return 0


def _worker(args_tuple):
    return run_one(*args_tuple)


if __name__ == "__main__":
    sys.exit(main())
