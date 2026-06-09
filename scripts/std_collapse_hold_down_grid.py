#!/usr/bin/env python3
"""
Grid runner for bot-strategy#500 std-collapse hold-down validation.

Runs byte-exact BT cells over:

- STD_COLLAPSE_MIN_RATIO
- STD_COLLAPSE_HOLD_DOWN_SECS

The script compares each candidate against a baseline cell and reports PnL,
drawdown, reject counts, and baseline trades that disappeared under the
candidate. It is intentionally entry-gate only: no live config is touched.
"""
from __future__ import annotations

import argparse
import csv
import itertools
import multiprocessing
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
BINARY = REPO / "target/release/debot"
CONVERTER = REPO / "target/release/convert-data"
PROD_YAML = REPO / "configs/pairtrade/debot-pair-btceth.yaml"


@dataclass(frozen=True)
class Cell:
    ratio: float
    hold_down_secs: int

    @property
    def slug(self) -> str:
        return f"r{self.ratio:.2f}_hd{self.hold_down_secs}".replace(".", "p")


@dataclass
class Trade:
    entry_ts: int
    exit_ts: int
    direction: str
    pnl: float


def parse_float_list(raw: str) -> list[float]:
    return [float(x) for x in raw.split(",") if x.strip()]


def parse_int_list(raw: str) -> list[int]:
    return [int(x) for x in raw.split(",") if x.strip()]


def make_yaml(template: str, cell: Cell) -> str:
    out = re.sub(
        r"^std_collapse_min_ratio:\s*[0-9.]+",
        f"std_collapse_min_ratio: {cell.ratio}",
        template,
        count=1,
        flags=re.MULTILINE,
    )
    if re.search(r"^std_collapse_hold_down_secs:", out, flags=re.MULTILINE):
        out = re.sub(
            r"^std_collapse_hold_down_secs:\s*\d+",
            f"std_collapse_hold_down_secs: {cell.hold_down_secs}",
            out,
            count=1,
            flags=re.MULTILINE,
        )
    else:
        out = re.sub(
            r"^(std_collapse_min_ratio:\s*[0-9.]+)$",
            rf"\1\nstd_collapse_hold_down_secs: {cell.hold_down_secs}",
            out,
            count=1,
            flags=re.MULTILINE,
        )
    return out


def ensure_shared_bin(out_root: Path, data_dir: Path) -> Path:
    shared = out_root / "shared"
    shared.mkdir(parents=True, exist_ok=True)
    live_bin = shared / "live.bin"
    if live_bin.exists() and live_bin.stat().st_size > 0:
        return live_bin

    combined = shared / "combined.jsonl"
    with open(combined, "wb") as fh:
        files = sorted(data_dir.glob("market_data_btceth_*.jsonl"))
        if not files:
            raise RuntimeError(f"no market_data_btceth_*.jsonl under {data_dir}")
        for path in files:
            fh.write(path.read_bytes())

    if CONVERTER.exists():
        cmd = [str(CONVERTER), str(combined), str(live_bin), "0"]
    else:
        cmd = [
            "cargo",
            "run",
            "--release",
            "--bin",
            "convert-data",
            "--",
            str(combined),
            str(live_bin),
            "0",
        ]
    subprocess.run(cmd, cwd=REPO, check=True)
    return live_bin


def parse_rejects(log_path: Path) -> dict[str, int]:
    totals: dict[str, int] = {}
    pat = re.compile(r"\b([a-zA-Z0-9_]+)=(\d+)")
    for line in log_path.read_text(errors="replace").splitlines():
        if "[ENTRY_REJECT_SUMMARY]" not in line:
            continue
        for key, value in pat.findall(line):
            if key in {"total"}:
                continue
            totals[key] = totals.get(key, 0) + int(value)
    return totals


def extract_trades(log_path: Path) -> list[Trade]:
    sys.path.insert(0, str(REPO / "scripts"))
    from log_analyzer import parse_log_line

    open_by_pair: dict[str, dict] = {}
    trades: list[Trade] = []
    seen_entry_keys: set[tuple] = set()
    seen_exit_keys: set[tuple] = set()

    with open(log_path, "r", errors="replace") as fh:
        for line in fh:
            data = parse_log_line(line)
            if not data:
                continue
            pair = data["pair"]
            ts = int(data["timestamp"].timestamp())
            key = (
                data["type"],
                pair,
                ts,
                data["direction"],
                str(data["size_a"]),
                str(data["price_a"]),
                str(data["size_b"]),
                str(data["price_b"]),
            )
            if data["type"] == "ENTRY":
                if key in seen_entry_keys:
                    continue
                seen_entry_keys.add(key)
                open_by_pair[pair] = data
            elif data["type"] == "EXIT":
                if key in seen_exit_keys:
                    continue
                seen_exit_keys.add(key)
                entry = open_by_pair.pop(pair, None)
                if not entry:
                    continue
                pnl = data.get("pnl")
                if pnl is None:
                    pnl = calc_pnl(entry, data)
                trades.append(
                    Trade(
                        entry_ts=int(entry["timestamp"].timestamp()),
                        exit_ts=ts,
                        direction=data["direction"],
                        pnl=float(pnl),
                    )
                )
    return trades


def calc_pnl(entry: dict, exit_: dict) -> Decimal:
    if entry["direction"] == "LongSpread":
        pnl_a = (exit_["price_a"] - entry["price_a"]) * entry["size_a"]
        pnl_b = (entry["price_b"] - exit_["price_b"]) * entry["size_b"]
    else:
        pnl_a = (entry["price_a"] - exit_["price_a"]) * entry["size_a"]
        pnl_b = (exit_["price_b"] - entry["price_b"]) * entry["size_b"]
    return pnl_a + pnl_b


def metrics(log_path: Path, fee_bps: float) -> dict:
    sys.path.insert(0, str(REPO / "scripts"))
    from log_analyzer import calculate_pnl, compute_max_drawdown, compute_sharpe

    warmup_start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    pnl, trade_pnls, _, _ = calculate_pnl(str(log_path), warmup_start, None, fee_bps, 0.0)
    n = len(trade_pnls)
    dd = compute_max_drawdown(trade_pnls)
    return {
        "pnl": float(pnl),
        "n": n,
        "wins": sum(1 for x in trade_pnls if x > 0),
        "sharpe": compute_sharpe(trade_pnls),
        "maxdd": dd,
        "calmar": float(pnl) / dd if dd > 0 else 0.0,
    }


def run_cell(args_tuple) -> tuple[Cell, dict]:
    cell, out_root, live_bin, events_dir, snapshot, template = args_tuple
    work = out_root / cell.slug
    work.mkdir(parents=True, exist_ok=True)
    yaml_path = work / "cell.yaml"
    yaml_path.write_text(make_yaml(template, cell))
    log_path = work / "bt.log"

    if not log_path.exists() or log_path.stat().st_size == 0:
        env = {
            "PATH": "/usr/bin:/usr/local/bin",
            "HOME": str(Path.home()),
            "LD_LIBRARY_PATH": "/home/shigeo/bot/lighter-go",
            "BT_WARM_START_SNAPSHOT": str(snapshot),
            "BT_EVAL_TIMESTAMPS_FILE": str(events_dir / "eval_ts.txt"),
            "BT_RESTART_TIMESTAMPS_FILE": str(events_dir / "restart_ts.txt"),
            "BACKTEST_MODE": "true",
            "BACKTEST_FILE": str(live_bin),
            "DRY_RUN": "true",
            "ENABLE_DATA_DUMP": "false",
            "RUST_LOG": "warn,debot::pairtrade=info,debot=info",
            "UNIVERSE_PAIRS": "BTC/ETH",
            "PAIRTRADE_CONFIG_PATH": str(yaml_path),
        }
        with open(log_path, "wb") as fh:
            subprocess.run([str(BINARY)], cwd=REPO, env=env, stdout=fh, stderr=subprocess.STDOUT)

    return cell, {
        "log": str(log_path),
        "0bp": metrics(log_path, 0.0),
        "5bp": metrics(log_path, 5.0),
        "rejects": parse_rejects(log_path),
        "trades": extract_trades(log_path),
    }


def compare_to_baseline(baseline: list[Trade], candidate: list[Trade]) -> dict:
    cand_entries = {t.entry_ts for t in candidate}
    blocked = [t for t in baseline if t.entry_ts not in cand_entries]
    blocked_losses = [t for t in blocked if t.pnl < 0]
    killed_wins = [t for t in blocked if t.pnl > 0]
    base_losses = [t for t in baseline if t.pnl < 0]
    base_wins = [t for t in baseline if t.pnl > 0]
    return {
        "blocked_trades": len(blocked),
        "blocked_loss": len(blocked_losses),
        "blocked_loss_pnl": sum(t.pnl for t in blocked_losses),
        "natural_win_kill": len(killed_wins),
        "natural_win_kill_pnl": sum(t.pnl for t in killed_wins),
        "blocked_loss_recall": len(blocked_losses) / len(base_losses) if base_losses else 0.0,
        "natural_win_kill_rate": len(killed_wins) / len(base_wins) if base_wins else 0.0,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data-dir", type=Path, required=True)
    ap.add_argument("--events-dir", type=Path, required=True)
    ap.add_argument("--snapshot", type=Path, required=True)
    ap.add_argument("--out", type=Path, default=Path("/tmp/std_collapse_hold_down_grid"))
    ap.add_argument("--ratios", default="0.10,0.15,0.20,0.25")
    ap.add_argument("--hold-downs", default="0,900,1800,3600,7200")
    ap.add_argument("--workers", type=int, default=max(1, multiprocessing.cpu_count() // 2))
    ap.add_argument("--use-existing-bin", action="store_true")
    args = ap.parse_args()

    if not BINARY.exists():
        print(f"ERROR: missing {BINARY}; build pairtrade release binary first", file=sys.stderr)
        return 1
    args.out.mkdir(parents=True, exist_ok=True)

    if args.use_existing_bin:
        live_bin = args.out / "shared/live.bin"
        if not live_bin.exists():
            src = Path("/tmp/bt_live_data/live.bin")
            if not src.exists():
                print("ERROR: --use-existing-bin requires out/shared/live.bin or /tmp/bt_live_data/live.bin", file=sys.stderr)
                return 1
            live_bin.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, live_bin)
    else:
        live_bin = ensure_shared_bin(args.out, args.data_dir)

    ratios = parse_float_list(args.ratios)
    hold_downs = parse_int_list(args.hold_downs)
    cells = [Cell(r, h) for r, h in itertools.product(ratios, hold_downs)]
    template = PROD_YAML.read_text()

    print(f"# std-collapse hold-down grid: {len(cells)} cells, workers={args.workers}", file=sys.stderr)
    with multiprocessing.Pool(args.workers) as pool:
        results = dict(pool.imap_unordered(
            run_cell,
            [(c, args.out, live_bin, args.events_dir, args.snapshot, template) for c in cells],
        ))

    baseline_cell = Cell(0.20, 0) if Cell(0.20, 0) in results else Cell(ratios[0], 0)
    baseline = results[baseline_cell]

    csv_path = args.out / "results.csv"
    with open(csv_path, "w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow([
            "ratio", "hold_down_secs", "fee", "trades", "pnl", "delta_pnl",
            "maxdd", "delta_maxdd", "calmar", "reject_std_hold_down",
            "blocked_trades", "blocked_loss", "blocked_loss_pnl",
            "blocked_loss_recall", "natural_win_kill", "natural_win_kill_pnl",
            "natural_win_kill_rate",
        ])
        for cell in sorted(cells, key=lambda c: (c.ratio, c.hold_down_secs)):
            result = results[cell]
            cmp_ = compare_to_baseline(baseline["trades"], result["trades"])
            for fee in ("0bp", "5bp"):
                m = result[fee]
                b = baseline[fee]
                writer.writerow([
                    cell.ratio, cell.hold_down_secs, fee, m["n"], f"{m['pnl']:.4f}",
                    f"{m['pnl'] - b['pnl']:.4f}", f"{m['maxdd']:.4f}",
                    f"{m['maxdd'] - b['maxdd']:.4f}", f"{m['calmar']:.4f}",
                    result["rejects"].get("std_collapse_hold_down", 0),
                    cmp_["blocked_trades"], cmp_["blocked_loss"], f"{cmp_['blocked_loss_pnl']:.4f}",
                    f"{cmp_['blocked_loss_recall']:.4f}", cmp_["natural_win_kill"],
                    f"{cmp_['natural_win_kill_pnl']:.4f}", f"{cmp_['natural_win_kill_rate']:.4f}",
                ])

    print(f"# results saved to {csv_path}\n")
    print("## 0bp summary vs baseline\n")
    print("| ratio | hold_down | trades | PnL | dPnL | MaxDD | dMaxDD | hold rejects | blocked loss | win kill |")
    print("|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for cell in sorted(cells, key=lambda c: (c.ratio, c.hold_down_secs)):
        result = results[cell]
        cmp_ = compare_to_baseline(baseline["trades"], result["trades"])
        m = result["0bp"]
        b = baseline["0bp"]
        print(
            f"| {cell.ratio:.2f} | {cell.hold_down_secs} | {m['n']} | {m['pnl']:+.2f} "
            f"| {m['pnl'] - b['pnl']:+.2f} | {m['maxdd']:.2f} | {m['maxdd'] - b['maxdd']:+.2f} "
            f"| {result['rejects'].get('std_collapse_hold_down', 0)} "
            f"| {cmp_['blocked_loss']} | {cmp_['natural_win_kill']} |"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
