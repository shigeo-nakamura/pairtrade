#!/usr/bin/env python3
"""Summarize pairtrade observer eligibility and z-score duty cycle."""

from __future__ import annotations

import argparse
import gzip
import math
import re
import statistics
from collections import defaultdict
from datetime import datetime
from pathlib import Path

OUTER_TS = re.compile(
    r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?[+-]\d{4})\s"
)
PAIR_METRIC = re.compile(
    r"(?P<pair>[A-Z0-9]+/[A-Z0-9]+)\s+elig=(?P<elig>true|false)\s+"
    r"z=(?P<z>[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?)"
)


def open_text(path: Path):
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8", errors="replace")
    return path.open(encoding="utf-8", errors="replace")


def percentage(numerator: int, denominator: int) -> str:
    return f"{100 * numerator / denominator:.1f}%" if denominator else "n/a"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("logs", nargs="+", type=Path)
    parser.add_argument(
        "--lighter-baseline",
        default="18-31%",
        help="comparison label shown in the table (default: %(default)s)",
    )
    args = parser.parse_args()

    samples: dict[str, list[tuple[bool, float]]] = defaultdict(list)
    first = None
    last = None
    counts = defaultdict(int)

    for path in args.logs:
        with open_text(path) as src:
            for line in src:
                stamp_match = OUTER_TS.match(line)
                if stamp_match:
                    stamp = datetime.strptime(
                        stamp_match.group(1), "%Y-%m-%dT%H:%M:%S.%f%z"
                    )
                    first = stamp if first is None else min(first, stamp)
                    last = stamp if last is None else max(last, stamp)
                counts["selected"] += 1
                counts["entries"] += "[ENTRY]" in line
                counts["exits"] += "[EXIT]" in line or "[CLOSE]" in line
                counts["warnings"] += "[WARN]" in line
                counts["errors"] += "[ERROR]" in line
                if "[METRICS]" not in line:
                    continue
                counts["metric_lines"] += 1
                for match in PAIR_METRIC.finditer(line):
                    samples[match.group("pair")].append(
                        (match.group("elig") == "true", float(match.group("z")))
                    )

    if first and last:
        hours = (last - first).total_seconds() / 3600
        print(f"Coverage: {first.isoformat()} -> {last.isoformat()} ({hours:.2f}h)")
    else:
        print("Coverage: no timestamped lines")
    print(
        "Selected lines: "
        f"{counts['selected']} (metrics={counts['metric_lines']}, "
        f"entries={counts['entries']}, exits={counts['exits']}, "
        f"warnings={counts['warnings']}, errors={counts['errors']})"
    )
    print()
    print("| Pair | N | elig=true | z mean | z stdev | z range | |z|>=1.5 | |z|>=2 | |z|>=3 |")
    print("|---|---:|---:|---:|---:|---:|---:|---:|---:|")
    for pair in sorted(samples):
        values = samples[pair]
        z_values = [z for _, z in values if math.isfinite(z)]
        eligible = sum(flag for flag, _ in values)
        mean = statistics.fmean(z_values) if z_values else math.nan
        stdev = statistics.pstdev(z_values) if z_values else math.nan
        z_range = f"{min(z_values):.2f}..{max(z_values):.2f}" if z_values else "n/a"
        print(
            f"| {pair} | {len(values)} | {percentage(eligible, len(values))} | "
            f"{mean:.2f} | {stdev:.2f} | {z_range} | "
            f"{percentage(sum(abs(z) >= 1.5 for z in z_values), len(z_values))} | "
            f"{percentage(sum(abs(z) >= 2 for z in z_values), len(z_values))} | "
            f"{percentage(sum(abs(z) >= 3 for z in z_values), len(z_values))} |"
        )
    print()
    print(f"Lighter comparison baseline: elig=true {args.lighter_baseline}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
