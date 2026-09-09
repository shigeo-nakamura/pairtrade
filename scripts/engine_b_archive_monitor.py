#!/usr/bin/env python3
"""Read-only archive monitoring, independent of the market-data process."""

import argparse
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
from pathlib import Path
import re
import shutil
import subprocess
import time


PREFIX = "engine_b_phase0_archive_"
PARTITION = re.compile(r"engine_b_phase0_(\d{8}_\d{2})\.sqlite3")


def unit_properties(unit):
    result = subprocess.run(
        ["systemctl", "show", unit, "--no-pager",
         "--property=LoadState,ActiveState,Result"],
        check=True, capture_output=True, text=True, timeout=5,
        env={"PATH": "/usr/bin:/bin", "LC_ALL": "C", "TZ": "UTC"},
    )
    values = dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)
    if values.get("LoadState") != "loaded":
        raise ValueError(f"unit not loaded: {unit}")
    return values


def collect(state_dir, retention_hours, now=None, read_unit=unit_properties):
    """Missing evidence remains zero/unknown; an idle successful unit is not a backup."""
    now = time.time() if now is None else now
    values = {"probe_success": 0, "probe_timestamp_seconds": now}
    errors = []
    try:
        data = state_dir / "data"
        usage = shutil.disk_usage(data)
        values.update(disk_available_bytes=usage.free, disk_size_bytes=usage.total)
        partitions = []
        for path in data.iterdir():
            match = PARTITION.fullmatch(path.name)
            if match:
                start = datetime.strptime(match[1], "%Y%m%d_%H").replace(tzinfo=timezone.utc).timestamp()
                partitions.append((start + 3600, path.stat().st_size))
        eligible = [(end, size) for end, size in partitions if end <= now - retention_hours * 3600]
        values.update(
            retained_partition_bytes=sum(size for _, size in partitions),
            eligible_partition_count=len(eligible),
            eligible_partition_bytes=sum(size for _, size in eligible),
            oldest_eligible_age_seconds=max((now - end for end, _ in eligible), default=0),
        )
        # Seals are written before upload verification. Only a seal whose source
        # DB is gone witnesses completion of the verified-deletion path. This is
        # local evidence, not a fresh independent check of the remote S3 object.
        sealed = state_dir / "sealed"
        latest = 0
        for path in sealed.iterdir():
            if not re.fullmatch(r"\d{8}_\d{2}\.json", path.name):
                continue
            partition = path.stem
            if (data / f"engine_b_phase0_{partition}.sqlite3").exists():
                continue
            seal = json.loads(path.read_text())
            if not isinstance(seal, dict):
                raise ValueError(f"invalid seal: {path.name}")
            if seal.get("partition") != partition or not re.fullmatch(r"[0-9a-f]{64}", seal.get("sha256", "")):
                raise ValueError(f"invalid seal: {path.name}")
            if not isinstance(seal.get("sealed_at"), str):
                raise ValueError(f"invalid seal timestamp: {path.name}")
            stamp = datetime.fromisoformat(seal["sealed_at"].replace("Z", "+00:00"))
            if stamp.tzinfo is None or stamp.timestamp() > now:
                raise ValueError(f"invalid seal timestamp: {path.name}")
            latest = max(latest, stamp.timestamp())
        values["last_verified_removal_timestamp_seconds"] = latest
    except (OSError, ValueError, KeyError, TypeError) as exc:
        errors.append(f"storage: {exc}")
    try:
        service = read_unit("engine-b-phase0-archive.service")
        timer = read_unit("engine-b-phase0-archive.timer")
        if "Result" not in service or "ActiveState" not in service or "ActiveState" not in timer:
            raise ValueError("missing systemd properties")
        values.update(
            service_failed=int(service["Result"] != "success" or service["ActiveState"] == "failed"),
            service_running=int(service["ActiveState"] in ("active", "activating")),
            timer_active=int(timer["ActiveState"] == "active"),
        )
    except (OSError, ValueError, subprocess.SubprocessError) as exc:
        errors.append(f"systemd: {exc}")
    values["probe_success"] = int(not errors)
    return {"metrics": values, "errors": errors}


def render(report):
    return "".join(
        f"# TYPE {PREFIX}{name} gauge\n{PREFIX}{name} {value}\n"
        for name, value in sorted(report["metrics"].items())
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, default=Path("/var/lib/engine-b-phase0"))
    parser.add_argument("--retention-hours", type=int, default=24)
    parser.add_argument("--port", type=int, default=9473)
    parser.add_argument("--once", action="store_true", help="Print a JSON report and exit")
    args = parser.parse_args()
    if args.retention_hours < 0:
        parser.error("retention must be nonnegative")
    if args.once:
        report = collect(args.state_dir, args.retention_hours)
        print(json.dumps(report, indent=2))
        return 0 if report["metrics"]["probe_success"] else 1

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path != "/metrics":
                self.send_error(404)
                return
            body = render(collect(args.state_dir, args.retention_hours)).encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args):
            pass

    HTTPServer(("127.0.0.1", args.port), Handler).serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
