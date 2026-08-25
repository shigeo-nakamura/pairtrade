#!/usr/bin/env python3
"""Replay Arcus signals from live-tick's own journaled observations."""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

sys.path.insert(0, str(Path(__file__).resolve().parent))
import arcus_live_tick_event_stream as event_stream

SIGNAL_FLAT_EPSILON = 1e-12


class ReplayError(ValueError):
    pass


@dataclass(frozen=True)
class Observation:
    sequence: int
    observed_at: datetime
    observed_at_raw: str
    relative_log_price: float | None
    z_score: float | None
    hold_code: str | None


@dataclass(frozen=True)
class ThresholdChange:
    effective_at: datetime
    value: float


@dataclass(frozen=True)
class Trade:
    direction: str
    entry_sequence: int
    entry_at: str
    exit_sequence: int
    exit_at: str
    entry_z: float
    exit_z: float | None
    exit_type: str
    hold_seconds: int
    gross_bps: float
    net_bps: float


def parse_timestamp(value: str) -> datetime:
    match = re.fullmatch(
        r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d+))?"
        r"(Z|[+-]\d{2}:\d{2})",
        value,
    )
    if not match:
        raise ReplayError(f"invalid RFC3339 timestamp {value!r}")
    base, fraction, offset = match.groups()
    micros = (fraction or "0")[:6].ljust(6, "0")
    normalized = f"{base}.{micros}{'+00:00' if offset == 'Z' else offset}"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as error:
        raise ReplayError(f"invalid RFC3339 timestamp {value!r}") from error
    return parsed.astimezone(timezone.utc)


def _event_from_object(value: Any) -> Observation | None:
    required = {"sequence", "observed_at", "relative_log_price", "z_score", "decision"}
    if not isinstance(value, dict) or not required.issubset(value):
        return None
    decision = value["decision"]
    if not isinstance(decision, dict):
        raise ReplayError("runtime event decision must be an object")
    hold = decision.get("hold")
    hold_code = hold.get("code") if isinstance(hold, dict) else None
    sequence, observed_at_raw = value["sequence"], value["observed_at"]
    if (not isinstance(sequence, int) or isinstance(sequence, bool) or sequence < 1
            or not isinstance(observed_at_raw, str)):
        raise ReplayError(f"invalid event identity at sequence {sequence!r}")

    def optional_finite(name: str) -> float | None:
        raw = value[name]
        if raw is None:
            return None
        if not isinstance(raw, (int, float)) or isinstance(raw, bool) or not math.isfinite(raw):
            raise ReplayError(f"sequence {sequence}: {name} must be finite or null")
        return float(raw)

    return Observation(sequence, parse_timestamp(observed_at_raw), observed_at_raw,
                       optional_finite("relative_log_price"), optional_finite("z_score"),
                       hold_code if isinstance(hold_code, str) else None)


def extract_observations_with_integrity(
        text: str) -> tuple[list[Observation], int, dict[str, Any] | None]:
    """Extract pretty JSON events from ``journalctl -o cat`` output."""
    first_line = next((line for line in text.splitlines() if line), "")
    try:
        first_object = json.loads(first_line)
    except json.JSONDecodeError:
        first_object = None
    stream_fields = {
        "schema_version", "previous_chain_sha256", "event_sha256",
        "chain_sha256", "event_json",
    }
    if isinstance(first_object, dict) and set(first_object) == stream_fields:
        try:
            values, integrity = event_stream.verify_stream_bytes([
                ("durable-event-stream", text.encode())
            ])
        except event_stream.StreamError as error:
            raise ReplayError(f"invalid durable event stream: {error}") from error
        observations = []
        for value in values:
            event = _event_from_object(value)
            if event is None:
                raise ReplayError("durable stream payload is not a runtime event")
            observations.append(event)
        return observations, 0, integrity

    decoder, observations, ignored, offset = json.JSONDecoder(), [], 0, 0
    while True:
        start = text.find("{", offset)
        if start < 0:
            break
        try:
            value, consumed = decoder.raw_decode(text[start:])
        except json.JSONDecodeError:
            offset = start + 1
            continue
        offset = start + consumed
        event = _event_from_object(value)
        if event is None:
            ignored += 1
        else:
            observations.append(event)
    if not observations:
        raise ReplayError("no Arcus live-tick runtime events found")
    for previous, current in zip(observations, observations[1:]):
        if current.sequence < previous.sequence or current.observed_at < previous.observed_at:
            raise ReplayError(f"journal is not ordered at sequence {current.sequence}")
        if (current.sequence == previous.sequence
                and (current.hold_code != "stale_or_duplicate_observation"
                     or current.relative_log_price is not None
                     or current.z_score is not None)):
            raise ReplayError(
                f"non-advancing sequence {current.sequence} is not a stale observation")
    return observations, ignored, None


def extract_observations(text: str) -> tuple[list[Observation], int]:
    observations, ignored, _ = extract_observations_with_integrity(text)
    return observations, ignored


def informative_sample_count(history: Sequence[float]) -> int:
    if not history:
        return 0
    count, last = 1, history[0]
    for value in history[1:]:
        if abs(value - last) > SIGNAL_FLAT_EPSILON:
            count, last = count + 1, value
    return count


def runtime_z_score(history: Sequence[float], current: float, minimum: int) -> float | None:
    if len(history) < minimum or informative_sample_count(history) < minimum:
        return None
    mean = sum(history) / len(history)
    deviation = math.sqrt(sum((value - mean) ** 2 for value in history) / len(history))
    if not math.isfinite(deviation) or deviation <= SIGNAL_FLAT_EPSILON:
        return None
    score = (current - mean) / deviation
    return score if math.isfinite(score) else None


def verify_recomputed_scores(observations: Sequence[Observation], window: int,
                             minimum: int) -> dict[str, int]:
    history, verified, resets = [], 0, 0
    previous_sequence = None
    for event in observations:
        if previous_sequence is not None and event.sequence > previous_sequence + 1:
            history = []
            resets += 1
        previous_sequence = event.sequence
        current = event.relative_log_price
        if current is None:
            continue
        # The missing pre-retention prefix or an internal sequence gap affects
        # z until one complete live window has been collected after it.
        if len(history) >= window:
            expected = runtime_z_score(history[-window:], current, minimum)
            matches = expected == event.z_score or (
                expected is not None and event.z_score is not None
                and math.isclose(expected, event.z_score, rel_tol=1e-12, abs_tol=1e-12))
            if not matches:
                raise ReplayError(f"z mismatch at sequence {event.sequence}: "
                                  f"logged={event.z_score!r}, recomputed={expected!r}")
            verified += 1
        history.append(current)
    return {"scores_verified": verified, "score_mismatches": 0,
            "score_window_resets": resets}


def load_checkpoint(path: Path) -> dict[str, Any]:
    try:
        checkpoint = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ReplayError(f"failed to read checkpoint {path}: {error}") from error
    if checkpoint.get("schema_version") != 1:
        raise ReplayError(f"unsupported checkpoint schema {checkpoint.get('schema_version')!r}")
    if not isinstance(checkpoint.get("config"), dict) or not isinstance(checkpoint.get("state"), dict):
        raise ReplayError("checkpoint must contain config and state objects")
    return checkpoint


def verify_checkpoint_tail(observations: Sequence[Observation],
                           checkpoint: dict[str, Any]) -> dict[str, Any]:
    state, config = checkpoint["state"], checkpoint["config"]
    history, window, sequence = (state.get("relative_log_price_history"),
                                 config.get("signal_window_samples"), state.get("sequence"))
    minimum = config.get("min_signal_samples")
    if not isinstance(sequence, int) or isinstance(sequence, bool):
        raise ReplayError("checkpoint sequence must be an integer")
    if not isinstance(history, list) or not all(isinstance(v, (int, float))
                                                and not isinstance(v, bool) for v in history):
        raise ReplayError("checkpoint relative_log_price_history must be numeric")
    if (not isinstance(window, int) or isinstance(window, bool) or window < 2
            or len(history) > window):
        raise ReplayError("invalid checkpoint signal window/history length")
    if (not isinstance(minimum, int) or isinstance(minimum, bool)
            or minimum < 2 or minimum > window):
        raise ReplayError("invalid checkpoint minimum signal samples")
    if observations[-1].sequence != sequence:
        raise ReplayError(f"journal ends at {observations[-1].sequence}, checkpoint is {sequence}")
    journal = [event.relative_log_price for event in observations
               if event.relative_log_price is not None]
    if len(journal) < len(history):
        raise ReplayError("journal has fewer samples than the checkpoint tail")
    for index, (logged, stored) in enumerate(zip(journal[-len(history):], history)):
        if float(logged).hex() != float(stored).hex():
            raise ReplayError(f"journal/checkpoint tail differs at index {index}")
    return {"checkpoint_sequence": sequence, "checkpoint_history_samples": len(history),
            "checkpoint_tail_exact": True, "signal_window_samples": window,
            "min_signal_samples": minimum}


def threshold_at(changes: Sequence[ThresholdChange], observed_at: datetime) -> float:
    selected = changes[0].value
    for change in changes[1:]:
        if change.effective_at > observed_at:
            break
        selected = change.value
    return selected


def replay_trades(observations: Sequence[Observation], changes: Sequence[ThresholdChange],
                  exit_z: float, max_hold_secs: int, cost_bps: float
                  ) -> tuple[list[Trade], dict[str, Any] | None]:
    open_trade, trades = None, []
    for event in observations:
        if event.relative_log_price is None:
            continue
        z = event.z_score
        if open_trade is None:
            if z is None:
                continue
            entry_z = threshold_at(changes, event.observed_at)
            if z >= entry_z:
                open_trade = ("token_a_to_b", event)
            elif z <= -entry_z:
                open_trade = ("token_b_to_a", event)
            continue
        direction, entry = open_trade
        hold = int((event.observed_at - entry.observed_at).total_seconds())
        max_hold = hold >= max_hold_secs
        reverted = z is not None and ((direction == "token_a_to_b" and z <= exit_z)
                                      or (direction == "token_b_to_a" and z >= -exit_z))
        if not max_hold and not reverted:
            continue
        gross = ((entry.relative_log_price - event.relative_log_price)
                 if direction == "token_a_to_b"
                 else (event.relative_log_price - entry.relative_log_price)) * 10_000.0
        trades.append(Trade(direction, entry.sequence, entry.observed_at_raw,
                            event.sequence, event.observed_at_raw, float(entry.z_score), z, "max_hold" if max_hold else "mean_reversion",
                            hold, gross, gross - cost_bps))
        open_trade = None
    if open_trade:
        direction, entry = open_trade
        opened = {"direction": direction, "entry_sequence": entry.sequence,
                  "entry_at": entry.observed_at_raw, "entry_z": entry.z_score, "age_seconds_at_last_observation":
                  int((observations[-1].observed_at - entry.observed_at).total_seconds())}
    else:
        opened = None
    return trades, opened


def summarize_trades(trades: Sequence[Trade]) -> dict[str, Any]:
    def group(exit_type: str | None) -> dict[str, Any]:
        selected = [t for t in trades if exit_type is None or t.exit_type == exit_type]
        gross, net = [t.gross_bps for t in selected], [t.net_bps for t in selected]
        return {"count": len(selected), "winners_gross": sum(v > 0 for v in gross),
                "losers_gross": sum(v < 0 for v in gross),
                "mean_gross_bps": sum(gross) / len(gross) if gross else None,
                "sum_gross_bps": sum(gross),
                "mean_net_bps": sum(net) / len(net) if net else None,
                "sum_net_bps": sum(net)}
    return {"mean_reversion": group("mean_reversion"), "max_hold": group("max_hold"),
            "all": group(None)}


def exceedance_counts(observations: Sequence[Observation],
                      thresholds: Iterable[float]) -> dict[str, int]:
    scores = [abs(event.z_score) for event in observations if event.z_score is not None]
    return {format(t, "g"): sum(score >= t for score in scores) for t in thresholds}


def filter_observations(observations: Sequence[Observation], start: int | None,
                        end: int | None) -> list[Observation]:
    selected = [event for event in observations
                if (start is None or event.sequence >= start)
                and (end is None or event.sequence <= end)]
    if not selected:
        raise ReplayError("no observations remain in the requested sequence range")
    return selected


def sequence_gaps(observations: Sequence[Observation]) -> list[dict[str, int]]:
    return [{"after": previous.sequence, "before": current.sequence,
             "missing": current.sequence - previous.sequence - 1}
            for previous, current in zip(observations, observations[1:])
            if current.sequence > previous.sequence + 1]


def parse_threshold_change(raw: str) -> ThresholdChange:
    try:
        timestamp, value = raw.rsplit("=", 1)
        change = ThresholdChange(parse_timestamp(timestamp), float(value))
    except (ValueError, ReplayError) as error:
        raise argparse.ArgumentTypeError("expected RFC3339=POSITIVE_Z") from error
    if not math.isfinite(change.value) or change.value <= 0:
        raise argparse.ArgumentTypeError("threshold must be finite and positive")
    return change


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("journal", type=Path, help="journalctl -o cat export")
    parser.add_argument("--checkpoint", type=Path)
    parser.add_argument("--entry-z", type=float, default=2.5)
    parser.add_argument("--entry-z-change", action="append", default=[], type=parse_threshold_change,
                        metavar="RFC3339=Z")
    parser.add_argument("--exit-z", type=float, default=0.25)
    parser.add_argument("--max-hold-secs", type=int, default=86400)
    parser.add_argument("--round-trip-cost-bps", type=float, default=0.0)
    parser.add_argument("--count-threshold", type=float, action="append", default=[2.5, 2.0, 1.5])
    parser.add_argument("--start-sequence", type=int)
    parser.add_argument("--end-sequence", type=int)
    parser.add_argument("--allow-gaps", action="store_true",
                        help="emit explicitly non-authoritative output across gaps")
    return parser


def run(arguments: argparse.Namespace) -> dict[str, Any]:
    numeric = (arguments.entry_z, arguments.exit_z, arguments.round_trip_cost_bps,
               *arguments.count_threshold)
    if (not all(math.isfinite(value) for value in numeric)
            or arguments.entry_z <= 0 or arguments.exit_z < 0
            or arguments.round_trip_cost_bps < 0 or arguments.max_hold_secs <= 0
            or any(value <= 0 for value in arguments.count_threshold)):
        raise ReplayError("entry/exit/cost/max-hold/count parameters are invalid")
    if (arguments.start_sequence is not None and arguments.end_sequence is not None
            and arguments.start_sequence > arguments.end_sequence):
        raise ReplayError("start sequence must not exceed end sequence")

    observations, ignored, stream_integrity = extract_observations_with_integrity(
        arguments.journal.read_text(encoding="utf-8"))
    observations = filter_observations(
        observations, arguments.start_sequence, arguments.end_sequence)
    gaps = sequence_gaps(observations)
    if gaps and not arguments.allow_gaps:
        ranges = ", ".join(
            f"{gap['after'] + 1}-{gap['before'] - 1}" for gap in gaps)
        raise ReplayError(
            f"journal has {sum(gap['missing'] for gap in gaps)} missing "
            f"sequence(s) ({ranges}); replay refused")

    verification = {"authoritative": not gaps, "sequence_gaps": gaps}
    if stream_integrity is not None:
        verification["durable_event_stream"] = stream_integrity
    window, minimum = 96, 32
    if arguments.checkpoint:
        checkpoint = load_checkpoint(arguments.checkpoint)
        verification.update(verify_checkpoint_tail(observations, checkpoint))
        window = checkpoint["config"]["signal_window_samples"]
        minimum = checkpoint["config"]["min_signal_samples"]
    verification.update(verify_recomputed_scores(observations, window, minimum))

    changes = [ThresholdChange(datetime.min.replace(tzinfo=timezone.utc), arguments.entry_z),
               *arguments.entry_z_change]
    if changes != sorted(changes, key=lambda change: change.effective_at):
        raise ReplayError("threshold changes must be chronological")
    if arguments.exit_z >= min(change.value for change in changes):
        raise ReplayError("exit z must be below every entry threshold")

    trades, opened = replay_trades(observations, changes, arguments.exit_z,
                                   arguments.max_hold_secs,
                                   arguments.round_trip_cost_bps)
    return {"input": {"observations": len(observations),
                      "ignored_json_objects": ignored,
                      "first_sequence": observations[0].sequence,
                      "last_sequence": observations[-1].sequence,
                      "first_observed_at": observations[0].observed_at_raw,
                      "last_observed_at": observations[-1].observed_at_raw},
            "parameters": {"entry_z_schedule": [
                {"effective_at": change.effective_at.isoformat(),
                 "value": change.value} for change in changes],
                "exit_z_score": arguments.exit_z,
                "max_hold_secs": arguments.max_hold_secs,
                "round_trip_cost_bps": arguments.round_trip_cost_bps,
                "counterfactual_initial_regime": "neutral",
                "counterfactual_scope":
                    "signal-only; route/risk/inventory/fills are not replayed"},
            "verification": verification,
            "exceedance_counts": exceedance_counts(
                observations, arguments.count_threshold),
            "summary": summarize_trades(trades),
            "trades": [asdict(trade) for trade in trades],
            "open_trade": opened}


def main() -> int:
    parser = build_parser()
    try:
        report = run(parser.parse_args())
    except (OSError, ReplayError) as error:
        parser.exit(2, f"error: {error}\n")
    json.dump(report, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
