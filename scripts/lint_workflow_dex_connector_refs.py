#!/usr/bin/env python3
"""Check that every workflow takes its dex-connector ref from the shared resolver.

bot-strategy#899 made `_resolve-dex-connector-ref.yml` the single source of
truth for the ref every build checks out, because hand-maintained
`DEX_CONNECTOR_REF: vX.Y.Z` pins drifted from Cargo.lock: ci.yml stayed green
off Cargo.lock's pin while a deploy workflow shipped a binary built against
whatever tag someone last remembered to type (bot-strategy#865).

bot-strategy#973 added a second shape to guard. `_test-arcus-spot-live.yml`
does the checkout on its caller's behalf and takes the ref as a
`workflow_call` input, so "the file that checks out dex-connector calls the
resolver" is no longer the whole rule: the caller owns the ref, and
`dex-connector-ref: v4.7.20` in a `with:` block is the same pin the env var
was.

This started as a `grep` pipeline inside ci.yml. Codex pointed out twice
(pairtrade#314) that a file-wide text search cannot tell which block a match
came from: `_resolve-dex-connector-ref.yml` matched the explanatory comment
every caller carries, `.outputs.ref` matched any job's output rather than the
resolver's, and `ref: ${{ inputs.dex-connector-ref }}` matched a comment or
an unrelated checkout step while the dex-connector checkout itself moved to a
literal. So this walks the parsed workflow instead, and resolves each
checkout's `ref` expression back to the job that produced it.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

import yaml

DEX_CONNECTOR_REPO = "shigeo-nakamura/dex-connector"
RESOLVER = "./.github/workflows/_resolve-dex-connector-ref.yml"

NEEDS_OUTPUT_RE = re.compile(
    r"^\$\{\{\s*needs\.(?P<job>[A-Za-z0-9_-]+)\.outputs\.(?P<output>[A-Za-z0-9_-]+)\s*\}\}$"
)
ENV_RE = re.compile(r"^\$\{\{\s*env\.(?P<name>[A-Za-z0-9_-]+)\s*\}\}$")
INPUTS_RE = re.compile(r"^\$\{\{\s*inputs\.(?P<name>[A-Za-z0-9_-]+)\s*\}\}$")
STATIC_PIN_RE = re.compile(r"^v[0-9]")


class Findings:
    def __init__(self) -> None:
        self.items: list[tuple[str, str]] = []

    def add(self, path: str, message: str) -> None:
        self.items.append((path, message))

    def __bool__(self) -> bool:
        return bool(self.items)

    def __repr__(self) -> str:  # keeps assertion output readable
        return f"Findings({self.items!r})"


def _load(path: Path) -> dict:
    with path.open() as handle:
        loaded = yaml.safe_load(handle)
    return loaded if isinstance(loaded, dict) else {}


def _on(workflow: dict) -> dict:
    # YAML 1.1 parses a bare `on:` key as the boolean True.
    trigger = workflow.get("on", workflow.get(True))
    return trigger if isinstance(trigger, dict) else {}


def _jobs(workflow: dict) -> dict:
    jobs = workflow.get("jobs")
    return jobs if isinstance(jobs, dict) else {}


def _needs(job: dict) -> list[str]:
    needs = job.get("needs")
    if isinstance(needs, str):
        return [needs]
    if isinstance(needs, list):
        return [n for n in needs if isinstance(n, str)]
    return []


def _steps(job: dict) -> list[dict]:
    steps = job.get("steps")
    if not isinstance(steps, list):
        return []
    return [s for s in steps if isinstance(s, dict)]


def _env(scope: dict) -> dict:
    env = scope.get("env")
    return env if isinstance(env, dict) else {}


def _is_resolver_job(job: dict) -> bool:
    return isinstance(job.get("uses"), str) and job["uses"].strip() == RESOLVER


def _local_reusable_path(uses: str, workflows_dir: Path) -> Path | None:
    """Map a `uses:` value to the local reusable workflow file it names.

    Resolved against the directory being linted rather than the repository
    root, so a copy of the workflows (a test fixture, a pre-merge tree)
    still finds its own siblings.
    """
    uses = uses.strip()
    prefix = "./.github/workflows/"
    if not uses.startswith(prefix):
        return None
    name = uses[len(prefix):]
    if "/" in name:
        return None
    return workflows_dir / name


def _reusable_inputs(path: Path) -> dict:
    if not path.is_file():
        return {}
    call = _on(_load(path)).get("workflow_call")
    if not isinstance(call, dict):
        return {}
    inputs = call.get("inputs")
    return inputs if isinstance(inputs, dict) else {}


def _describe(expression: object) -> str:
    return "(missing)" if expression is None else repr(expression)


def _resolve_ref_source(
    expression: object,
    workflow: dict,
    job_name: str,
    job: dict,
    seen_env: set[str],
) -> tuple[str, str]:
    """Classify where a checkout's `ref:` value comes from.

    Returns (kind, detail) where kind is one of:
      "resolver"  - a resolver job's output, correctly wired
      "input"     - this workflow's own `dex-connector-ref` workflow_call input
      "bad"       - anything else; detail explains why
    """
    if not isinstance(expression, str):
        return "bad", f"ref is {_describe(expression)}, not a resolver-derived expression"

    expression = expression.strip()

    match = INPUTS_RE.match(expression)
    if match:
        name = match.group("name")
        inputs = _on(workflow).get("workflow_call")
        declared = isinstance(inputs, dict) and name in (inputs.get("inputs") or {})
        if name != "dex-connector-ref":
            return "bad", f"ref comes from input `{name}`, not `dex-connector-ref`"
        if not declared:
            return "bad", "ref uses `inputs.dex-connector-ref`, which the workflow does not declare"
        return "input", name

    match = ENV_RE.match(expression)
    if match:
        name = match.group("name")
        if name in seen_env:
            return "bad", f"env `{name}` resolves to itself"
        value = _env(job).get(name, _env(workflow).get(name))
        if value is None:
            return "bad", f"ref uses env `{name}`, which is not defined for this job"
        return _resolve_ref_source(value, workflow, job_name, job, seen_env | {name})

    match = NEEDS_OUTPUT_RE.match(expression)
    if match:
        producer_name = match.group("job")
        producer = _jobs(workflow).get(producer_name)
        if not isinstance(producer, dict):
            return "bad", f"ref comes from `needs.{producer_name}`, which is not a job here"
        if not _is_resolver_job(producer):
            return (
                "bad",
                f"ref comes from job `{producer_name}`, which does not call {RESOLVER}",
            )
        if producer_name not in _needs(job):
            return (
                "bad",
                f"job `{job_name}` reads `needs.{producer_name}` without listing it in `needs`",
            )
        return "resolver", producer_name

    return "bad", f"ref is {_describe(expression)}, not a resolver-derived expression"


def _check_static_pins(path: Path, workflow: dict, findings: Findings) -> None:
    scopes = [("workflow", workflow)]
    for job_name, job in _jobs(workflow).items():
        if not isinstance(job, dict):
            continue
        scopes.append((f"job `{job_name}`", job))
        for index, step in enumerate(_steps(job)):
            scopes.append((f"job `{job_name}` step {index}", step))
    for where, scope in scopes:
        value = _env(scope).get("DEX_CONNECTOR_REF")
        if isinstance(value, str) and STATIC_PIN_RE.match(value.strip()):
            findings.add(
                str(path),
                f"{where} pins DEX_CONNECTOR_REF to the literal `{value.strip()}` — "
                f"resolve it from Cargo.lock via {RESOLVER} instead (bot-strategy#899).",
            )


def _check_checkouts(path: Path, workflow: dict, findings: Findings) -> None:
    is_reusable = "workflow_call" in _on(workflow)
    for job_name, job in _jobs(workflow).items():
        if not isinstance(job, dict):
            continue
        for index, step in enumerate(_steps(job)):
            uses = step.get("uses")
            if not isinstance(uses, str) or not uses.startswith("actions/checkout"):
                continue
            with_block = step.get("with")
            if not isinstance(with_block, dict):
                continue
            if str(with_block.get("repository", "")).strip() != DEX_CONNECTOR_REPO:
                continue
            kind, detail = _resolve_ref_source(
                with_block.get("ref"), workflow, job_name, job, set()
            )
            where = f"job `{job_name}` step {index}"
            if kind == "resolver":
                continue
            if kind == "input":
                if is_reusable:
                    continue
                findings.add(
                    str(path),
                    f"{where} checks out dex-connector from a workflow_call input, "
                    "but this workflow is not reusable.",
                )
                continue
            findings.add(
                str(path),
                f"{where} checks out dex-connector but {detail} "
                f"(bot-strategy#899/#973).",
            )


def _check_reusable_calls(
    path: Path, workflow: dict, workflows_dir: Path, findings: Findings
) -> None:
    for job_name, job in _jobs(workflow).items():
        if not isinstance(job, dict):
            continue
        uses = job.get("uses")
        if not isinstance(uses, str):
            continue
        target = _local_reusable_path(uses, workflows_dir)
        if target is None or "dex-connector-ref" not in _reusable_inputs(target):
            continue
        with_block = job.get("with")
        with_block = with_block if isinstance(with_block, dict) else {}
        kind, detail = _resolve_ref_source(
            with_block.get("dex-connector-ref"), workflow, job_name, job, set()
        )
        if kind == "resolver":
            continue
        findings.add(
            str(path),
            f"job `{job_name}` calls {uses.strip()} but {detail} "
            f"(bot-strategy#899/#973).",
        )


def lint_workflows(workflows_dir: Path) -> Findings:
    findings = Findings()
    for path in sorted(workflows_dir.glob("*.yml")):
        try:
            workflow = _load(path)
        except yaml.YAMLError as error:  # pragma: no cover - malformed YAML is its own failure
            findings.add(str(path), f"could not be parsed as YAML: {error}")
            continue
        _check_static_pins(path, workflow, findings)
        _check_checkouts(path, workflow, findings)
        _check_reusable_calls(path, workflow, workflows_dir, findings)
    return findings


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--workflows-dir",
        default=".github/workflows",
        type=Path,
        help="directory holding the workflow files to check",
    )
    args = parser.parse_args(argv)

    findings = lint_workflows(args.workflows_dir)
    for path, message in findings.items:
        if os.environ.get("GITHUB_ACTIONS"):
            print(f"::error file={path}::{message}")
        else:
            print(f"{path}: {message}", file=sys.stderr)
    if findings:
        return 1
    print(f"dex-connector ref lint: {len(list(args.workflows_dir.glob('*.yml')))} workflows OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
