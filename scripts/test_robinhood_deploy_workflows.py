#!/usr/bin/env python3
"""Structural regression checks for the Robinhood deploy failure boundary."""

from pathlib import Path


repo = Path(__file__).resolve().parents[1]
config_workflow = (repo / ".github/workflows/deploy-configs.yml").read_text()
runtime_workflow = (repo / ".github/workflows/ci.yml").read_text()

config_job = config_workflow.split("  deploy-robinhood-configs:\n", 1)[1]
assert "bootstrap-robinhood-sidecar.sh" not in config_job, (
    "config-only Robinhood deploy must not bootstrap or restart the sidecar"
)
assert "deploy-robinhood-config.sh" in config_job

runtime_job = runtime_workflow.split("  deploy-robinhood-lighter:\n", 1)[1]
assert "deploy-robinhood-runtime.sh" in runtime_job
assert "bootstrap-robinhood-sidecar.sh" not in runtime_job, (
    "runtime workflow must delegate staged sidecar preflight to the runtime deploy script"
)
assert "systemctl restart debot-pair-robinhood-lighter" not in runtime_job
assert "systemctl start debot-pair-robinhood-lighter" not in runtime_job

print("Robinhood deploy workflow isolation tests passed")
