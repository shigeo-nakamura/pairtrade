#!/usr/bin/env python3
"""Structural regression checks for the Robinhood deploy failure boundary."""

from pathlib import Path


repo = Path(__file__).resolve().parents[1]
config_workflow = (repo / ".github/workflows/deploy-configs.yml").read_text()
runtime_workflow = (repo / ".github/workflows/ci.yml").read_text()
runtime_deployer = (repo / "scripts/deploy-robinhood-runtime.sh").read_text()
sidecar_activator = (repo / "scripts/activate-robinhood-sidecar.sh").read_text()
service_unit = (repo / "deploy/debot-pair-robinhood-lighter.service").read_text()

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
assert "RUNTIME_DIR_OWNER=${RUNTIME_DIR_OWNER:-ec2-user}" in runtime_deployer
assert "RUNTIME_DIR_GROUP=${RUNTIME_DIR_GROUP:-ec2-user}" in runtime_deployer
assert "systemctl restart debot-pair-robinhood-lighter" not in runtime_job
assert "systemctl start debot-pair-robinhood-lighter" not in runtime_job

hook_download = runtime_job.index(
    "aws s3 cp s3://${S3_BUCKET}/deploy/debot-pair-robinhood-lighter.service"
)
hook_verify = runtime_job.index(
    "systemd-analyze verify /tmp/debot-pair-robinhood-lighter.service"
)
hook_install = runtime_job.index(
    "mv /tmp/debot-pair-robinhood-lighter.service "
    "/etc/systemd/system/debot-pair-robinhood-lighter.service"
)
hook_reload = runtime_job.index("systemctl daemon-reload")
runtime_deploy = runtime_job.index("deploy-robinhood-runtime.sh")
assert hook_download < hook_verify < hook_install < hook_reload < runtime_deploy, (
    "the coordinated-start unit must be validated and installed before the "
    "new runtime and local sidecar bundle are committed"
)

preflight = runtime_deployer.index('bash "$BOOTSTRAP_BIN" --validate-only')
runtime_commit = runtime_deployer.index("if ! commit_runtime")
assert preflight < runtime_commit
assert 'bash "$BOOTSTRAP_BIN" "$S3_BUCKET" "$INSTALL_DIR"' not in runtime_deployer
assert "robinhood-sidecar-bundle" in runtime_deployer
assert "robinhood-sidecar-s3-bucket" not in runtime_deployer
assert "s3://" not in sidecar_activator
assert "SIDECAR_BUNDLE_DIR" in sidecar_activator
assert "bundle not staged yet" in sidecar_activator

sidecar_prestart = service_unit.index(
    "ExecStartPre=+/bin/bash /opt/debot/scripts/activate-robinhood-sidecar.sh"
)
bot_start = service_unit.index("ExecStart=/bin/bash /opt/debot/scripts/debot-pair-robinhood-lighter.sh")
assert sidecar_prestart < bot_start

print("Robinhood deploy workflow isolation tests passed")
