#!/usr/bin/env python3
"""Structural regression checks for the Robinhood deploy failure boundary."""

from pathlib import Path


repo = Path(__file__).resolve().parents[1]
config_workflow = (repo / ".github/workflows/deploy-configs.yml").read_text()
runtime_workflow = (repo / ".github/workflows/ci.yml").read_text()
runtime_deployer = (repo / "scripts/deploy-robinhood-runtime.sh").read_text()
sidecar_activator = (repo / "scripts/activate-robinhood-sidecar.sh").read_text()
service_unit = (repo / "deploy/debot-pair-robinhood-lighter.service").read_text()
activation_unit = (
    repo / "deploy/debot-pair-robinhood-sidecar-activation.service"
).read_text()

config_job = config_workflow.split("  deploy-robinhood-configs:\n", 1)[1]
assert "bootstrap-robinhood-sidecar.sh" not in config_job, (
    "config-only Robinhood deploy must not bootstrap or restart the sidecar"
)
assert "deploy-robinhood-config.sh" in config_job
assert "debot-pair-robinhood-sidecar-activation.service" in config_job
assert "systemd-analyze verify /tmp/debot-pair-robinhood-sidecar-activation.service" in config_job

runtime_job = runtime_workflow.split("  deploy-robinhood-lighter:\n", 1)[1]
assert "deploy-robinhood-runtime.sh" in runtime_job
assert "bootstrap-robinhood-sidecar.sh" not in runtime_job, (
    "runtime workflow must delegate staged sidecar preflight to the runtime deploy script"
)
assert "RUNTIME_DIR_OWNER=${RUNTIME_DIR_OWNER:-ec2-user}" in runtime_deployer
assert "RUNTIME_DIR_GROUP=${RUNTIME_DIR_GROUP:-ec2-user}" in runtime_deployer
assert "systemctl restart debot-pair-robinhood-lighter" not in runtime_job
assert "systemctl start debot-pair-robinhood-lighter" not in runtime_job

activation_download = runtime_job.index(
    "aws s3 cp s3://${S3_BUCKET}/deploy/"
    "debot-pair-robinhood-sidecar-activation.service"
)
bot_unit_download = runtime_job.index(
    "aws s3 cp s3://${S3_BUCKET}/deploy/debot-pair-robinhood-lighter.service"
)
units_verify = runtime_job.index(
    "systemd-analyze verify /tmp/debot-pair-robinhood-sidecar-activation.service "
    "/tmp/debot-pair-robinhood-lighter.service"
)
activation_install = runtime_job.index(
    "mv /tmp/debot-pair-robinhood-sidecar-activation.service "
    "/etc/systemd/system/debot-pair-robinhood-sidecar-activation.service"
)
bot_unit_install = runtime_job.index(
    "mv /tmp/debot-pair-robinhood-lighter.service "
    "/etc/systemd/system/debot-pair-robinhood-lighter.service"
)
hook_reload = runtime_job.index("systemctl daemon-reload")
runtime_deploy = runtime_job.index("deploy-robinhood-runtime.sh")
assert (
    activation_download
    < bot_unit_download
    < units_verify
    < activation_install
    < bot_unit_install
    < hook_reload
    < runtime_deploy
), (
    "the activation dependency and bot unit must be validated and installed "
    "before runtime commit"
)

preflight = runtime_deployer.index('bash "$BOOTSTRAP_BIN" --validate-only')
runtime_commit = runtime_deployer.index("if ! commit_runtime")
assert preflight < runtime_commit
assert 'bash "$BOOTSTRAP_BIN" "$S3_BUCKET" "$INSTALL_DIR"' not in runtime_deployer
assert "robinhood-sidecar-bundle" in runtime_deployer
assert "robinhood-sidecar-s3-bucket" not in runtime_deployer
assert "s3://" not in sidecar_activator
assert (
    "ROBINHOOD_RUNTIME_LOCK_FILE:-/run/lock/debot-robinhood-runtime.lock"
    in runtime_deployer
)
assert (
    "ROBINHOOD_RUNTIME_LOCK_FILE:-/run/lock/debot-robinhood-runtime.lock"
    in sidecar_activator
)
assert '"$FLOCK_BIN" -x 9' in runtime_deployer
assert '"$FLOCK_BIN" -x 9' in sidecar_activator
assert "SIDECAR_BUNDLE_DIR" in sidecar_activator
assert "bundle not staged yet" in sidecar_activator

assert "ExecStartPre=+/bin/bash /opt/debot/scripts/activate-robinhood-sidecar.sh" not in service_unit
assert "Requires=debot-pair-robinhood-sidecar-activation.service" in service_unit
assert "After=debot-pair-robinhood-sidecar-activation.service" in service_unit
assert "Before=debot-pair-robinhood-lighter.service" in activation_unit
assert "ExecStart=/bin/bash /opt/debot/scripts/activate-robinhood-sidecar.sh" in activation_unit

print("Robinhood deploy workflow isolation tests passed")
