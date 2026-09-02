#!/usr/bin/env python3
"""Structural regression checks for the Robinhood deploy failure boundary."""

from pathlib import Path


repo = Path(__file__).resolve().parents[1]
config_workflow = (repo / ".github/workflows/deploy-configs.yml").read_text()
runtime_workflow = (repo / ".github/workflows/ci.yml").read_text()
runtime_deployer = (repo / "scripts/deploy-robinhood-runtime.sh").read_text()
sidecar_activator = (repo / "scripts/activate-robinhood-sidecar.sh").read_text()
launcher = (repo / "scripts/debot-pair-robinhood-lighter.sh").read_text()
bot_unit = (repo / "deploy/debot-pair-robinhood-lighter.service").read_text()
activation_unit = (
    repo / "deploy/debot-pair-robinhood-sidecar-activation.service"
).read_text()

activation_name = "debot-pair-robinhood-sidecar-activation.service"
bot_name = "debot-pair-robinhood-lighter.service"

config_job = config_workflow.split("  deploy-robinhood-configs:\n", 1)[1]
assert "bootstrap-robinhood-sidecar.sh" not in config_job, (
    "config-only Robinhood deploy must not bootstrap or restart the sidecar"
)
assert "deploy-robinhood-config.sh" in config_job
assert "debot/engine-b-phase0/releases/${GITHUB_SHA}" in config_workflow
# The Robinhood host's install command is a JSON commands array
# (--parameters file://...), not inline shorthand: __GITHUB_SHA__ is a
# placeholder substituted by a sed line right after the heredoc.
assert "ENGINE_B_PHASE0_OBSERVER_SOURCE=/tmp/engine-b-phase0-__GITHUB_SHA__" in config_job
assert "bash /tmp/engine-b-phase0-__GITHUB_SHA__/scripts/install_engine_b_phase0.sh" in config_job
assert "__GITHUB_SHA__|${GITHUB_SHA}" in config_job
assert "cancel-in-progress: false" in config_workflow
assert activation_name in config_job
assert config_job.index(activation_name) < config_job.index(bot_name)
assert "systemd-analyze verify /tmp/debot-pair-robinhood-sidecar-activation.service " \
       "/tmp/debot-pair-robinhood-lighter.service" in config_job

runtime_job = runtime_workflow.split("  deploy-robinhood-lighter:\n", 1)[1]
assert "deploy-robinhood-runtime.sh" in runtime_job
assert "bootstrap-robinhood-sidecar.sh" not in runtime_job, (
    "runtime workflow must delegate sidecar preflight to the runtime deploy script"
)
assert activation_name in runtime_job
assert runtime_job.index(activation_name) < runtime_job.index(bot_name)
assert "RUNTIME_DIR_OWNER=${RUNTIME_DIR_OWNER:-root}" in runtime_deployer
assert "RUNTIME_DIR_GROUP=${RUNTIME_DIR_GROUP:-root}" in runtime_deployer
assert "systemctl restart debot-pair-robinhood-lighter" not in runtime_job
assert "systemctl start debot-pair-robinhood-lighter" not in runtime_job

activation_download = runtime_job.index(
    "aws s3 cp s3://${S3_BUCKET}/deploy/"
    "debot-pair-robinhood-sidecar-activation.service"
)
bot_download = runtime_job.index(
    "aws s3 cp s3://${S3_BUCKET}/deploy/debot-pair-robinhood-lighter.service"
)
unit_verify = runtime_job.index(
    "systemd-analyze verify /tmp/debot-pair-robinhood-sidecar-activation.service "
    "/tmp/debot-pair-robinhood-lighter.service"
)
activation_install = runtime_job.index(
    "mv /tmp/debot-pair-robinhood-sidecar-activation.service "
    "/etc/systemd/system/debot-pair-robinhood-sidecar-activation.service"
)
bot_install = runtime_job.index(
    "mv /tmp/debot-pair-robinhood-lighter.service "
    "/etc/systemd/system/debot-pair-robinhood-lighter.service"
)
unit_reload = runtime_job.index("systemctl daemon-reload")
runtime_deploy = runtime_job.index("deploy-robinhood-runtime.sh")
assert (
    activation_download
    < bot_download
    < unit_verify
    < activation_install
    < bot_install
    < unit_reload
    < runtime_deploy
), "both coordinated-start units must be installed before runtime publication"

preflight = runtime_deployer.index("bash \"$BOOTSTRAP_BIN\" --validate-only")
publish = runtime_deployer.index("if ! publish_release")
pointer_switch = runtime_deployer.index("\"$MV_BIN\" -Tf \"$NEXT_LINK\"")
assert preflight < publish < pointer_switch
assert "robinhood-releases" in runtime_deployer
assert "robinhood-runtime-current" in runtime_deployer
assert "commit_runtime" not in runtime_deployer
assert "RUNTIME_FILES" not in runtime_deployer
assert "systemctl" not in runtime_deployer
assert "s3://" not in sidecar_activator
assert "robinhood-runtime-current" in sidecar_activator
assert "ROBINHOOD_RUNTIME_PIN" in sidecar_activator
assert "local-bundle \"$RELEASE_DIR\"" in sidecar_activator
assert "pinned legacy runtime" in sidecar_activator

assert "ExecStartPre=+/bin/bash /opt/debot/scripts/activate-robinhood-sidecar.sh" not in bot_unit
assert f"Requires={activation_name}" in bot_unit
assert f"After={activation_name}" in bot_unit
assert "DEBOT_ROBINHOOD_RUNTIME_DIR=/run/debot-pair-robinhood-lighter/runtime" in bot_unit
assert f"Before={bot_name}" in activation_unit
assert "Type=oneshot" in activation_unit
assert "activate-robinhood-sidecar.sh /opt/debot " \
       "/run/debot-pair-robinhood-lighter/runtime" in activation_unit

assert "DEBOT_ROBINHOOD_RUNTIME_DIR" in launcher
assert "exec \"$RUNTIME_DIR/bin/debot\"" in launcher
assert "export LIGHTER_GO_PATH=\"$RUNTIME_DIR/lib\"" in launcher
assert "exec /opt/debot/bin/debot" not in launcher
variant_loader = launcher.split("vars_for_variant() {", 1)[1].split("while IFS=", 1)[0]
assert "unset LIGHTER_PUBLIC_API_KEY LIGHTER_PRIVATE_API_KEY" in variant_loader
assert variant_loader.index("unset LIGHTER_PUBLIC_API_KEY") < variant_loader.index(
    'source "$1"'
)

print("Robinhood atomic deploy workflow tests passed")
