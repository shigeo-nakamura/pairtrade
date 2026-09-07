#!/bin/bash
# Is an S3 object present?  exit 0 = yes, 10 = confirmed missing (404),
# 1 = could not tell (403, throttling, 5xx, ...).
#
# The deploy workflows use this to distinguish a genuine first rollout,
# where the counterpart artifact has not been published yet and skipping
# is correct, from a transient or permission failure, where skipping would
# leave a freshly built binary uninstalled behind a green run.
# `head-object` reports far more than 404 (see the AWS API docs), so its
# exit status alone cannot carry that distinction.
set -uo pipefail

BUCKET=${1:?bucket}
KEY=${2:?key}

if out=$(aws s3api head-object --bucket "$BUCKET" --key "$KEY" 2>&1); then
  exit 0
fi
if printf '%s' "$out" | grep -qE '\(404\)|Not Found'; then
  exit 10
fi
echo "s3_object_present: cannot determine whether s3://$BUCKET/$KEY exists: $out" >&2
exit 1
