#!/usr/bin/env python3
"""Unit tests for scripts/lint_workflow_dex_connector_refs.py (bot-strategy#973).

Every case here is a mutation the previous `grep`-based lint let through, or
a shape the repository actually uses. The point of the lint is to fail on a
dex-connector ref that did not come from the shared resolver, so each test
asserts on that failure rather than only on the happy path.
"""

from __future__ import annotations

import shutil
import textwrap
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from lint_workflow_dex_connector_refs import lint_workflows

REPO_ROOT = Path(__file__).resolve().parent.parent

RESOLVER_WORKFLOW = """\
name: Resolve dex-connector ref
on:
  workflow_call:
    inputs:
      input-ref:
        required: false
        type: string
        default: ""
    outputs:
      ref:
        value: ${{ jobs.resolve.outputs.ref }}
jobs:
  resolve:
    runs-on: ubuntu-latest
    outputs:
      ref: ${{ steps.resolve.outputs.ref }}
    steps:
      - id: resolve
        run: echo "ref=v1.2.3" >> "$GITHUB_OUTPUT"
"""

REUSABLE_TEST_WORKFLOW = """\
name: Test Arcus Spot live executor
on:
  workflow_call:
    inputs:
      dex-connector-ref:
        required: true
        type: string
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v5
        with:
          path: debot
      - uses: actions/checkout@v5
        with:
          repository: shigeo-nakamura/dex-connector
          ref: ${{ inputs.dex-connector-ref }}
          path: dex-connector
      - run: cargo test
"""

CALLER_WORKFLOW = """\
name: Caller
on:
  push: {}
jobs:
  resolve-ref:
    uses: ./.github/workflows/_resolve-dex-connector-ref.yml
  build:
    needs: resolve-ref
    runs-on: ubuntu-latest
    env:
      DEX_CONNECTOR_REF: ${{ needs.resolve-ref.outputs.ref }}
    steps:
      - uses: actions/checkout@v5
        with:
          repository: shigeo-nakamura/dex-connector
          ref: ${{ env.DEX_CONNECTOR_REF }}
          path: dex-connector
  test-arcus-spot-live:
    needs: resolve-ref
    uses: ./.github/workflows/_test-arcus-spot-live.yml
    with:
      dex-connector-ref: ${{ needs.resolve-ref.outputs.ref }}
"""


class LintTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.workflows = Path(self._tmp.name) / ".github" / "workflows"
        self.workflows.mkdir(parents=True)
        self.write("_resolve-dex-connector-ref.yml", RESOLVER_WORKFLOW)
        self.write("_test-arcus-spot-live.yml", REUSABLE_TEST_WORKFLOW)
        self.write("caller.yml", CALLER_WORKFLOW)

    def write(self, name: str, body: str) -> None:
        (self.workflows / name).write_text(textwrap.dedent(body))

    def run_lint(self):
        return lint_workflows(self.workflows)

    def assertClean(self) -> None:
        findings = self.run_lint()
        self.assertFalse(findings, f"expected no findings, got {findings.items}")

    def assertFlags(self, name: str, needle: str) -> None:
        findings = self.run_lint()
        self.assertTrue(findings, f"expected a finding mentioning {needle!r}, got none")
        matching = [m for p, m in findings.items if Path(p).name == name and needle in m]
        self.assertTrue(
            matching,
            f"expected a finding on {name} mentioning {needle!r}; got {findings.items}",
        )


class ValidWiring(LintTestCase):
    def test_the_repository_shape_passes(self) -> None:
        self.assertClean()

    def test_a_workflow_without_dex_connector_is_ignored(self) -> None:
        self.write(
            "unrelated.yml",
            """\
            name: Unrelated
            on:
              push: {}
            jobs:
              build:
                runs-on: ubuntu-latest
                steps:
                  - uses: actions/checkout@v5
                  - run: make
            """,
        )
        self.assertClean()


class StaticPins(LintTestCase):
    def test_a_literal_env_pin_is_rejected(self) -> None:
        self.write("caller.yml", CALLER_WORKFLOW.replace(
            "DEX_CONNECTOR_REF: ${{ needs.resolve-ref.outputs.ref }}",
            "DEX_CONNECTOR_REF: v4.7.20",
        ))
        self.assertFlags("caller.yml", "pins DEX_CONNECTOR_REF")

    def test_a_workflow_level_literal_pin_is_rejected(self) -> None:
        self.write("caller.yml", CALLER_WORKFLOW.replace(
            "jobs:\n",
            "env:\n  DEX_CONNECTOR_REF: v4.7.20\njobs:\n",
            1,
        ))
        self.assertFlags("caller.yml", "pins DEX_CONNECTOR_REF")


class CallerWiring(LintTestCase):
    """Codex round 2, pairtrade#314: `.outputs.ref` from *any* job used to pass."""

    def test_a_literal_tag_passed_to_the_reusable_workflow_is_rejected(self) -> None:
        self.write("caller.yml", CALLER_WORKFLOW.replace(
            "      dex-connector-ref: ${{ needs.resolve-ref.outputs.ref }}",
            "      dex-connector-ref: v4.7.20",
        ))
        self.assertFlags("caller.yml", "not a resolver-derived expression")

    def test_an_output_named_ref_from_an_unrelated_job_is_rejected(self) -> None:
        self.write("caller.yml", CALLER_WORKFLOW.replace(
            "  test-arcus-spot-live:\n    needs: resolve-ref",
            "  test-arcus-spot-live:\n    needs: [resolve-ref, build]",
        ).replace(
            "      dex-connector-ref: ${{ needs.resolve-ref.outputs.ref }}",
            "      dex-connector-ref: ${{ needs.build.outputs.ref }}",
        ))
        self.assertFlags("caller.yml", "does not call")

    def test_a_resolver_output_the_job_does_not_depend_on_is_rejected(self) -> None:
        self.write("caller.yml", CALLER_WORKFLOW.replace(
            "  test-arcus-spot-live:\n    needs: resolve-ref\n",
            "  test-arcus-spot-live:\n",
        ))
        self.assertFlags("caller.yml", "without listing it in `needs`")

    def test_omitting_the_input_entirely_is_rejected(self) -> None:
        self.write("caller.yml", CALLER_WORKFLOW.replace(
            "    with:\n      dex-connector-ref: ${{ needs.resolve-ref.outputs.ref }}\n",
            "",
        ))
        self.assertFlags("caller.yml", "(missing)")

    def test_naming_the_resolver_only_in_a_comment_is_rejected(self) -> None:
        self.write("caller.yml", CALLER_WORKFLOW.replace(
            "  resolve-ref:\n    uses: ./.github/workflows/_resolve-dex-connector-ref.yml",
            "  # uses: ./.github/workflows/_resolve-dex-connector-ref.yml\n"
            "  resolve-ref:\n    uses: ./.github/workflows/_other.yml",
        ))
        self.assertFlags("caller.yml", "does not call")


class CheckoutWiring(LintTestCase):
    """Codex round 2, pairtrade#314: the ref match must belong to *this* checkout."""

    def test_a_reusable_workflow_checking_out_a_literal_is_rejected(self) -> None:
        self.write("_test-arcus-spot-live.yml", REUSABLE_TEST_WORKFLOW.replace(
            "          ref: ${{ inputs.dex-connector-ref }}",
            "          ref: v4.7.22",
        ))
        self.assertFlags("_test-arcus-spot-live.yml", "not a resolver-derived expression")

    def test_the_input_surviving_on_another_step_does_not_excuse_the_literal(self) -> None:
        # The exact shape a file-wide fixed-string grep could not see: the
        # dex-connector checkout moves to a literal while the interpolation
        # stays behind on an unrelated checkout step.
        self.write("_test-arcus-spot-live.yml", REUSABLE_TEST_WORKFLOW.replace(
            "      - uses: actions/checkout@v5\n        with:\n          path: debot\n",
            "      - uses: actions/checkout@v5\n        with:\n"
            "          repository: shigeo-nakamura/pairtrade\n"
            "          ref: ${{ inputs.dex-connector-ref }}\n          path: debot\n",
        ).replace(
            "          ref: ${{ inputs.dex-connector-ref }}\n          path: dex-connector",
            "          ref: v4.7.22\n          path: dex-connector",
        ))
        self.assertFlags("_test-arcus-spot-live.yml", "not a resolver-derived expression")

    def test_a_checkout_reading_an_env_var_defined_from_an_unrelated_job_is_rejected(self) -> None:
        self.write("caller.yml", CALLER_WORKFLOW.replace(
            "      DEX_CONNECTOR_REF: ${{ needs.resolve-ref.outputs.ref }}",
            "      DEX_CONNECTOR_REF: ${{ needs.other.outputs.ref }}",
        ))
        self.assertFlags("caller.yml", "which is not a job here")

    def test_a_checkout_reading_an_undefined_env_var_is_rejected(self) -> None:
        self.write("caller.yml", CALLER_WORKFLOW.replace(
            "    env:\n      DEX_CONNECTOR_REF: ${{ needs.resolve-ref.outputs.ref }}\n",
            "",
        ))
        self.assertFlags("caller.yml", "not defined for this job")

    def test_a_non_reusable_workflow_may_not_read_a_workflow_call_input(self) -> None:
        self.write("caller.yml", CALLER_WORKFLOW.replace(
            "          ref: ${{ env.DEX_CONNECTOR_REF }}",
            "          ref: ${{ inputs.dex-connector-ref }}",
        ))
        self.assertFlags("caller.yml", "does not declare")


class RepositoryTree(unittest.TestCase):
    """The checked-in workflows must pass, and must still be being checked."""

    def test_the_checked_in_workflows_pass(self) -> None:
        findings = lint_workflows(REPO_ROOT / ".github" / "workflows")
        self.assertFalse(findings, f"repository workflows have findings: {findings.items}")

    def test_pinning_a_real_workflow_is_caught(self) -> None:
        with TemporaryDirectory() as tmp:
            workflows = Path(tmp) / "workflows"
            shutil.copytree(REPO_ROOT / ".github" / "workflows", workflows)
            target = workflows / "deploy-arcus-spot-executor.yml"
            body = target.read_text()
            mutated = body.replace(
                "      dex-connector-ref: ${{ needs.resolve-ref.outputs.ref }}",
                "      dex-connector-ref: v4.7.20",
            )
            self.assertNotEqual(body, mutated, "mutation did not apply — update this test")
            target.write_text(mutated)
            self.assertTrue(
                lint_workflows(workflows),
                "a literal dex-connector-ref in a real workflow was not caught",
            )


if __name__ == "__main__":
    unittest.main()
