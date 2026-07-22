# Contributing

Thanks for your interest in contributing to Lakehouse. This guide covers
dev environment setup, the test gates every PR must pass, and the PR flow.

## Dev setup

**Helm chart (`chart/`):**

```bash
# Helm 3 + the helm-unittest plugin
helm plugin install https://github.com/helm-unittest/helm-unittest
```

**Console backend / tools (Python):**

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r console/backend/requirements.txt
pip install -r tools/requirements.txt   # if present; otherwise pytest + project deps
pip install pytest
```

**Shell scripts (bootstrap, chart test scripts):**

```bash
# shellcheck — lint every .sh file you touch before opening a PR
shellcheck bootstrap/*.sh chart/tests/*.sh bootstrap/tests/*.sh
```

## Running the test gates locally

Every one of these must pass before a PR is merged; run them all locally
first:

```bash
# Helm chart unit tests
helm unittest chart

# Full-chart namespace + service-reference-closure integrity gate
bash chart/tests/integrity.sh

# Customer/tenant-identity guard — no hardcoded customer-specific tokens
bash chart/tests/no-customer-identity_test.sh

# Sizing-tier overlays (dev/medium/large) render + content-assertion suite
bash chart/tests/sizing-tiers_test.sh

# Bootstrap (Layer-0) script test suite
bash bootstrap/tests/bootstrap_test.sh

# Console backend + tools Python test suites
cd console/backend && pytest && cd -
python3 -m pytest tools -q   # from the repo root (tools tests import the `tools.` package)
```

Also run `helm lint chart` — it must stay clean (no `[ERROR]` lines).

## PR flow

1. Branch off `main`: `git checkout -b <your-branch>`.
2. Make your change. Keep commits focused — one logical change per commit.
3. Run the full test gate list above locally before opening the PR.
4. Open a PR against `main`. CI re-runs the same gates; all must be green.
5. Address review feedback; avoid force-pushing over review comments when
   possible (prefer new commits during review, squash/rebase before merge
   if the maintainer asks for it).

## Code style

- Comments may be written in **Turkish or English** — this codebase has a
  bilingual comment history and both are accepted. Prefer English for new
  community-facing documentation (README, this file, etc.).
- Whatever language you write in, the customer-identity guard
  (`chart/tests/no-customer-identity_test.sh`) must stay clean: no
  customer-specific names/abbreviations may appear in any tracked file.
  Keep new content generic/neutral (example domains, placeholder names).
- Match the existing style of the file you're editing (indentation, header
  comment conventions, doc-comment density) rather than introducing a new
  convention in a single file.
