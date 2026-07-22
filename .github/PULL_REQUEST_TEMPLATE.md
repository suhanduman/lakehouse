## What does this PR do?

Short description of the change and why it's needed.

## Related issue(s)

Closes #

## Test gate checklist

All of these must pass locally before requesting review (see
`CONTRIBUTING.md` for setup):

- [ ] `helm lint chart` — clean
- [ ] `helm unittest chart` — all suites pass
- [ ] `bash chart/tests/integrity.sh` — 6/6 render+check combinations pass
- [ ] `bash chart/tests/no-customer-identity_test.sh` — clean (no
      customer-identity tokens in any tracked file, including new/changed
      files in this PR)
- [ ] `bash chart/tests/sizing-tiers_test.sh` — render matrix + content
      assertions + knob-existence + requests-sum all pass
- [ ] `bash bootstrap/tests/bootstrap_test.sh` — pass
- [ ] Console backend / tools `pytest` — pass (if `console/backend/` or
      `tools/` changed)
- [ ] `shellcheck` on any `.sh` files touched — clean

## Checklist

- [ ] I've read `CONTRIBUTING.md`
- [ ] New/changed `values.yaml` knobs are documented inline (doc comment)
      and, if applicable, added to the sizing-tier overlays
- [ ] No secret material (keys, passwords, tokens) is committed
- [ ] Docs updated if this changes user-facing behavior (`README.md`,
      `docs/UPGRADING.md`, etc.)
