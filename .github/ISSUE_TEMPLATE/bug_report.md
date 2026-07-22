---
name: Bug report
about: Report a problem with the chart, bootstrap, console, or docs
title: "[Bug] "
labels: bug
---

## Describe the bug

A clear, concise description of what's wrong.

## Environment

- Chart version (`chart/Chart.yaml` `version`, or output of `helm list`):
- Platform: `vanilla` / `openshift`
- Sizing tier (if any): `values-dev.yaml` / `values-medium.yaml` / `values-large.yaml` / custom
- Secrets mode: `generated` / `external` / `manual`
- Kubernetes/OpenShift version:
- Helm version (`helm version`):

## Steps to reproduce

1.
2.
3.

## Expected behavior

What you expected to happen.

## Actual behavior

What actually happened. Include relevant logs/output (redact any secret
material before pasting).

## Relevant test gate output (if applicable)

If one of the test suites fails, paste its output:

```
helm lint chart
helm unittest chart
bash chart/tests/integrity.sh
bash chart/tests/no-customer-identity_test.sh
bash chart/tests/sizing-tiers_test.sh
bash bootstrap/tests/bootstrap_test.sh
```

## Additional context

Anything else that might help (values overlay used, custom overrides, etc.).
