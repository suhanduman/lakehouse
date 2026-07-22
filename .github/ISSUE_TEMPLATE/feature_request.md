---
name: Feature request
about: Suggest a new capability, component, or chart knob
title: "[Feature] "
labels: enhancement
---

## Problem / motivation

What problem does this solve? What can't you do today?

## Proposed solution

Describe the change you'd like (new `values.yaml` knob, new component
toggle, new template, doc addition, etc.).

## Alternatives considered

Any other approaches you thought about, and why you didn't pick them.

## Scope check

- [ ] This fits the project's scope (open-source medallion CDC lakehouse
      platform packaged as a single Helm chart; two platforms —
      `vanilla`/`openshift`; three sizing tiers)
- [ ] This does not require hardcoding any customer-specific value (the
      customer-identity guard, `chart/tests/no-customer-identity_test.sh`,
      must stay clean)
- [ ] If this adds a new `values.yaml` knob, it will be covered by the
      sizing-tier knob-existence check (`chart/tests/sizing-tiers_test.sh`)

## Additional context

Anything else — mockups, links to related issues, upstream component docs.
