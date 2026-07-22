#!/usr/bin/env bash
# chart/tests/no-customer-identity_test.sh — guard test for Product Foundation
# Phase 1 (customer-independence): no customer-identity token may appear in
# ANY git-tracked file in this repo.
#
# Tokens (case-insensitive): `katip`, `çelebi`, `celebi`, and the standalone
# abbreviation `kç`/`KÇ` (word-bounded — see note below).
#
# TEMPORARY EXCLUSIONS (shrink as later Product Foundation tasks land):
#   - docs/superpowers/**, .superpowers/**  — Claude Code planning/session
#     artifacts, not shipped product content; out of scope for this repo's
#     customer-independence entirely.
#   - this script itself (chart/tests/no-customer-identity_test.sh) — it
#     necessarily spells out the tokens it searches for (in these comments
#     and in the grep pattern literal below), so it would otherwise always
#     flag itself. This exclusion is PERMANENT, not one that shrinks.
#   - .gitignore — necessarily spells out the two untracked customer-doc
#     filenames (KatipCelebi-BigData-Sartnamesi-v5.md, KatipÇelebi_v2.txt) so
#     git never re-tracks them. This exclusion is PERMANENT, not one that
#     shrinks (it only grows/changes if the ignored filenames change).
#
# The customer's own requirement documents themselves are no longer
# git-tracked (git rm --cached + .gitignore above) so `git ls-files` never
# surfaces them here — no exclusion entry needed for the documents.
# Top-level README.md and docs/egitim/* have been genericized in place and
# are now covered by the standard scan below.
#
# `kç` note: as a bare 2-letter substring it collides with ordinary Turkish
# words that have nothing to do with the customer name (e.g. "tedarikçi",
# "gerekçe", "açıkça", "nazikçe" all contain "kç" as part of a "-kçi"/"-kçe"
# style suffix). Matching it word-bounded (`\bkç\b`, case-insensitive) avoids
# those false positives while still catching the abbreviation used on its
# own (e.g. "KÇ", "kç").
#
# Uses `git ls-files` to enumerate tracked files. The working-directory path
# name (e.g. a checkout under a customer-named directory) is irrelevant —
# only tracked file CONTENT is checked. Exit 0 = clean.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT" || exit 1

#
# This script's own path is also excluded: it necessarily spells out the
# tokens it searches for (in its comments and in the grep pattern literal
# below), so it would otherwise always flag itself.
EXCLUDE_REGEX='^(docs/superpowers/|\.superpowers/|chart/tests/no-customer-identity_test\.sh$|\.gitignore$)'

fail=0
total_files=0

while IFS= read -r -d '' f; do
  [[ "$f" =~ $EXCLUDE_REGEX ]] && continue
  total_files=$((total_files + 1))
  [ -f "$f" ] || continue
  if hits=$(grep -nIiE 'katip|çelebi|celebi|\bkç\b' -- "$f" 2>/dev/null); then
    while IFS= read -r line; do
      echo "OFFENDING: $f:$line"
    done <<< "$hits"
    fail=1
  fi
done < <(git ls-files -z)

if [ "$fail" -ne 0 ]; then
  echo "no-customer-identity_test.sh: FAIL (customer-identity token(s) found in tracked file(s) above)" >&2
  exit 1
fi

echo "no-customer-identity_test.sh: OK (${total_files} tracked file(s) checked, none excluded, all clean)"
