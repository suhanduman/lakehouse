#!/usr/bin/env bash
# bootstrap/lib/secrets.sh — sourced library: generate-if-absent Secret
# helpers for bootstrap/bootstrap.sh's "generated" secrets-mode (Layer 0).
#
# Contract (see bootstrap/README.md for the full writeup):
#   - NEVER regenerate/clobber an existing Secret. Every creator checks
#     `kubectl get secret` first and skips if it already exists — that is
#     the whole idempotency story for this file.
#   - NEVER print secret material to stdout/stderr. Only "created <name>" /
#     "exists <name> — skipped" / plan lines (dry-run) are ever printed.
#   - All randomness comes from `openssl rand`, available on every platform
#     this project targets (no extra dependency).
#
# Not a standalone script — no shebang execution expected; always sourced.
# Deliberately does NOT `set -e`/`set -u` itself (a caller sourcing this
# into an already-`set -euo pipefail` shell, as bootstrap.sh does, gets that
# behavior for free; a test sourcing it standalone stays in control of its
# own shell options).

# bootstrap::rand_alnum LEN — LEN random alphanumeric characters ([A-Za-z0-9]).
bootstrap::rand_alnum() {
  local len="$1"
  # Oversample bytes (base64 expands ~4/3x and we then strip non-alnum), loop
  # until we have enough characters. Each iteration is failure-tolerant: under
  # a caller's `set -e` (bootstrap.sh sources this), a bare
  # `out+="$(cmd | cmd2)"` would abort the whole script the instant the
  # pipeline returned non-zero (e.g. a transient openssl hiccup) even though
  # retrying is perfectly safe here — so failures are swallowed per-iteration
  # (`|| chunk=""`) and the loop just tries again, bounded so a persistently
  # broken `openssl`/`tr` fails loudly instead of spinning forever.
  local out="" chunk attempts=0
  while [[ ${#out} -lt $len ]]; do
    chunk="$(openssl rand -base64 "$((len * 2))" 2>/dev/null | tr -dc 'A-Za-z0-9')" || chunk=""
    out+="$chunk"
    attempts=$((attempts + 1))
    if [[ ${#out} -lt $len && $attempts -ge 50 ]]; then
      echo "bootstrap.sh: failed to generate ${len} random alnum character(s) after ${attempts} attempt(s) (openssl/tr unavailable?)" >&2
      return 1
    fi
  done
  printf '%s' "${out:0:len}"
}

# bootstrap::secret_exists NAME NAMESPACE — 0 if the Secret already exists.
bootstrap::secret_exists() {
  local name="$1" namespace="$2"
  kubectl get secret "$name" -n "$namespace" >/dev/null 2>&1
}

# bootstrap::create_secret_if_absent NAME NAMESPACE DRY_RUN LITERAL_ARGS...
#   DRY_RUN: "true" or "false" (string, not a bash boolean).
#   LITERAL_ARGS...: one or more `--from-literal=key=value` args for
#     `kubectl create secret generic`. Never echoed — they are the secret
#     material.
bootstrap::create_secret_if_absent() {
  local name="$1" namespace="$2" dry_run="$3"
  shift 3
  # Check dry_run FIRST, same as bootstrap::ensure_namespace below — a
  # `kubectl get secret` is itself a live cluster read, and --dry-run's
  # whole point is to work without touching a live cluster at all, so it
  # must never even probe existence.
  if [[ "$dry_run" == "true" ]]; then
    echo "  [dry-run] would ensure Secret exists (create if absent): kubectl create secret generic ${name} -n ${namespace} --from-literal=... (values withheld)"
    return 0
  fi
  if bootstrap::secret_exists "$name" "$namespace"; then
    echo "  exists ${name} — skipped"
    return 0
  fi
  kubectl create secret generic "$name" -n "$namespace" "$@" >/dev/null
  echo "  created ${name}"
}

# bootstrap::ensure_namespace NAMESPACE DRY_RUN — idempotent namespace create.
bootstrap::ensure_namespace() {
  local namespace="$1" dry_run="$2"
  if [[ "$dry_run" == "true" ]]; then
    echo "  [dry-run] would ensure namespace exists: kubectl get ns ${namespace} || kubectl create ns ${namespace}"
    return 0
  fi
  if kubectl get ns "$namespace" >/dev/null 2>&1; then
    echo "  namespace exists: ${namespace}"
  else
    kubectl create ns "$namespace" >/dev/null
    echo "  namespace created: ${namespace}"
  fi
}

# bootstrap::seed_generated_secrets NAMESPACE S3_ENDPOINT S3_FROM_ENV DRY_RUN
#   Seeds the four platform-internal Secrets for secrets-mode=generated.
#   S3_ENDPOINT: explicit --s3-endpoint value, or "" to use the in-cluster
#     MinIO default (dev convenience — see NOTE printed below).
#   S3_FROM_ENV: "true" to read S3_ACCESS_KEY_ID/S3_SECRET_ACCESS_KEY from
#     the environment instead of generating random values (customer's own
#     external S3 — never passed as CLI args, never logged).
bootstrap::seed_generated_secrets() {
  local namespace="$1" s3_endpoint="$2" s3_from_env="$3" dry_run="$4"

  # --- s3-credentials ---
  local endpoint="$s3_endpoint"
  if [[ -z "$endpoint" ]]; then
    endpoint="http://minio.${namespace}.svc:9000"
    echo "  NOTE: no --s3-endpoint given — defaulting s3-credentials.endpoint to the in-cluster dev MinIO service (${endpoint})."
    echo "        This assumes components.minio (Task 8) will be enabled. For a real external S3 store, re-run with"
    echo "        --s3-endpoint <URL> --s3-secret-from-env (reads S3_ACCESS_KEY_ID/S3_SECRET_ACCESS_KEY from the environment)."
  fi
  if [[ "$s3_from_env" == "true" ]]; then
    if [[ -z "${S3_ACCESS_KEY_ID:-}" || -z "${S3_SECRET_ACCESS_KEY:-}" ]]; then
      echo "bootstrap.sh: --s3-secret-from-env given but S3_ACCESS_KEY_ID/S3_SECRET_ACCESS_KEY are not both set in the environment" >&2
      return 1
    fi
    bootstrap::create_secret_if_absent s3-credentials "$namespace" "$dry_run" \
      "--from-literal=access-key-id=${S3_ACCESS_KEY_ID}" \
      "--from-literal=secret-access-key=${S3_SECRET_ACCESS_KEY}" \
      "--from-literal=endpoint=${endpoint}" \
      "--from-literal=region=us-east-1"
  else
    bootstrap::create_secret_if_absent s3-credentials "$namespace" "$dry_run" \
      "--from-literal=access-key-id=$(bootstrap::rand_alnum 20)" \
      "--from-literal=secret-access-key=$(bootstrap::rand_alnum 40)" \
      "--from-literal=endpoint=${endpoint}" \
      "--from-literal=region=us-east-1"
  fi

  # --- oidc-credentials ---
  # issuer-url/trino-client-id are BEST-EFFORT placeholders for a stock Task
  # 8 install, not a guarantee — Keycloak itself doesn't exist yet at
  # bootstrap time, so this can only assume the chart's own defaults hold:
  #   - Service name: the Keycloak Operator's generated Service is
  #     "keycloak-service" (chart/templates/_helpers.tpl
  #     `lakehouse.svc.keycloak`, NOT a "keycloak"/"keycloak-service.<ns>"
  #     guess), referenced by the chart's own Certificate/Route.
  #   - Scheme+port: the chart's Keycloak CR sets `http.tlsSecret:
  #     keycloak-tls` (chart/templates/10-keycloak.yaml) with no
  #     `http.httpEnabled`, so the Operator serves HTTPS-only on 8443 (its
  #     documented default whenever a TLS secret is configured) — plain
  #     http/8080 is NOT listening.
  #   - Realm path: `.Values.keycloak.realm.name`, default "lakehouse"
  #     (chart/values.yaml) — if that value is overridden, this Secret's
  #     issuer-url must be corrected manually (or re-seeded) to match.
  #   - Client id: the "trino" Keycloak client id is a literal in
  #     chart/templates/10-keycloak.yaml, not values-driven.
  # trino-client-secret is generated fresh (the one part of this Secret
  # bootstrap actually controls end-to-end).
  bootstrap::create_secret_if_absent oidc-credentials "$namespace" "$dry_run" \
    "--from-literal=issuer-url=https://keycloak-service.${namespace}.svc:8443/realms/lakehouse" \
    "--from-literal=trino-client-id=trino" \
    "--from-literal=trino-client-secret=$(bootstrap::rand_alnum 32)"

  # --- keycloak-admin-credentials ---
  bootstrap::create_secret_if_absent keycloak-admin-credentials "$namespace" "$dry_run" \
    "--from-literal=username=admin" \
    "--from-literal=password=$(bootstrap::rand_alnum 24)"

  # --- trino-internal-secret ---
  bootstrap::create_secret_if_absent trino-internal-secret "$namespace" "$dry_run" \
    "--from-literal=shared-secret=$(bootstrap::rand_alnum 32)"

  bootstrap::print_source_db_note
}

# bootstrap::print_source_db_note — pg/mssql/mongo are customer database
# credentials, created when a source is onboarded (Console), never
# generated by bootstrap.
bootstrap::print_source_db_note() {
  echo "  NOTE: source-database Secrets (pg / mssql / mongo) are NOT generated by bootstrap — they are customer database"
  echo "        credentials created when a source is onboarded (see the Console's add-source flow)."
}

# bootstrap::print_external_inventory — secrets-mode=external guidance.
bootstrap::print_external_inventory() {
  echo "  mode=external: bootstrap creates/verifies NO secret material. An ExternalSecrets Operator + SecretStore/"
  echo "  ClusterSecretStore (secrets.external.secretStoreName in chart/values.yaml) must already exist out-of-band;"
  echo "  the chart itself renders one ExternalSecret per named Secret (chart/templates/15-external-secrets.yaml)."
  bootstrap::print_source_db_note
}

# bootstrap::print_manual_inventory — secrets-mode=manual guidance (mirrors
# chart/templates/NOTES.txt's manual-mode wording, single source of truth
# there; this is the pre-install-time echo of the same inventory).
bootstrap::print_manual_inventory() {
  local namespace="$1"
  echo "  mode=manual: create each Secret by hand before installing the chart, e.g.:"
  echo "    kubectl -n ${namespace} create secret generic s3-credentials \\"
  echo "      --from-literal=access-key-id=... --from-literal=secret-access-key=... \\"
  echo "      --from-literal=endpoint=... --from-literal=region=..."
  echo "    kubectl -n ${namespace} create secret generic oidc-credentials \\"
  echo "      --from-literal=issuer-url=... --from-literal=trino-client-id=... --from-literal=trino-client-secret=..."
  echo "    kubectl -n ${namespace} create secret generic keycloak-admin-credentials \\"
  echo "      --from-literal=username=admin --from-literal=password=..."
  echo "    kubectl -n ${namespace} create secret generic trino-internal-secret --from-literal=shared-secret=..."
  bootstrap::print_source_db_note
}
