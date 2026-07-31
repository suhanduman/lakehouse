"""FastAPI dependencies: OIDC bearer-token -> claims -> roles, an authz gate
that runs *before* any expensive provider is constructed, and the
service-provider dependencies (`K8sService`/`S3Service`/`TrinoService`/
`AddSourceOrchestrator`) the routers need.

`get_roles` is the only piece here that talks to a real IdP: it extracts
the `Authorization: Bearer <token>` header, verifies the token's signature
against the configured OIDC provider's JWKS (`app.config.settings.
oidc_issuer`/`oidc_audience`/`oidc_algorithms`), and maps the resulting
claims to internal `Role`s via `app.services.authz.roles_from_claims`.
Routers only ever depend on `get_roles` / `require_action` (never touch
tokens/JWKS themselves), so tests fully replace `get_roles` with
`app.dependency_overrides[get_roles] = lambda: {Role.ANALYST}` -- no token,
no network, no real IdP required.

`require_action(action)` / `require_delete_mode` are *authz gates*: used as
route-decorator `dependencies=[...]`, FastAPI resolves them BEFORE any
body-parameter provider dependency (they're inserted at the front of the
dependant list), so an unauthorized caller gets 403 *without* ever building
a K8s/S3/Trino client (verified by test).

`get_k8s`/`get_s3`/`get_trino`/`get_orchestrator` build the *real* clients
for production and are each overridable in tests.
"""

from __future__ import annotations

import os
import time
from typing import Dict, Optional, Set, Tuple

import httpx
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import jwt
from jose.exceptions import JOSEError
from kubernetes import client as k8s_client
from kubernetes import config as k8s_config

from app.config import settings
from app.models import DeleteMode
from app.orchestrator import AddSourceOrchestrator
from app.services import render_service
from app.services.authz import Action, Role, can, roles_from_claims
from app.services.connect_service import ApicurioClient, ConnectService
from app.services.iceberg_service import IcebergService
from app.services.k8s_service import K8sService
from app.services.s3_service import S3Service
from app.services.trino_service import TrinoService

_bearer_scheme = HTTPBearer(auto_error=False)


# --------------------------------------------------------------------------
# JWKS cache (TTL + kid-miss refetch) -- avoids fetching discovery+JWKS on
# every single request while still picking up IdP key rotation promptly.
# --------------------------------------------------------------------------

_JWKS_TTL_SECONDS = 3600
# issuer -> (expires_at_epoch, jwks_dict)
_JWKS_CACHE: Dict[str, Tuple[float, dict]] = {}


def _fetch_jwks(issuer: str, force: bool = False) -> dict:
    """OIDC discovery document -> JWKS, cached per-issuer with a TTL. `force`
    bypasses the cache (used on a `kid` cache-miss, i.e. after key rotation).
    Only ever hit on the real (non-overridden) path -- tests never reach
    this, since they override `get_roles` itself."""
    now = time.time()
    cached = _JWKS_CACHE.get(issuer)
    if cached is not None and not force and cached[0] > now:
        return cached[1]
    discovery = httpx.get(f"{issuer.rstrip('/')}/.well-known/openid-configuration", timeout=5.0)
    discovery.raise_for_status()
    jwks_uri = discovery.json()["jwks_uri"]
    jwks_response = httpx.get(jwks_uri, timeout=5.0)
    jwks_response.raise_for_status()
    jwks = jwks_response.json()
    _JWKS_CACHE[issuer] = (now + _JWKS_TTL_SECONDS, jwks)
    return jwks


def _verify_and_decode(token: str) -> dict:
    """Verify `token`'s signature/issuer/audience against the configured
    OIDC provider (using the explicit `oidc_algorithms` allowlist -- the
    token's own `alg` header is never trusted) and return its claims dict.

    Any verification failure -- bad/`none` alg (`JWKError`/`JWTError`, both
    `JOSEError` subclasses), expired, wrong audience/issuer, malformed
    token, or IdP unreachable -- becomes a 401 (never a 500).
    """
    try:
        jwks = _fetch_jwks(settings.oidc_issuer)
        # kid cache-miss -> the IdP likely rotated keys; refetch once.
        kid = jwt.get_unverified_header(token).get("kid")
        if kid and not any(key.get("kid") == kid for key in jwks.get("keys", [])):
            jwks = _fetch_jwks(settings.oidc_issuer, force=True)
        claims: dict = jwt.decode(
            token,
            jwks,
            algorithms=settings.oidc_algorithms,
            audience=settings.oidc_audience,
            issuer=settings.oidc_issuer,
        )
    except (JOSEError, httpx.HTTPError, KeyError, ValueError) as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"invalid or unverifiable token: {exc}",
        ) from exc
    return claims


def get_roles(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(_bearer_scheme),
) -> Set[Role]:
    """FastAPI dependency: verified OIDC bearer token -> `set[Role]`.

    Overridable in tests via
    `app.dependency_overrides[get_roles] = lambda: {Role.ANALYST}`.
    """
    if credentials is None or not credentials.credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="missing bearer token",
        )
    claims = _verify_and_decode(credentials.credentials)
    return roles_from_claims(claims)


# --------------------------------------------------------------------------
# Authz gates -- meant for route-decorator `dependencies=[...]`, so they run
# BEFORE any body-parameter provider dependency is constructed.
# --------------------------------------------------------------------------

def _forbid(action: Action) -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail=f"forbidden: role set does not grant {action.value}",
    )


def require_action(action: Action):
    """Return a dependency that 403s unless the caller's roles grant `action`.

    Use as `@router.post(..., dependencies=[Depends(require_action(Action.X))])`
    so the gate runs before providers are built.
    """

    def _dep(roles: Set[Role] = Depends(get_roles)) -> Set[Role]:
        if not can(roles, action):
            raise _forbid(action)
        return roles

    return _dep


def require_delete_mode(
    mode: DeleteMode = "pipeline_only",
    roles: Set[Role] = Depends(get_roles),
) -> Set[Role]:
    """DELETE gate: `with_data` needs `SOURCE_DELETE_WITH_DATA` (admin-only),
    `pipeline_only` needs the weaker `SOURCE_DELETE_PIPELINE`. Runs before
    providers are built."""
    action = (
        Action.SOURCE_DELETE_WITH_DATA if mode == "with_data" else Action.SOURCE_DELETE_PIPELINE
    )
    if not can(roles, action):
        raise _forbid(action)
    return roles


# --------------------------------------------------------------------------
# Service providers (all overridable in tests)
# --------------------------------------------------------------------------

def get_k8s() -> K8sService:
    """Real `K8sService`, built from in-cluster config (falls back to the
    local kubeconfig for out-of-cluster/dev use). Overridable in tests via
    `app.dependency_overrides[get_k8s]`."""
    try:
        k8s_config.load_incluster_config()
    except k8s_config.ConfigException:
        k8s_config.load_kube_config()
    return K8sService(
        custom_api=k8s_client.CustomObjectsApi(),
        core_api=k8s_client.CoreV1Api(),
        namespace=settings.namespace,
    )


def get_s3() -> S3Service:
    """Real `S3Service` over a boto3 S3 client (MinIO-compatible via
    `settings.s3_endpoint`). Overridable in tests via
    `app.dependency_overrides[get_s3]`."""
    import boto3

    return S3Service(client=boto3.client("s3", endpoint_url=settings.s3_endpoint or None))


def get_trino() -> TrinoService:
    """Real `TrinoService` (connection opened lazily per call). Overridable
    in tests via `app.dependency_overrides[get_trino]`."""
    import trino.dbapi

    return TrinoService(
        conn_factory=lambda: trino.dbapi.connect(
            host=settings.trino_host, port=settings.trino_port, user=settings.trino_user
        )
    )


def get_iceberg() -> IcebergService:
    """Real `IcebergService` (pyiceberg RestCatalog over Nessie, built lazily
    per call, warehouse-aware). CDC tablo pre-create bunu kullanır (Trino DDL
    DEĞİL — Trino identifier field set edemez). Overridable in tests via
    `app.dependency_overrides[get_iceberg]` (fake, pyiceberg gerekmez).

    `warehouse` construction property, tools/create_iceberg_table.py
    `_load_catalog`'ı birebir yansıtır: verildiğinde (bronze -> "rawdata")
    RestCatalog o Nessie warehouse'a bağlanır (S3 konumunu bu belirler);
    None ise (silver) prop hiç eklenmez -> Nessie default warehouse'u
    kullanılır. Bir "warehouse" namespace property'si stamplemek Nessie
    tarafından yok sayılır -- bu YALNIZCA construction-time prop olarak
    çalışır."""

    def _catalog(warehouse: str | None = None):
        from pyiceberg.catalog.rest import RestCatalog

        props = {
            "uri": settings.nessie_uri,
            "s3.endpoint": settings.s3_endpoint,
            "s3.access-key-id": settings.s3_access_key,
            "s3.secret-access-key": settings.s3_secret_key,
            "s3.path-style-access": "true",
        }
        if warehouse:
            props["warehouse"] = warehouse
        return RestCatalog("nessie", **props)

    return IcebergService(catalog_factory=_catalog)


def get_git_writer():
    """GitWriter from settings.gitops_* + the mounted credential Secret. None
    when deploy_mode != gitops. Credential files are mounted by the chart at
    /var/run/gitops (ssh-privatekey [+ known_hosts])."""
    if settings.deploy_mode != "gitops":
        return None
    from app.services.git_writer import GitCredential, GitWriter

    if not settings.gitops_repo_url:
        raise RuntimeError("deploy_mode=gitops but GITOPS_REPO_URL is empty (fail-loud)")
    key, kh = "/var/run/gitops/ssh-privatekey", "/var/run/gitops/known_hosts"
    cred = GitCredential(
        ssh_key_path=key if os.path.exists(key) else None,
        known_hosts_path=kh if os.path.exists(kh) else None,
    )
    return GitWriter(settings.gitops_repo_url, settings.gitops_branch, settings.gitops_path, cred)


def get_orchestrator(
    k8s: K8sService = Depends(get_k8s),
    s3: S3Service = Depends(get_s3),
    trino: TrinoService = Depends(get_trino),
    iceberg: IcebergService = Depends(get_iceberg),
    git_writer=Depends(get_git_writer),
) -> AddSourceOrchestrator:
    """Real `AddSourceOrchestrator`, wired to the real K8s/S3/Trino/Iceberg
    providers above plus the `render_service` module. `iceberg` drives the CDC
    table pre-create step (identifier field). `spark_image`/`s3_secret_name`
    (from `settings`) drive the spark-batch lane's single spark-job step
    (Plan B2). `deploy_mode`/`git_writer` (Task 4) route add_source/
    edit_spark_source through GitWriter instead of the cluster when
    `settings.deploy_mode == "gitops"`. Overridable in tests via
    `app.dependency_overrides[get_orchestrator]`."""
    return AddSourceOrchestrator(
        k8s, s3, trino, render_service, iceberg=iceberg,
        spark_image=settings.spark_image, s3_secret_name=settings.s3_secret_name,
        deploy_mode=settings.deploy_mode, git_writer=git_writer,
    )


def get_connect() -> ConnectService:
    """Real `ConnectService` over an `httpx.Client`, pointed at
    `settings.connect_url` (the Kafka Connect REST API). Overridable in
    tests via `app.dependency_overrides[get_connect]`."""
    return ConnectService(settings.connect_url, httpx.Client())


def get_apicurio() -> ApicurioClient:
    """Real `ApicurioClient` over an `httpx.Client`, pointed at
    `settings.apicurio_url` (the Apicurio Registry v2 REST API). Overridable
    in tests via `app.dependency_overrides[get_apicurio]`."""
    return ApicurioClient(settings.apicurio_url, httpx.Client())
