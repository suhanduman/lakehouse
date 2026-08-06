# Internally-built JupyterHub Hub image (hand-rolled JupyterHub, per-user
# notebook sandbox — chart/templates/23-jupyterhub.yaml). Runs ONLY the Hub
# process. The KubeSpawner it bakes tells the Hub to spawn per-user
# singleuser pods from the SEPARATE `images/jupyter` image — this image is
# NEVER used as the notebook/singleuser image.
#
# NO config is baked here. `jupyterhub_config.py` is supplied at runtime via
# a mounted ConfigMap (wired by a later task's Deployment) and passed with
# `--config=<path>` in the container's `args` — this Dockerfile only pins the
# Python packages the Hub process needs to import that config.
#
# VERSION MATRIX (all four resolved together; re-check PyPI `requires_dist`
# before bumping any ONE independently — a later bump here must keep every
# constraint below satisfied):
#   jupyterhub              5.5.0  -- Hub core
#   jupyterhub-kubespawner  7.1.0  -- KubeSpawner (requires jupyterhub>=4.0.0)
#   oauthenticator          17.4.0 -- Keycloak OIDC via GenericOAuthenticator
#                                     (requires jupyterhub>=2.2, python>=3.10)
#   jupyterhub-idle-culler  2.0.0  -- OPTIONAL idle-server-reaper Hub service
#                                     (requires python-dateutil; no hard
#                                     jupyterhub floor, tested against 5.x)
# python:3.11-slim satisfies oauthenticator's python>=3.10 floor.
#
# Build (from repo root):
#   podman build -t <registry>/jupyterhub:<tag> images/jupyterhub
FROM python:3.11-slim

ARG JUPYTERHUB_VERSION=5.5.0
ARG KUBESPAWNER_VERSION=7.1.0
ARG OAUTHENTICATOR_VERSION=17.4.0
ARG IDLE_CULLER_VERSION=2.0.0

RUN pip install --no-cache-dir \
      "jupyterhub==${JUPYTERHUB_VERSION}" \
      "jupyterhub-kubespawner==${KUBESPAWNER_VERSION}" \
      "oauthenticator==${OAUTHENTICATOR_VERSION}" \
      "jupyterhub-idle-culler==${IDLE_CULLER_VERSION}"

# Non-root: a dedicated, fixed-UID user owning its own home (the Hub's
# default sqlite state db / cookie-secret file live under CWD unless a later
# task's Deployment mounts a PVC and points `--config`/`--db-url` elsewhere).
RUN useradd -m -u 1001 jupyterhub
USER jupyterhub
WORKDIR /home/jupyterhub

ENTRYPOINT ["jupyterhub"]
