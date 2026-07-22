# Lakehouse Console frontend -- deploy image.
#
# Multi-stage build: compile the Vite/React static bundle with Node, then
# serve it with an unprivileged nginx image. nginxinc/nginx-unprivileged is
# OpenShift-friendly -- it runs as a non-root, arbitrary UID and listens on
# an unprivileged port (8080) out of the box, so it works under OpenShift's
# default "restricted" SCC without extra `runAsUser`/`anyuid` grants.
#
# Not built in this environment (docker unavailable). Validated instead via
# `npm --prefix console/frontend run build`, which exercises the exact
# `npm ci && npm run build` steps used in the build stage below and produces
# console/frontend/dist/ -- see console/frontend/vite.config.ts and
# package.json for the build script.

# ---- Stage 1: build the static assets --------------------------------
FROM node:22-alpine AS build

WORKDIR /srv/app

# Copy manifests first so `npm ci` is cached separately from source changes.
COPY package.json package-lock.json ./
RUN npm ci

COPY . .
RUN npm run build

# ---- Stage 2: serve the static assets with nginx ----------------------
FROM nginxinc/nginx-unprivileged:1.27-alpine

COPY --from=build /srv/app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 8080
