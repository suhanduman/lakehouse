#!/usr/bin/env bash
# Site değerlerinin tutarlılık kontrolü (docs/30-kurulum.md §2). Çıkış 0 = tutarlı, 1 = hata.
# platform/values/site/{glue,trino,jupyterhub}.yaml müşterinin düzenlediği TEK yerdir; bu üç dosya
# birbirinin kopyası olan değerleri (Keycloak URL'i, JupyterHub callback'i, LDAP, S3 endpoint) taşır.
# Ayrıca Helm'in çok satırlı string blokları bütünüyle ezmesi nedeniyle site/trino.yaml'da bulunması
# ZORUNLU ürün satırlarını da denetler (blok kopyalanırken satır düşerse Trino sessizce yanlış açılır).
# Gereksinim: python3 + PyYAML.
set -euo pipefail
cd "$(dirname "$0")/.."
python3 - <<'PY'
import sys, yaml

def dump(path):
    return yaml.safe_dump(yaml.safe_load(open(path)) or {}, width=10**9, allow_unicode=True)

g = yaml.safe_load(open("platform/values/site/glue.yaml")) or {}
t = dump("platform/values/site/trino.yaml")
j = dump("platform/values/site/jupyterhub.yaml")
p = open("platform/polaris/setup.yaml").read()
errs, warns = [], []

def get(d, path):
    cur = d
    for k in path.split("."):
        if not isinstance(cur, dict) or k not in cur:
            return None
        cur = cur[k]
    return cur

# 1) site/glue.yaml: zorunlu alanlar dolu mu
for k in ["appsDomain", "s3.endpoint", "connect.buildImage", "keycloak.ldap.connectionUrl", "keycloak.ldap.usersDn",
          "keycloak.ldap.groupsDn", "keycloak.ldap.bindDn", "backup.s3.endpoint", "backup.s3.bucket"]:
    if get(g, k) in (None, ""):
        errs.append(f"site/glue.yaml: {k} boş/eksik")
if not str(get(g, "s3.endpoint") or "").startswith("https://"):
    errs.append("site/glue.yaml: s3.endpoint https:// olmalı")
kc_explicit = get(g, "keycloak.hostname")
if kc_explicit and not str(kc_explicit).startswith("https://"):
    errs.append(f"site/glue.yaml: keycloak.hostname AÇIK verildiyse TAM URL olmalı (https://...), verilen: {kc_explicit}")
if get(g, "keycloak.ldap.enabled") is not True:
    errs.append("site/glue.yaml: keycloak.ldap.enabled true olmalı (AD gün-1 zorunlu)")
if get(g, "connect.buildImage") and str(get(g, "connect.buildImage")).endswith(":latest"):
    errs.append("site/glue.yaml: connect.buildImage :latest OLMAMALI (digest sabitlenemez)")

# 2) site/glue.yaml'dan türetilen değerler site/trino.yaml ve site/jupyterhub.yaml'da birebir geçmeli
apps = get(g, "appsDomain") or ""
ns = g.get("namespace") or "lakehouse"
kc = get(g, "keycloak.hostname") or (f"https://keycloak-{ns}.{apps}" if apps else "")
hub = get(g, "jupyterhub.hostname") or (f"jupyterhub-{ns}.{apps}" if apps else "")
realm = get(g, "keycloak.realm.name") or "lakehouse"
issuer = f"{kc}/realms/{realm}"
if kc and issuer not in t:
    errs.append(f"site/trino.yaml: oauth2 issuer '{issuer}' bulunamadı")
for key, path in [("ldap.url", "keycloak.ldap.connectionUrl"),
                  ("ldap.admin-user", "keycloak.ldap.bindDn"),
                  ("ldap.user-base-dn", "keycloak.ldap.usersDn")]:
    v = get(g, path) or ""
    if v and f"{key}={v}" not in t:
        errs.append(f"site/trino.yaml: {key}={v} bulunamadı (site/glue.yaml {path} ile aynı olmalı)")
s3 = get(g, "s3.endpoint") or ""
if s3 and f"s3.endpoint={s3}" not in t:
    errs.append(f"site/trino.yaml: lakehouse kataloğunda s3.endpoint={s3} bulunamadı")
region = get(g, "s3.region") or ""
if region and f"s3.region={region}" not in t:
    errs.append(f"site/trino.yaml: lakehouse kataloğunda s3.region={region} bulunamadı (site/glue.yaml s3.region ile aynı olmalı)")
vended = get(g, "s3.vendedCredentials")
if vended is not None and f"iceberg.rest-catalog.vended-credentials-enabled={str(vended).lower()}" not in t:
    errs.append(f"site/trino.yaml: iceberg.rest-catalog.vended-credentials-enabled={str(vended).lower()} bulunamadı (site/glue.yaml s3.vendedCredentials ile aynı olmalı)")
uri = f"iceberg.rest-catalog.uri=http://polaris.{ns}.svc:8181/api/catalog"
if uri not in t:
    errs.append(f"site/trino.yaml: lakehouse kataloğunda '{uri}' bulunamadı (namespace '{ns}')")
if hub and f"https://{hub}/hub/oauth_callback" not in j:
    errs.append(f"site/jupyterhub.yaml: oauth_callback_url https://{hub}/hub/oauth_callback bulunamadı")
if kc and kc not in j:
    errs.append("site/jupyterhub.yaml: Keycloak URL'leri site/glue.yaml ile uyuşmuyor")
if s3 and s3 not in j:
    errs.append(f"site/jupyterhub.yaml: singleuser.extraEnv.S3_ENDPOINT '{s3}' değil")

# 3) Helm çok satırlı string blokları BÜTÜNÜYLE ezer -> site/trino.yaml ürün satırlarını da taşımak zorunda
for line in ["http-server.authentication.oauth2.client-id=trino",
             "http-server.authentication.oauth2.client-secret=${ENV:OIDC_CLIENT_SECRET}",
             "http-server.authentication.oauth2.principal-field=preferred_username",
             "http-server.authentication.oauth2.scopes=openid",
             "web-ui.authentication.type=oauth2",
             "group-provider.name=ldap",
             "ldap.admin-password=${ENV:LDAP_BIND_PASSWORD}",
             "ldap.user-search-filter=(sAMAccountName={0})",
             "ldap.user-member-of-attribute=memberOf",
             "ldap.group-name-attribute=cn",
             "connector.name=iceberg",
             "iceberg.catalog.type=rest",
             "iceberg.rest-catalog.uri=",
             "iceberg.rest-catalog.warehouse=",
             "iceberg.rest-catalog.security=OAUTH2",
             "iceberg.rest-catalog.oauth2.credential=${ENV:POLARIS_CREDENTIAL}",
             "iceberg.rest-catalog.oauth2.scope=",
             "fs.s3.enabled=true",
             "s3.path-style-access=true"]:
    if line not in t:
        errs.append(f"site/trino.yaml: ürün satırı eksik -> {line}")

# 3b) AD kök CA'sı (ad-ca) üç tüketiciye TEK anahtardan açılır: glue'da keycloak.ldap.caSecret,
# Trino'da mount (../trino-ldap.yaml) + ldap.ssl.truststore.path (site/trino.yaml group-provider bloğu).
# Biri açık, öteki kapalı bırakılırsa Trino ya PKIX hatası verir ya da olmayan bir dosyayı gösterir.
ca = get(g, "keycloak.ldap.caSecret")
ts = "ldap.ssl.truststore.path=/etc/trino/ad-ca/ca.crt"
if ca and ts not in t:
    errs.append(f"site/trino.yaml: keycloak.ldap.caSecret='{ca}' iken group-provider bloğunda '{ts}' bulunmalı")
if not ca and ts in t:
    errs.append(f"site/trino.yaml: keycloak.ldap.caSecret boşken '{ts}' satırı kalmamalı (ad-ca bağlanmıyor)")

# 4) Polaris sunucusunun kendi endpoint'i ayrı bir dosyada ve dev'de MinIO'yu gösterir (test/e2e) -> UYARI
if s3 and s3 not in p:
    warns.append(f"platform/polaris/setup.yaml: endpoint '{s3}' değil — Polaris sunucusu için ayrıca düzenlenir (docs/30-kurulum.md)")

for w in warns:
    print("UYARI:", w)
for e in errs:
    print("HATA:", e)
print("check-site: OK" if not errs else f"check-site: {len(errs)} hata")
sys.exit(1 if errs else 0)
PY
