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
import json, os, re, sys, yaml

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
# Ad alanı adı üründe SABİTTİR (docs/30-kurulum.md §4.2): site/glue.yaml'a `namespace` YAZILMAZ.
# Tek başına değiştirilirse kurulum iki ad alanına bölünür, bu yüzden varlığı HATAdır.
if "namespace" in g:
    errs.append("site/glue.yaml: `namespace` anahtarı bulunmamalı — ad alanı adı üründe sabittir "
                "(lakehouse). Satırı silin: docs/90-referans/values-anahtarlari.md §1 ('Eklemeyin') "
                "ve docs/30-kurulum.md §4.2.")
ns = "lakehouse"
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
# KURAL: denetlenen çift `caSecret` <-> `ldap.ssl.truststore.path` SATIRIDIR; trino-ldap.yaml'daki mount
# DENETLENMEZ çünkü optional: true'dur (Secret yoksa Trino yine açılır, mount sessizce boş kalır).
# Biri açık, öteki kapalı bırakılırsa Trino ya PKIX hatası verir ya da olmayan bir dosyayı gösterir.
ca = get(g, "keycloak.ldap.caSecret")
ts = "ldap.ssl.truststore.path=/etc/trino/ad-ca/ca.crt"
if ca and ts not in t:
    errs.append(f"site/trino.yaml: keycloak.ldap.caSecret='{ca}' iken group-provider bloğunda '{ts}' bulunmalı")
if not ca and ts in t:
    errs.append(f"site/trino.yaml: keycloak.ldap.caSecret boşken '{ts}' satırı kalmamalı (ad-ca bağlanmıyor)")

# 3c) site/trino.yaml accessControl.rules."rules.json" AÇIKÇA verildiyse (kullanici-ve-yetki.md §5.2),
# Helm bloğu bütünüyle ezdiği için ürünün servis hesabı satırları da orada olmak ZORUNDADIR. Düşerse
# Superset panoları ve Zeppelin not defterleri Trino'dan sessizce `Access Denied` alır.
# `e2e` yalnız test/e2e koşusunun kullandığı hesaptır; ÜRETİM site dosyasında bulunması gerekmez, bu
# yüzden kural `|e2e` ekini İSTEĞE BAĞLI kabul eder (varsa da hata değildir).
tr_site = yaml.safe_load(open("platform/values/site/trino.yaml")) or {}
site_rules = get(tr_site, "accessControl.rules") or {}
rules_json = site_rules.get("rules.json") if isinstance(site_rules, dict) else None
# Denetim METİN üzerinde değil, AYRIŞTIRILMIŞ JSON üzerinde yapılır: JSON nesnesinde anahtar
# sırası anlamsızdır ("privileges" önce, "user" sonra yazılabilir) ve metin eşleme böyle bir
# dosyada kuralı var sayıp sessizce KAÇIRIRDI.
if rules_json:
    try:
        rules = json.loads(rules_json)
    except ValueError as exc:
        rules = None
        errs.append(f'site/trino.yaml: accessControl.rules."rules.json" geçerli JSON değil: {exc}')
    svc = ("superset|zeppelin", "superset|zeppelin|e2e")

    def has_rule(section, fields):
        for r in (rules or {}).get(section) or []:
            if isinstance(r, dict) and r.get("user") in svc and all(r.get(k) == v for k, v in fields.items()):
                return True
        return False

    if isinstance(rules, dict):
        for section, fields, shown in [
            ("catalogs", {"catalog": "lakehouse|system", "allow": "read-only"},
             '{"user": "superset|zeppelin|e2e", "catalog": "lakehouse|system", "allow": "read-only"}'),
            ("tables", {"privileges": ["SELECT"]},
             '{"user": "superset|zeppelin|e2e", "privileges": ["SELECT"]}')]:
            if not has_rule(section, fields):
                errs.append(f'site/trino.yaml: ürün satırı eksik -> {shown} '
                            f'(accessControl.rules."rules.json" -> "{section}" listesinde bulunmalı)')

# 4) Polaris sunucusunun kendi endpoint'i ayrı bir dosyada ve dev'de MinIO'yu gösterir (test/e2e) -> UYARI
if s3 and s3 not in p:
    warns.append(f"platform/polaris/setup.yaml: endpoint '{s3}' değil — Polaris sunucusu için ayrıca düzenlenir (docs/30-kurulum.md)")

# 5) Yedek bucket'ı veri bucket'ından AYRI olmalıdır: geri yükleme hedefi ile kaynağı aynı olamaz.
# Veri bucket'ı site değerlerinde yoktur; platform/polaris/setup.yaml'daki default_base_location'tadır.
# Değer tırnaklı yazılabilir ('s3://...' / "s3://..."): tırnak desene dâhil edilmezse eşleşme
# sessizce KAÇARDI ve yedek/veri bucket'ı aynı olsa bile kural hiç işlemezdi.
m = re.search(r"""^\s*default_base_location:\s*(?P<q>['"]?)s3://(?P<b>[^/'"\s]+)""", p, re.M)
data_bucket = m.group("b") if m else None
backup_bucket = str(get(g, "backup.s3.bucket") or "").strip()
if data_bucket and backup_bucket and data_bucket == backup_bucket:
    errs.append(f"site/glue.yaml: backup.s3.bucket '{backup_bucket}' veri bucket'ı ile AYNI "
                f"(platform/polaris/setup.yaml default_base_location: s3://{data_bucket}/) — "
                f"yedek bucket'ı ayrı olmalıdır")

# 6) site/glue.yaml argocdNamespace, kurulumun ARGOCD_NS'i ile ÇAPRAZ denetlenir: NetworkPolicy
# seçicisi argocdNamespace'ten üretilir (bkz. yukarıdaki `namespace` notu ve values-anahtarlari.md
# §1); uyuşmazsa seçici hiçbir ad alanıyla eşleşmez. Kaynak sırası: install/lakehouse.env varsa
# (müşterinin gerçek dosyası, .gitignore'lu) o, yoksa install/lakehouse.env.example (izlenen
# şablon) kullanılır. Değişken env dosyasında yoksa UYARI (HATA değil): dosya eksik olabilir ama
# bu denetim canlı kümedeki gerçek değeri göremez.
env_path = "install/lakehouse.env" if os.path.exists("install/lakehouse.env") else "install/lakehouse.env.example"
env_argocd_ns = None
env_match = re.search(r"^ARGOCD_NS=(.*)$", open(env_path).read(), re.M)
if env_match:
    raw = re.sub(r"\s*#.*$", "", env_match.group(1)).strip()
    env_argocd_ns = raw.strip("'\"")
if not env_argocd_ns:
    warns.append(f"{env_path}: ARGOCD_NS tanımlı değil — site/glue.yaml argocdNamespace ile "
                 f"çapraz denetlenemedi")
else:
    site_argocd_ns = get(g, "argocdNamespace")
    if site_argocd_ns is None:
        chart_defaults = yaml.safe_load(open("glue/values.yaml")) or {}
        site_argocd_ns = get(chart_defaults, "argocdNamespace") or "argocd"
    if str(site_argocd_ns) != env_argocd_ns:
        errs.append(f"site/glue.yaml: argocdNamespace='{site_argocd_ns}' {env_path} "
                    f"ARGOCD_NS='{env_argocd_ns}' ile uyuşmuyor (docs/90-referans/"
                    f"values-anahtarlari.md §1 'argocdNamespace' satırı, pre-ship 1.12)")

for w in warns:
    print("UYARI:", w)
for e in errs:
    print("HATA:", e)
print("check-site: OK" if not errs else f"check-site: {len(errs)} hata")
sys.exit(1 if errs else 0)
PY
