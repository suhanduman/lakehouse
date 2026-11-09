"""check.py <shop_rows> <crm_rows> <shop_rows_user>
Trino'ya küme içinden: (1) servis hesabı (Basic) sayımlar + sandbox yazma reddi; (2) analist (Keycloak jetonu -> Bearer)
tam okuma, nginx remote maskesiz, sandbox'a CTAS + DROP; (3) son kullanıcı: shop.orders satır filtresi, remote maskesi
'x.x.x.x', sandbox yazma reddi.

Kimlikler ortamdan (test/e2e/lib.sh, Secret e2e-acceptance): kind'da geliştirme kullanıcıları (analyst1/user1) ve
parola akışı; üretimde AD test hesapları ve tarayıcı akışı (ACC_FLOW=authcode). Servis hesabı ACC_SA: kind'da e2e,
üretimde zeppelin (aynı yetki). Parolası trino-service-accounts Secret'ından SA_<ad> olarak gelir."""
import os
import re
import sys

import requests
import trino
from trino.auth import BasicAuthentication, JWTAuthentication

HOST, CA = os.environ["TRINO_HOST"], os.environ["CA_FILE"]
shop_rows, crm_rows, shop_rows_user = (int(a) for a in sys.argv[1:4])
SA = os.environ.get("ACC_SA", "e2e")
ANALYST = (os.environ.get("ACC_ANALYST_USER", "analyst1"), os.environ.get("ACC_ANALYST_PASS", "analyst1-dev"))
USER = (os.environ.get("ACC_USER_USER", "user1"), os.environ.get("ACC_USER_PASS", "user1-dev"))
KC_INTERNAL = "http://keycloak-service.lakehouse.svc:8080"


def conn(auth):
    return trino.dbapi.connect(host=HOST, port=8443, http_scheme="https", verify=CA, auth=auth, catalog="lakehouse", schema="shop")


def q(c, sql):
    cur = c.cursor()
    cur.execute(sql)
    return cur.fetchall()


def _authcode(user, password):
    """Üretim: Keycloak parola akışı kapalı -> tarayıcının yaptığını küme içinden yap (giriş formu + kod).
    Form adresi Keycloak'ın DIŞ adresidir; aynı yol iç servise gönderilir (çerezler Secure değil — canlı doğrulandı).
    Jetonun iss'i dış adres kalır, Trino'nun beklediği budur."""
    import html
    import urllib.parse as up
    s, red = requests.Session(), os.environ["TRINO_REDIRECT"]
    base = KC_INTERNAL + "/realms/lakehouse/protocol/openid-connect"
    r = s.get(base + "/auth", timeout=30, params={"client_id": "trino", "redirect_uri": red,
                                                  "response_type": "code", "scope": "openid"})
    m = re.search(r'<form[^>]+id="kc-form-login"[^>]+action="([^"]+)"', r.text)
    if not m:
        print(f"HATA Keycloak giriş formu yok (HTTP {r.status_code})")
        sys.exit(1)
    p = up.urlsplit(html.unescape(m.group(1)))
    r = s.post(up.urlunsplit(("http", up.urlsplit(KC_INTERNAL).netloc, p.path, p.query, "")), allow_redirects=False,
               timeout=30, data={"username": user, "password": password, "credentialId": ""})
    code = up.parse_qs(up.urlsplit(r.headers.get("Location", "")).query).get("code", [None])[0]
    if not code:
        print(f"HATA {user}: Keycloak girişi reddedildi (HTTP {r.status_code}; parola ya da AD grubu?)")
        sys.exit(1)
    t = s.post(base + "/token", timeout=30, data={"grant_type": "authorization_code", "code": code, "redirect_uri": red,
                                                  "client_id": "trino", "client_secret": os.environ["KC_CLIENT_SECRET"]})
    t.raise_for_status()
    return t.json()["access_token"]


def token(user, password):
    if os.environ.get("ACC_FLOW", "password") == "authcode":
        return _authcode(user, password)
    r = requests.post(os.environ["KC_TOKEN_URL"], timeout=30, data={
        "grant_type": "password", "client_id": "trino", "client_secret": os.environ["KC_CLIENT_SECRET"],
        "username": user, "password": password, "scope": "openid"})
    r.raise_for_status()
    return r.json()["access_token"]


def expect(cond, msg):
    if not cond:
        print("HATA", msg)
        sys.exit(1)
    print("OK", msg)


def expect_denied(c, sql, msg):
    try:
        q(c, sql)
    except trino.exceptions.TrinoUserError as e:
        expect("Access Denied" in str(e), f"{msg} ({str(e)[:80]})")
        return
    print("HATA", msg, "-> reddedilmedi")
    sys.exit(1)


svc = conn(BasicAuthentication(SA, os.environ["SA_" + SA]))
expect(q(svc, "select count(*) from shop.orders")[0][0] == shop_rows, f"{SA} shop.orders == {shop_rows}")
expect(q(svc, "select count(*) from crm.customers")[0][0] == crm_rows, f"{SA} crm.customers == {crm_rows}")
expect(q(svc, "select count(*) from nginx_raw.access_log")[0][0] >= 3, f"{SA} nginx_raw.access_log >= 3")
expect_denied(svc, "create table sandbox.e2e_svc as select 1 x", f"{SA} servis hesabı sandbox yazma reddi")
# Tablo düzeyi veri metrikleri (spec §8, docs/50-isletme/veri-metrikleri.md): Iceberg metadata tabloları Trino'dan
# okunabilir olmalı — dashboard'un (glue/files/superset/iceberg-metadata) TEK veri kaynağı budur.
# MERGE #1/#2 sonrası shop.orders en az 2 snapshot taşır.
snaps = q(svc, 'select count(*) from shop."orders$snapshots"')[0][0]
expect(snaps >= 2, f"snapshots shop.orders {snaps} >= 2 (MERGE sonrası; metadata tablosu okunabilir)")

an = conn(JWTAuthentication(token(*ANALYST)))
expect(q(an, "select count(*) from shop.orders")[0][0] == shop_rows, f"{ANALYST[0]} (analist) shop.orders filtresiz")
remote = q(an, "select remote from nginx_raw.access_log limit 1")[0][0]
expect(re.fullmatch(r"\d+\.\d+\.\d+\.\d+", remote or "") is not None, f"{ANALYST[0]} remote maskesiz ({remote})")
q(an, "drop table if exists sandbox.e2e_orders")
q(an, "create table sandbox.e2e_orders as select * from shop.orders")
expect(q(an, "select count(*) from sandbox.e2e_orders")[0][0] == shop_rows,
       f"{ANALYST[0]} sandbox CTAS (Polaris sandbox_writers + Trino owner)")
q(an, "drop table sandbox.e2e_orders")
print(f"OK {ANALYST[0]} sandbox DROP")

usr = conn(JWTAuthentication(token(*USER)))
expect(q(usr, "select count(*) from shop.orders")[0][0] == shop_rows_user,
       f"{USER[0]} (son kullanıcı) satır filtresi status <> 'shipped' -> {shop_rows_user}")
expect(q(usr, "select remote from nginx_raw.access_log limit 1")[0][0] == "x.x.x.x", f"{USER[0]} remote maskesi")
expect_denied(usr, "create table sandbox.e2e_user as select 1 x", f"{USER[0]} sandbox yazma reddi")
print("OK TRINO_OK")
