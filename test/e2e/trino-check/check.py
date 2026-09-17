"""check.py <shop_rows> <crm_rows> <shop_rows_student>
Trino'ya küme içinden: (1) servis hesabı e2e (Basic) sayımlar + sandbox yazma reddi; (2) analyst1 (Keycloak password grant -> Bearer)
tam okuma, nginx remote maskesiz, sandbox'a CTAS + DROP; (3) student1: shop.orders satır filtresi, remote maskesi 'x.x.x.x', sandbox yazma reddi."""
import os
import re
import sys

import requests
import trino
from trino.auth import BasicAuthentication, JWTAuthentication

HOST, CA = os.environ["TRINO_HOST"], os.environ["CA_FILE"]
shop_rows, crm_rows, shop_rows_student = (int(a) for a in sys.argv[1:4])


def conn(auth):
    return trino.dbapi.connect(host=HOST, port=8443, http_scheme="https", verify=CA, auth=auth, catalog="lakehouse", schema="shop")


def q(c, sql):
    cur = c.cursor()
    cur.execute(sql)
    return cur.fetchall()


def token(user, password):
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


svc = conn(BasicAuthentication("e2e", os.environ["E2E_PASSWORD"]))
expect(q(svc, "select count(*) from shop.orders")[0][0] == shop_rows, f"e2e shop.orders == {shop_rows}")
expect(q(svc, "select count(*) from crm.customers")[0][0] == crm_rows, f"e2e crm.customers == {crm_rows}")
expect(q(svc, "select count(*) from nginx_raw.access_log")[0][0] >= 3, "e2e nginx_raw.access_log >= 3")
expect_denied(svc, "create table sandbox.e2e_svc as select 1 x", "e2e servis hesabı sandbox yazma reddi")

an = conn(JWTAuthentication(token("analyst1", "analyst1-dev")))
expect(q(an, "select count(*) from shop.orders")[0][0] == shop_rows, "analyst1 shop.orders filtresiz")
remote = q(an, "select remote from nginx_raw.access_log limit 1")[0][0]
expect(re.fullmatch(r"\d+\.\d+\.\d+\.\d+", remote or "") is not None, f"analyst1 remote maskesiz ({remote})")
q(an, "drop table if exists sandbox.e2e_orders")
q(an, "create table sandbox.e2e_orders as select * from shop.orders")
expect(q(an, "select count(*) from sandbox.e2e_orders")[0][0] == shop_rows, "analyst1 sandbox CTAS (Polaris sandbox_writers + Trino owner)")
q(an, "drop table sandbox.e2e_orders")
print("OK analyst1 sandbox DROP")

st = conn(JWTAuthentication(token("student1", "student1-dev")))
expect(q(st, "select count(*) from shop.orders")[0][0] == shop_rows_student, f"student1 satır filtresi status <> 'shipped' -> {shop_rows_student}")
expect(q(st, "select remote from nginx_raw.access_log limit 1")[0][0] == "x.x.x.x", "student1 remote maskesi")
expect_denied(st, "create table sandbox.e2e_student as select 1 x", "student1 sandbox yazma reddi")
print("OK TRINO_OK")
