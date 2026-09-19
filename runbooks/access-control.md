# Yetkilendirme (gruplar, Trino rules.json, Polaris rolleri, servis hesapları)

Tek kimlik kaynağı **AD**: AD grubu → Keycloak grubu → Trino grubu, üçü de **aynı adla**. Üç grup vardır ve ad değişmez:
`lakehouse-admins`, `lakehouse-analysts`, `lakehouse-users`.

| Bileşen | Kimlik | Grup nereden gelir |
|---|---|---|
| Trino | Keycloak OIDC (Bearer/JWT, `preferred_username`) ya da servis hesabı (HTTP Basic) | **group provider**: dev dosya (`platform/values/trino-dev.yaml` `auth.groups`), prod LDAP (`platform/values/trino-ldap.yaml`). Trino'nun OAuth2'sinde grup talebi okunamaz (`groups-field` yok) |
| Superset | Keycloak OIDC | `groups` userinfo talebi (realm'in `superset` client'ındaki group membership mapper) → `AUTH_ROLES_MAPPING` |
| JupyterHub | Keycloak OIDC | `auth_state_groups_key: oauth_user.groups` → `allowed_groups` / `admin_groups` |
| Zeppelin | **Shiro + AD (LDAPS)** — OIDC yok (0.12'de pac4j/OIDC realm'i yok) | `activeDirectoryRealm.groupRolesMap` → Shiro rolleri; `[urls]` son kuralı `anyofroles[admin, analyst, user]` ile **rol zorunludur** (`examples/zeppelin/shiro-ad.ini`) |

**İstisna — Superset'te kayıt açıktır (bilinçli, kabul edilen):** `glue/templates/superset.yaml` → `AUTH_USER_REGISTRATION = True` + `AUTH_USER_REGISTRATION_ROLE = "Public"`. Flask-AppBuilder OIDC'de bu bayrak olmadan **hiç kimse** giremez: ilk girişte kullanıcı kaydı yaratılmadığı için `lakehouse-admins` üyesi bile 401 alır. Sonuç: Keycloak realm'inde kimliği doğrulanan (ama `lakehouse-*` gruplarının hiçbirinde olmayan) bir kullanıcı Superset'te **hesap açabilir** — ancak `Public` rolüyle: veri kaynağı, SQL Lab, dashboard ve chart izinleri YOKTUR, boş bir arayüz görür. `AUTH_ROLES_SYNC_AT_LOGIN` her girişte rolü grup üyeliğinden yeniden hesaplar, dolayısıyla grup dışı kullanıcı `Public`'te kalır. Grubu olmayanı Superset'e hiç sokmamak isteniyorsa doğru yer **Keycloak**'tır (`superset` client'ında grup zorunluluğu / client scope policy) — F5 kalemi; Superset tarafında `AUTH_USER_REGISTRATION=False` yapmak girişi herkes için kırar.

Rol eşlemeleri: Superset `glue/values.yaml` → `superset.roleMapping` (`lakehouse-admins: [Admin]`, `analysts: [Alpha]`, `users: [Gamma]`; her girişte `AUTH_ROLES_SYNC_AT_LOGIN`). JupyterHub `platform/values/jupyterhub.yaml` (`allowed_groups` üçü de, `admin_groups` yalnız admins).

## Trino `rules.json` anatomisi

Dosya `platform/values/trino.yaml` → `accessControl.rules["rules.json"]` içindedir (chart bunu ConfigMap'e yazar; `accessControl.type: configmap`, `refreshPeriod: 60s`).

- Üç bölüm: **`catalogs`** (katalog görünürlüğü: `all` | `read-only` | `none`), **`schemas`** (şema **sahipliği** — `owner: true` = şema/tablo yaratma-silme hakkı), **`tables`** (tablo ayrıcalıkları + satır filtresi + kolon maskesi).
- **İLK EŞLEŞEN KURAL KAZANIR.** Bu yüzden özel (kullanıcı filtre/maske) kuralları genel `SELECT` kuralından ÖNCE yazılır; sırayı bozmak filtreyi sessizce devre dışı bırakır.
- Eşleştirme alanları düzenli ifadedir: `{"group": "lakehouse-admins|lakehouse-analysts", "catalog": "lakehouse|system"}`; `user` alanı servis hesapları içindir.
- **Satır filtresi**: `"filter": "status <> 'shipped'"` — kurala düşen kullanıcının o tabloya her erişimine WHERE olarak eklenir.
- **Kolon maskesi**: `"columns": [{"name": "remote", "mask": "'x.x.x.x'"}]` — maske bir SQL ifadesidir (sabit, `CASE`, hash…) ve kolon tipiyle uyumlu olmalıdır.

Canlı davranış (e2e `test/e2e/trino-check/check.py` her koşuda doğrular): `analyst1` → `shop.orders` 3 satır, `remote` maskesiz, `sandbox`'a CTAS **başarılı**; `user1` → aynı tabloda **2 satır** (filtre), `remote='x.x.x.x'` (maske), `sandbox` CTAS **Access Denied**; servis hesabı `e2e` → okur, sandbox'a yazamaz.

### Yeni grup / yeni kural ekleme
1. AD'de grup + Keycloak'ta aynı adlı grup (federasyon getirir; realm dosyasına elle grup eklendiyse `docs/30-kurulum.md` "Realm içeriğini sonradan değiştirmek" — import mevcut realm'i güncellemez).
2. `platform/values/trino.yaml` → `accessControl.rules` içine kuralları **doğru sıraya** ekle (özel → genel).
3. Grup adı üç yerde birden geçer: Superset `superset.roleMapping`, JupyterHub `allowed_groups`, Zeppelin `groupRolesMap` — gerekiyorsa onları da güncelle.
4. Commit + push → ArgoCD `trino`/`glue` sync. Coordinator'ı yeniden başlatmaya **gerek yok**: ConfigMap değişikliği pod'a yayıldıktan (~1 dk, kubelet) sonra `refreshPeriod: 60s` ile dosya yeniden okunur → toplam ≤ 2 dk.
5. Doğrula: ilgili kullanıcının token'ıyla bir `SELECT` (e2e `check.py` kalıbı) ya da `SHOW SCHEMAS FROM lakehouse`.

### LDAP group provider (prod)
`platform/values/trino-ldap.yaml` **yalnız prod** Application'ın `valueFiles` listesindedir (dev overlay listeyi `[trino.yaml, trino-dev.yaml]` ile değiştirir). Ayrı dosya olmasının nedeni: Helm map birleştirmesinde `additionalConfigFiles: {}` vermek prod anahtarını **silmez**. Doldurulacak alanlar:
`ldap.url` (ldaps://…:636), `ldap.admin-user` (bind DN), `ldap.admin-password` (`${ENV:LDAP_BIND_PASSWORD}` → Secret `keycloak-clients/ldap-bind`), `ldap.user-base-dn`, `ldap.user-search-filter` (`(sAMAccountName={0})`), `ldap.user-member-of-attribute` (`memberOf`), `ldap.group-name-attribute` (`cn`).
Trino grubu **CN** olarak alır → AD grubunun CN'i `lakehouse-analysts` gibi olmalıdır. Canlı doğrulama pre-ship OpenShift ortamında yapılacaktır (dev'de dosya provider'ı koşar).

## Yazma alanı: `sandbox`

Yazma iki katmandan birden geçer:
1. **Polaris** (`platform/polaris/setup.yaml`): katalog rolü `lakehouse_sandbox` yalnız `sandbox` namespace'inde TABLE_CREATE/DROP/READ/WRITE verir; principal rolü `sandbox_writers` bunu `trino` ve `notebooks` principal'larına bağlar → **motor** ancak sandbox'a yazabilir.
2. **Trino `rules.json`**: `schemas` → `{"group": "lakehouse-analysts", "schema": "sandbox", "owner": true}`, `tables` → aynı grup için `sandbox` şemasında SELECT/INSERT/DELETE/UPDATE/OWNERSHIP. `lakehouse-users` bu kurallara düşmez → CTAS/INSERT reddedilir.

Analist/kullanıcı ayrımı Trino'da, motor sınırı Polaris'te. Bronze/Silver namespace'lerine Trino üzerinden **hiç kimse** yazamaz (oraya yalnız `connect`/`spark` principal'ları yazar).

**Purge kapsamı:** `DROP TABLE` sırasında verinin de silinmesi (`purgeRequested=true`) artık **katalog düzeyinde** açılır — `platform/polaris/setup.yaml` → `lakehouse` kataloğunun `properties`'inde `polaris.config.drop-with-purge.enabled: "true"` (sunucu geneli `features.DROP_WITH_PURGE_ENABLED` KALDIRILDI). Yani bayrak yalnız bu kataloğu kapsar; ileride eklenecek başka kataloglar (federated/external dâhil) etkilenmez. Kalan risk: bu katalog içinde silme yetkisi olan her principal (`connect`, `spark` dâhil) silerken veriyi de purge edebilir — Trino tarafında yalnız `sandbox` şeması yazılabilir olduğundan analist/kullanıcı için kapsam sandbox'tır. **Kurulum uyarısı:** `polaris setup apply` katalog `properties`'ini yalnız katalog YARATILIRKEN yazar; var olan bir kurulumda `polaris catalogs update --set-property polaris.config.drop-with-purge.enabled=true lakehouse` gerekir (`docs/30-kurulum.md` "Realm içeriğini sonradan değiştirmek").

## Servis hesapları (paylaşımlı — spec §14)

`trino-service-accounts` Secret'ındaki `password.db` (htpasswd bcrypt) hesapları HTTP **Basic** ile bağlanır:

| Hesap | Kullanan | Yetki |
|---|---|---|
| `superset` | Superset'in Trino datasource'u | `rules.json` `{"user": "superset\|zeppelin\|e2e", …}` → **yalnız SELECT**, sandbox'a yazamaz |
| `zeppelin` | Zeppelin JDBC interpreter | aynı |
| `e2e` | yalnız dev/e2e (prod'da yaratılmaz) | aynı |

**Doğrudan sonucu:** Superset ve Zeppelin son kullanıcının kimliğini Trino'ya TAŞIMAZ → o araçlarda satır filtresi ve kolon maskesi **uygulanmaz** (servis hesabı maskesiz veriyi görür; kurallar yalnız "SELECT" düzeyinde sınırlar). Satır/kolon yetkilendirmesi isteniyorsa kullanıcı Trino'ya kendi kimliğiyle bağlanmalıdır: Trino Web UI (`/ui`), JDBC/CLI ile OIDC, ya da notebook'ta `trino.auth.OAuth2Authentication` (`runbooks/user-facing.md`). Bu nedenle Superset/Zeppelin'de kullanıcılara açılan veri, **araç düzeyinde** sınırlanmalıdır (Superset: Gamma rolü + veri kaynağı/şema izinleri; Zeppelin: not defteri paylaşım izinleri).

Parola rotasyonu: `password.db`'yi yeniden üret + düz anahtarları güncelle (`kubectl -n lakehouse create secret generic trino-service-accounts … --dry-run=client -o yaml | kubectl apply -f -`) → Trino parola dosyasını yeniden okur; Superset env'i pod restart ister (`kubectl -n lakehouse rollout restart deploy/superset-web-server`); Zeppelin için tohum semantiği geçerlidir (`runbooks/user-facing.md`).
