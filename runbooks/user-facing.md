# Kullanıcı yüzü (Superset, JupyterHub, Zeppelin, Trino istemcileri)

Ortak uç nokta: **`https://trino.lakehouse.svc:8443`** (küme içi) / `https://<trino.hostname>` (dışarıdan). HTTP 8080'de kimlik doğrulama YOKTUR (yalnız probe/iç trafik) — her istemci HTTPS kullanır ve `lakehouse-ca`'ya güvenir (`Secret lakehouse-ca`, `tls.crt`; pod'larda `/etc/lakehouse-ca/tls.crt`). Kimlik iki yoldan gelir: **servis hesabı** (HTTP Basic, SELECT-only) ya da **son kullanıcı** (Keycloak Bearer/JWT). Farkı ve sonuçları: `runbooks/access-control.md`.

## Superset

- İmaj: resmi **`apache/superset:6.1.0-dev`** (`glue/values.yaml` → `superset.imageTag`). `-dev` etiketi `psycopg2`, `trino`, `authlib` sürücülerini **içerir**; düz `6.1.0` içermez ve site-packages salt-okunurdur (uid 1000) → açılışta `pip install` YOK, pod başlangıcında PyPI erişimi gerekmez. Etiket yükseltilirken sürücülerin hâlâ imajda olduğu doğrulanmalıdır.
- Kurulum: Apache **Superset Kubernetes Operator 0.2.0** + `Superset/superset` CR (`glue/templates/superset.yaml`); resmi Helm chart **deprecated**. Web Deployment/Service adı `superset-web-server` (port 8088).
- TLS güveni bilinçli olarak **ikiye ayrılmıştır**: `REQUESTS_CA_BUNDLE` YOKTUR → Keycloak sistem kökleriyle doğrulanır (prod'da edge Route + router sertifikası; tek kök dayatmak OIDC'yi kırardı), Trino ise datasource'un `connect_args.verify=/etc/lakehouse-ca/tls.crt` alanıyla sabitlenir.

### Trino bağlantısını içe aktar (kurulum başına TEK SEFER)
Bağlantı tanımı deklaratiftir (`ConfigMap/superset-datasources` → `/app/configs/trino.yaml`); metastore'a bir kez aktarılır. Parola URI'de değildir, `SQLALCHEMY_CUSTOM_PASSWORD_STORE` ile `TRINO_PASSWORD` env'inden okunur.

```bash
POD=$(kubectl -n lakehouse get pod \
  -l app.kubernetes.io/name=superset,app.kubernetes.io/instance=superset,app.kubernetes.io/component=web-server \
  --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}')
kubectl -n lakehouse exec "$POD" -- superset legacy-import-datasources -p /app/configs/trino.yaml
```
Sonuç: SQL Lab'da `lakehouse` veritabanı (`allow_dml: false`; CTAS/CVAS açık — hedef şema **`sandbox`** olmalı). `legacy-import-datasources` v0 importer'dır (deprecated ama 6.1'de çalışır); 6.x'in `import-datasources` komutu yalnız v1 ZIP kabul eder. Komut idempotenttir (aynı `database_name` güncellenir); bağlantı ayarı değişince tekrar çalıştırın.

### Alerts & Reports (varsayılan KAPALI)
Kurulum Redis/Valkey'siz çalışır (`SimpleCache`; worker/beat yok). Alerts & Reports için CR'a **`valkey`** bloğu + **`celeryWorker`** (zamanlama için beat) eklenmeli, `CELERY_CONFIG`/`SCREENSHOT_*` yapılandırılmalı ve ekran görüntüsü için headless tarayıcı sağlanmalıdır — özel imaj gerekebileceğinden F5 kararıdır. Dış bildirim (Slack/SMTP) genel internete HTTPS ister; sistem kökleri kullanıldığı için bu yönüyle sorun yoktur.

## JupyterHub (z2jh)

- Giriş Keycloak ile; yalnız `lakehouse-*` gruplarının üyeleri (`allowed_groups`), `lakehouse-admins` hub yöneticisi. Her kullanıcıya kişisel PVC (dev 1Gi, prod 10Gi).
- Not defteri imajı `quay.io/jupyter/pyspark-notebook:spark-4.1.2`; **her spawn'da** `postStart` ile `pyiceberg[s3fs,pyarrow]` + `trino` kurulur (~1 dk, PyPI erişimi gerekir — kapalı ağda iç PyPI aynası şart). Soğuk imaj çekimi ölçülen ~4,5 dk; bu yüzden `singleuser.startTimeout: 1200`.
- Not defteri pod'unun çıkış trafiği **daraltılmıştır** (z2jh `singleuser.networkPolicy.egress`): küme içinde yalnız Trino 8443 ve Polaris 8181 (+dev MinIO 9000); genel internet (PyPI/S3) açık, diğer özel IP'ler kapalı. Yeni bir küme içi servise erişim gerekiyorsa kural `platform/values/jupyterhub.yaml`'a **ve** `jupyterhub-dev.yaml`'a eklenir (Helm listeleri birleştirmez, üzerine yazar → kurallar tekrarlanır).
- Hazır env: `POLARIS_URI`, `POLARIS_CREDENTIAL` (paylaşımlı `notebooks` principal'ı), `TRINO_HOST`, `S3_ENDPOINT`, `REQUESTS_CA_BUNDLE=/etc/lakehouse-ca/tls.crt`.

```python
# PyIceberg -> Polaris (paylaşımlı notebooks principal'ı; yazma yalnız sandbox namespace'inde)
import os
from pyiceberg.catalog import load_catalog
cat = load_catalog("lakehouse", type="rest", uri=os.environ["POLARIS_URI"], warehouse="lakehouse",
                   credential=os.environ["POLARIS_CREDENTIAL"], scope="PRINCIPAL_ROLE:ALL")
cat.list_namespaces()
tbl = cat.load_table("shop.orders"); tbl.scan(limit=10).to_pandas()
```

```python
# Trino: KENDİ kimliğinizle -> satır filtresi + kolon maskesi UYGULANIR.
# Tarayıcıda Keycloak onayı istenir; token yerel olarak saklanır (prod/pre-ship; kind'da çalışmaz).
import os, trino
conn = trino.dbapi.connect(host=os.environ["TRINO_HOST"], port=8443, http_scheme="https",
                           verify="/etc/lakehouse-ca/tls.crt",
                           auth=trino.auth.OAuth2Authentication(), catalog="lakehouse")
cur = conn.cursor(); cur.execute("select * from shop.orders limit 10"); cur.fetchall()
```
Tarayıcısız/otomatik işlerde `trino.auth.BasicAuthentication("<servis hesabı>", "<parola>")` kullanılır — bu durumda satır/kolon kuralları kullanıcı bazında işlemez (`runbooks/access-control.md`).

## Zeppelin

- Giriş **Shiro + AD (LDAPS)**, Keycloak DEĞİL (0.12'de OIDC/pac4j realm'i yok). Yapılandırma `Secret zeppelin-shiro` (`shiro.ini`; prod şablonu `runbooks/zeppelin/shiro-ad.ini`); değişiklikten sonra `kubectl -n lakehouse rollout restart deploy/zeppelin`.
- Kullanım `%jdbc` ile: `%jdbc` ⏎ `select count(*) from shop.orders`. Bağlantı `jdbc:trino://trino.lakehouse.svc:8443/lakehouse?SSL=true&SSLTrustStorePath=/etc/lakehouse-ca/tls.crt`, kullanıcı `zeppelin` servis hesabı (SELECT-only).
- **Tohum semantiği (önemli):** `Secret zeppelin-interpreter` yalnız bir TOHUM'dur. initContainer dosyayı PVC'ye (`/data/conf/interpreter.json`, `ZEPPELIN_CONFIG_FS_DIR`) **yalnız orada dosya yokken** kopyalar; Zeppelin her açılışta bu dosyayı kendisi yeniden yazar (kalan 23 interpreter'ı şablonlardan tamamlar). Yani Secret'ı güncellemek TEK BAŞINA etkisizdir. Parola/URL değiştirmek için:
  - UI → Interpreter → `jdbc` ayarını düzenle (önerilen; kullanıcı ayarlarını korur), **ya da**
  - `kubectl -n lakehouse exec deploy/zeppelin -- rm /data/conf/interpreter.json && kubectl -n lakehouse rollout restart deploy/zeppelin` — bu, UI'da yapılmış TÜM interpreter değişikliklerini sıfırlar.
- İlk açılışta `io.trino:trino-jdbc:483` Maven Central'dan indirilir (ölçüm ~64 s; `/data/local-repo` PVC'de kalıcı, sonraki açılışlarda indirilmez). O sırada paragraf çalıştırılırsa `Interpreter Setting 'jdbc' is not ready … DOWNLOADING_DEPENDENCIES` alınır — bekleyin. Kapalı ağda jar'ı PVC'ye koyup bağımlılığı `local: true` yapmak gerekir (F5).
- Yorumlayıcılar aynı pod'da yerel süreç olarak koşar (`ZEPPELIN_RUN_MODE=local`); varsayılan `auto`, küme içinde her interpreter için ayrı pod açmayı dener ve RBAC olmadığından düşer.

## Dev/kind sınırı (tarayıcı akışları)

Dev'de `keycloak.hostname` **küme içi** bir URL'dir (`http://keycloak-service.lakehouse.svc:8080`) — token `iss`'i küme içinden çözülebilsin diye. Sonuç: **kind'da tarayıcıyla OIDC girişi yapılamaz** (yönlendirme adresi dışarıdan çözülmez). e2e bu yüzden akışları başka türlü kanıtlar: Keycloak **password grant** ile alınan Bearer token'la Trino (satır filtresi/kolon maskesi/sandbox testleri), Superset'te login sayfasındaki provider + discovery + Authlib metadata yüklemesi, JupyterHub'da admin API ile spawn. Gerçek tarayıcı akışı prod/pre-ship'te (`https://keycloak.<domain>`) geçerlidir.

## Bilinen açıklar (pre-ship OpenShift'te canlı doğrulanacak)

| Konu | Şu anki kanıt | Eksik |
|---|---|---|
| Superset / Trino Web UI / JupyterHub tarayıcı OIDC akışı | e2e: password grant Bearer + provider/discovery/metadata kontrolleri | Gerçek redirect/callback, grup→rol senkronu |
| Trino Route **reencrypt** (`tls.caBundle` dolu) | Şablon render ediliyor; dev'de Route yok | Router'ın `destinationCACertificate` ile bağlanması |
| LDAP group provider (prod grupları) | Dev'de dosya provider'ı canlı | AD'ye bağlanıp `memberOf`/CN eşlemesi |
| Sertifika yenileme (`ssl-context.refresh-time 1m`) | cert-manager `trino-tls`'i üretiyor | Yenilemenin kesintisiz olduğunun gözlenmesi |
| Alerts & Reports | Kapalı (Valkey/worker yok) | Valkey + celeryWorker + tarayıcı kararı (F5) |
| Superset imaj kaynağı | Varsayılan `apachesuperset.docker.scarf.sh/apache/superset` (Scarf yönlendiricisi) | Müşteri aynasına `spec.image.repository` (+ digest) sabitleme |
