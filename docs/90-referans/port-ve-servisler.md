# 90 — Port ve servisler

**Bu bölümde:** kurulumun ürettiği **bütün** ağ uçları — dışarıya açılan Route'lar, küme içi
Service'ler ve portları, kümeden dışarı açılması gereken bağlantılar, ağ politikaları ve
yalnız geliştirme kurulumunda bulunan uçlar. Güvenlik duvarı talebi açarken ve bir
bağlantıyı teşhis ederken bu sayfa kullanılır.
**Süre:** okuma 10 dakika.
**Gereken yetki:** `$LAKEHOUSE_NS` ad alanında okuma.
**Nerede çalıştırılır:** `[bastion]`.

Planlama aşamasındaki özet tablo [10-planlama](../10-planlama.md) §4'tedir; burası ayrıntılı
listedir. Aşağıdaki Service satırları çalışan bir kümeden (`oc get svc`), Route satırları
ise üretim değerleriyle render edilmiş chart çıktısından alınmıştır.

---

## 1. Dışarıya açılan uçlar (Route)

glue chart'ı OpenShift'te altı Route üretir. Hostname'ler boş bırakıldığında
bileşen adı + `-$LAKEHOUSE_NS.$APPS_DOMAIN` biçiminde türetilir;
`platform/values/site/glue.yaml` içindeki
`hostname` anahtarı tek satırla ezer.

| Route | Hedef Service | Hedef port | TLS | Adres | Kim kullanır |
|---|---|---|---|---|---|
| `keycloak` | `keycloak-service` | `http` (8080) | `edge` | `keycloak-$LAKEHOUSE_NS.$APPS_DOMAIN` | Tarayıcılar (OIDC giriş akışı) |
| `trino` | `trino` | `https` (8443) | `passthrough`; `tls.caBundle` doluysa `reencrypt` | `trino-$LAKEHOUSE_NS.$APPS_DOMAIN` | Tarayıcı (Web UI), dış SQL istemcileri |
| `superset` | `superset-web-server` | `http` (8088) | `edge` | `superset-$LAKEHOUSE_NS.$APPS_DOMAIN` | Tarayıcılar |
| `jupyterhub` | `proxy-public` | `http` (80) | `edge` | `jupyterhub-$LAKEHOUSE_NS.$APPS_DOMAIN` | Tarayıcılar |
| `zeppelin` | `zeppelin` | `http` (8080) | `edge` | `zeppelin-$LAKEHOUSE_NS.$APPS_DOMAIN` | Tarayıcılar |
| `polaris` | `polaris` | 8181 | `edge` | Route'da host **verilmez**; OpenShift adresi kendisi üretir | Küme dışından katalog erişimi (isteğe bağlı — motorlar küme içi Service'i kullanır) |

Trino kendi TLS'ini sonlandırdığı için Route'u `edge` **değildir**: kimlik doğrulaması TLS
zorunlu kılar. `tls.caBundle` boşken `passthrough` üretilir ve tarayıcıların iç kök CA'ya
güvenmesi gerekir; değer doldurulduğunda Route `reencrypt` olur
([30-kurulum](../30-kurulum.md) §5.11).

Kafka'nın **dış dinleyicisi** ayrı bir uçtur ve varsayılan olarak **kapalıdır**
(`glue/values.yaml` → `kafka.externalListener: false`). Yalnız nginx erişim günlüğü akışı
kullanılacaksa açılır; OpenShift'te Strimzi `route` tipinde, vanilla/kind kurulumunda
`nodeport` tipinde uç üretir ([20-on-kosullar](../20-on-kosullar.md) madde 8).

Adresleri kümeden okumak için:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get route \
  -o custom-columns='AD:.metadata.name,HOST:.spec.host,TLS:.spec.tls.termination'
```

**Beklenen çıktı** (örnek — kendi `$APPS_DOMAIN` değerinizle):

```text
AD           HOST                                         TLS
keycloak     keycloak-lakehouse.apps.ocp.example.net      edge
polaris      polaris-lakehouse.apps.ocp.example.net       edge
trino        trino-lakehouse.apps.ocp.example.net         passthrough
superset     superset-lakehouse.apps.ocp.example.net      edge
jupyterhub   jupyterhub-lakehouse.apps.ocp.example.net    edge
zeppelin     zeppelin-lakehouse.apps.ocp.example.net      edge
```

**Ters giderse:** `No resources found` → chart vanilla modunda render edilmiştir
(`platform: vanilla`) ve Route yerine Ingress üretilmiştir. Bir Route'un `HOST` hücresi
boşsa router adresi henüz atanmamıştır, birkaç saniye sonra yeniden bakın.

---

## 2. Küme içi Service'ler

Aşağıdaki liste çalışan bir kümeden alınmıştır. Küme içi DNS adı
servis adı + `.$LAKEHOUSE_NS.svc` biçimindedir (örnek: `trino.lakehouse.svc`).

| Service | Port(lar) | Protokol | Kim bağlanır | Ne için |
|---|---|---|---|---|
| `trino` | 8080, 8443 | HTTP / HTTPS | Superset, Zeppelin, JupyterHub, Spark, dbt örneği | SQL. **8080'de kimlik doğrulama yoktur** (yalnız probe ve iç trafik); istemciler 8443 kullanır |
| `trino-worker` | 8080 | HTTP | Trino coordinator | Coordinator ↔ worker iç iletişimi (headless) |
| `polaris` | 8181 | HTTP | Trino, Spark, not defterleri, `scripts/polaris-setup.sh` | Iceberg REST kataloğu |
| `polaris-mgmt` | 8182 | HTTP | Prometheus (ServiceMonitor) | Sağlık ve metrik uçları (headless) |
| `keycloak-service` | 8080, 9000 | HTTP | Trino, Superset, JupyterHub, router | OIDC (8080); yönetim ve sağlık (9000) |
| `keycloak-discovery` | 7800 | TCP | Keycloak pod'ları | Küme içi önbellek keşfi (headless) |
| `superset-web-server` | 8088 | HTTP | Router | Superset arayüzü |
| `proxy-public` | 80 | HTTP | Router | JupyterHub girişi |
| `proxy-api` | 8001 | HTTP | JupyterHub hub | Proxy yönetim API'si |
| `hub` | 8081 | HTTP | JupyterHub proxy'si ve not defteri pod'ları | Hub API'si |
| `zeppelin` | 8080 | HTTP | Router | Zeppelin arayüzü |
| `lakehouse-kafka-bootstrap` | 9091, 9092, 9093 | TCP | Kafka Connect, Spark, kafka-exporter | Kafka istemci bağlantısı. **Üretimde yalnız 9093** (TLS + SCRAM-SHA-512); 9092 (TLS'siz) yalnız geliştirmede açılır |
| `lakehouse-kafka-brokers` | 8443, 9090, 9091, 9092, 9093 | TCP | Kafka broker'ları, Strimzi operatörü | Broker'lar arası ve operatör trafiği (headless) |
| `connect-connect-api` | 8083 | HTTP | Strimzi operatörü, teşhis | Kafka Connect REST API'si |
| `connect-connect` | 8083 | HTTP | Kafka Connect pod'ları | Connect küme içi koordinasyonu (headless) |
| `polaris-db-rw` / `-ro` / `-r` | 5432 | TCP | Polaris | Katalog veritabanı (yazma / okuma replikası / herhangi bir örnek) |
| `keycloak-db-rw` / `-ro` / `-r` | 5432 | TCP | Keycloak | Kimlik veritabanı |
| `superset-db-rw` / `-ro` / `-r` | 5432 | TCP | Superset | Metastore veritabanı |
| `spark-operator-webhook-svc` | 9443 | HTTPS | kube-apiserver | Admission webhook'u |
| `superset-operator-metrics` | 8443 | HTTPS | Prometheus | Operatör metrikleri |
| `keycloak-operator` | 80 | HTTP | kube-apiserver | Operatör servisi |

Metrik uçlarının bir kısmı Service değil **pod** düzeyindedir: Kafka broker'ları,
kafka-exporter ve Kafka Connect pod'ları `tcp-prometheus` (9404) portunu açar ve
`kafka-resources-metrics` PodMonitor'ü tarafından toplanır
([40-kurulum-sonrasi](../40-kurulum-sonrasi.md) §6).

Listeyi kendi kümenizden almak için:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get svc \
  -o custom-columns='AD:.metadata.name,TIP:.spec.type,PORT:.spec.ports[*].port'
```

**Beklenen çıktı** (örnek — yukarıdaki tablonun bir bölümü; geliştirme kümesinde ek satırlar
çıkar):

```text
AD                          TIP         PORT
connect-connect-api         ClusterIP   8083
keycloak-service            ClusterIP   8080,9000
lakehouse-kafka-bootstrap   ClusterIP   9091,9093
polaris                     ClusterIP   8181
superset-web-server         ClusterIP   8088
trino                       ClusterIP   8080,8443
zeppelin                    ClusterIP   8080
```

**Ters giderse:** beklenen bir Service listede yoksa ilgili uygulama henüz sync olmamıştır
(`oc -n "$ARGOCD_NS" get applications`).

---

## 3. Kümeden dışarı (egress)

Bu bağlantılar güvenlik duvarında açılmalıdır; kapalı ağda karşılıkları iç aynalardır.

| Kimden | Kime | Port / protokol | Ne için | Zorunlu mu |
|---|---|---|---|---|
| Kafka Connect, Spark | Kaynak PostgreSQL | 5432/TCP | CDC okuması | Kaynak varsa |
| Kafka Connect, Spark | Kaynak SQL Server | 1433/TCP | CDC okuması | Kaynak varsa |
| Kafka Connect, Spark | Kaynak MongoDB | 27017/TCP | CDC okuması | Kaynak varsa |
| Polaris, Kafka Connect, Spark, not defterleri | S3 (`$S3_ENDPOINT`) | 443/HTTPS | Iceberg verisi | **Evet** |
| CloudNativePG (Barman) | Yedek S3 (`$S3_BUCKET_BACKUP`) | 443/HTTPS | PostgreSQL yedekleri | **Evet** |
| Keycloak, Trino, Zeppelin | Active Directory (`$LDAP_URL`) | 636/LDAPS | Kimlik ve grup okuması | **Evet** |
| Kafka Connect (imaj üretimi), Spark, Zeppelin | Maven Central | 443/HTTPS | Connector ve sürücü artefaktları | **Evet** (ya da iç ayna) |
| JupyterHub not defterleri, dbt örneği | PyPI | 443/HTTPS | `pyiceberg`, `trino`, `dbt-trino` | Not defteri veya dbt kullanılacaksa |
| Bütün bileşenler | quay.io, ghcr.io, docker.io, `downloads.apache.org` | 443/HTTPS | İmaj ve chart çekimi | **Evet** (ya da iç ayna) |

Ayrıntılı alan adı listesi [20-on-kosullar](../20-on-kosullar.md) madde 10'dadır.

---

## 4. Dışarıdan kümeye (ingress)

| Kimden | Kime | Port / protokol | Ne için |
|---|---|---|---|
| Tarayıcılar | Router → Keycloak, Trino, Superset, JupyterHub, Zeppelin Route'ları | 443/HTTPS | Kullanıcı arayüzleri |
| Dış SQL istemcileri (JDBC/ODBC araçları) | Trino Route'u | 443/HTTPS | SQL |
| Fluent Bit ajanları | Kafka dış dinleyicisi | 443/TLS + SCRAM | nginx erişim günlüğü (isteğe bağlı akış) |
| Yönetim makinesi | kube-apiserver | 6443/HTTPS | `oc` komutları |

---

## 5. Ağ politikaları

glue chart'ı `$LAKEHOUSE_NS` ad alanına üç `NetworkPolicy` yazar
(`glue/templates/networkpolicy.yaml`). Üçü de **yalnız Ingress** yönünü kısıtlar; egress
serbest bırakılmıştır — Connect'in artefakt çekişi, S3 ve LDAP bu yüzden çalışır.

| Politika | Ne yapar |
|---|---|
| `default-deny-ingress` | Ad alanındaki bütün pod'lara gelen trafiği varsayılan olarak kapatır |
| `allow-same-namespace` | Aynı ad alanındaki pod'ların birbirine erişmesine izin verir |
| `allow-platform-namespaces` | Operatör ve platform ad alanlarını açar: `cnpg-system`, ArgoCD **(şablonda sabit `argocd`)**, router (`openshift-ingress`), `openshift-monitoring`, `openshift-user-workload-monitoring`; `velero.enabled` açıksa yedekleme ad alanı da eklenir |

ArgoCD satırındaki ad alanı seçicisi `glue/templates/networkpolicy.yaml` içinde **sabit
`argocd`** yazılıdır: OpenShift'te ArgoCD `$ARGOCD_NS` (`openshift-gitops`) ad alanında
koştuğu için bu seçici hiçbir ad alanıyla eşleşmez. Pratikte sorun çıkarmaz — ArgoCD
kube-apiserver ile konuşur, `$LAKEHOUSE_NS` pod'larına doğrudan bağlanmaz — ama ArgoCD'den
pod'a doğrudan erişim gerekirse politikaya kendi ad alanınızı eklemeniz gerekir.

JupyterHub not defteri pod'larının **çıkış** trafiği ayrıca daraltılmıştır (z2jh'nin kendi
`singleuser.networkPolicy.egress` ayarı): küme içinde yalnız Trino 8443 ve Polaris 8181'e
izin verilir, genel internet açık kalır. Yeni bir küme içi servise erişim gerekirse kural
`platform/values/jupyterhub.yaml` **ve** `platform/values/jupyterhub-dev.yaml` dosyalarının
ikisine birden eklenir: Helm listeleri birleştirmez, üzerine yazar.

---

## 6. Yalnız geliştirme kurulumunda görülen uçlar

Aşağıdakiler kind/geliştirme kümesinde vardır, üretimde **kurulmaz**:

| Service | Port | Ne |
|---|---|---|
| `minio` | 9000, 9001 | Geliştirme S3'ü (API ve konsol) |
| `demo-pg-rw` / `-ro` / `-r` | 5432 | Örnek kaynak PostgreSQL (e2e fixture'ı) |
| `demo-mongo` | 27017 | Örnek kaynak MongoDB (e2e fixture'ı) |
| `lakehouse-kafka-external-bootstrap` | 9094 (NodePort) | Kafka dış dinleyicisi — OpenShift'te bunun yerine Route üretilir |
| `lakehouse-dual-role-0` | 9094 (NodePort) | Broker başına dış dinleyici (yalnız vanilla) |

---

## Kontrol listesi

- [ ] Altı Route da `oc get route` çıktısında ve adresleri DNS'te çözülüyor.
- [ ] Trino Route'unun sonlandırması bilinçli seçildi (`passthrough` ya da `reencrypt`).
- [ ] §3'teki zorunlu egress bağlantıları güvenlik duvarında açık.
- [ ] Kafka dış dinleyicisi yalnız nginx akışı kullanılıyorsa açık.
- [ ] Üç `NetworkPolicy` ad alanında mevcut (`oc -n $LAKEHOUSE_NS get networkpolicy`).
- [ ] Üretim kümesinde §6'daki geliştirme Service'lerinden hiçbiri yok.

## Sonraki bölüm

[10-planlama.md](../10-planlama.md) §4 — planlama özeti ve DNS/sertifika kararları.
Bileşen sürümleri: [surumler-ve-lisanslar.md](surumler-ve-lisanslar.md).
