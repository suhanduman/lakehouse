# 90 — Port ve servisler

Lakehouse'un bütün ağ uçları bu sayfadadır: dışarıya açılan adresler (Route), küme içi
servisler ve portları, kümeden dışarıya açılması gereken bağlantılar ve ağ politikaları.
Güvenlik duvarı talebi açarken ve bir bağlantı sorununu araştırırken kullanılır.

**Ne kadar sürer:** okuması 10 dakika.

**Hangi yetkiler gerekir:** `$LAKEHOUSE_NS` ad alanında okuma.

**Komutlar nerede çalışır:** `[bastion]` — `oc login` ile kümeye bağlı yönetim makinesi.

Planlama aşamasındaki özet tablo [10-planlama](../10-planlama.md) §4'tedir; bu sayfa
ayrıntılı listedir. Aşağıdaki çıktılar OpenShift üzerindeki çalışan bir kurulumdan
alınmıştır.

---

## 1. Dışarıya açılan adresler (Route)

Ürün altı arayüz adresi açar. Adres verilmezse bileşenin adı ile `-$LAKEHOUSE_NS.$APPS_DOMAIN`
birleştirilir; `platform/values/site/glue.yaml` dosyasındaki `hostname` ayarıyla bir
adres değiştirilebilir.

| Route | Hedef servis | Hedef port | TLS | Adres | Kim kullanır |
|---|---|---|---|---|---|
| `keycloak` | `keycloak-service` | 8080 | `edge` | `keycloak-$LAKEHOUSE_NS.$APPS_DOMAIN` | tarayıcılar (giriş) |
| `trino` | `trino` | 8443 | `reencrypt` ya da `passthrough` | `trino-$LAKEHOUSE_NS.$APPS_DOMAIN` | tarayıcı (Trino arayüzü), dış SQL araçları |
| `superset` | `superset-web-server` | 8088 | `edge` | `superset-$LAKEHOUSE_NS.$APPS_DOMAIN` | tarayıcılar |
| `jupyterhub` | `proxy-public` | 80 | `edge` | `jupyterhub-$LAKEHOUSE_NS.$APPS_DOMAIN` | tarayıcılar |
| `zeppelin` | `zeppelin` | 8080 | `edge` | `zeppelin-$LAKEHOUSE_NS.$APPS_DOMAIN` | tarayıcılar |
| `polaris` | `polaris` | 8181 | `edge` | `polaris-$LAKEHOUSE_NS.$APPS_DOMAIN` | küme dışından katalog erişimi (isteğe bağlı; ürünün kendi bileşenleri küme içi servisi kullanır) |

**Trino'nun adresi farklıdır.** Trino kendi TLS'ini kendisi sonlandırır, çünkü giriş
yapılabilmesi için bağlantının şifreli olması gerekir. `tls.caBundle` ayarı doluysa adres
`reencrypt` olur ve tarayıcılar kurumun genel sertifikasını görür. Ayar boşsa adres
`passthrough` olur ve tarayıcıların ürünün iç kök sertifikasına güvenmesi gerekir
([30-kurulum](../30-kurulum.md) §5.11).

**Kafka'nın dış adresi** ayrıdır ve varsayılan olarak kapalıdır (`kafka.externalListener`).
nginx erişim günlüğü akışı ya da uygulama/IoT olay akışı kullanılacaksa açılır. Açıldığında
bir giriş adresi (`lakehouse-kafka-bootstrap`) ve her Kafka sunucusu için bir adres
(`lakehouse-dual-role-0`, `-1`, `-2`) eklenir; hepsi `passthrough`'tur ve bağlantı TLS ile
şifrelenir ([50-isletme/yeni-kaynak-ve-pipeline.md](../50-isletme/yeni-kaynak-ve-pipeline.md)
§10).

Adresleri kümeden okumak için:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get route \
  -o custom-columns='AD:.metadata.name,HOST:.spec.host,TLS:.spec.tls.termination'
```

**Beklenen çıktı** (örnek bir kurulumdan; Kafka'nın dış adresi açık; alan adı sizde
farklıdır):

```text
AD                          HOST                                                               TLS
jupyterhub                  jupyterhub-lakehouse.apps.ocpphy.solusbilisim.com                  edge
keycloak                    keycloak-lakehouse.apps.ocpphy.solusbilisim.com                    edge
lakehouse-dual-role-0       lakehouse-dual-role-0-lakehouse.apps.ocpphy.solusbilisim.com       passthrough
lakehouse-dual-role-1       lakehouse-dual-role-1-lakehouse.apps.ocpphy.solusbilisim.com       passthrough
lakehouse-dual-role-2       lakehouse-dual-role-2-lakehouse.apps.ocpphy.solusbilisim.com       passthrough
lakehouse-kafka-bootstrap   lakehouse-kafka-bootstrap-lakehouse.apps.ocpphy.solusbilisim.com   passthrough
polaris                     polaris-lakehouse.apps.ocpphy.solusbilisim.com                     edge
superset                    superset-lakehouse.apps.ocpphy.solusbilisim.com                    edge
trino                       trino-lakehouse.apps.ocpphy.solusbilisim.com                       reencrypt
zeppelin                    zeppelin-lakehouse.apps.ocpphy.solusbilisim.com                    edge
```

**Ters giderse:** bir satırın `HOST` hücresi boşsa OpenShift adresi henüz atamamıştır;
birkaç saniye sonra yeniden bakın. Bir Route hiç yoksa ilgili uygulama henüz
eşitlenmemiştir (`oc -n "$ARGOCD_NS" get applications`).

---

## 2. Küme içi servisler

Bir servisin küme içi adı, servis adı ile `.$LAKEHOUSE_NS.svc` birleşimidir (örnek:
`trino.lakehouse.svc`).

| Servis | Port(lar) | Protokol | Kim bağlanır | Ne için |
|---|---|---|---|---|
| `trino` | 8080, 8443 | HTTP / HTTPS | Superset, Zeppelin, JupyterHub, Spark | SQL. **8080'de giriş denetimi yoktur** (yalnız sağlık kontrolü ve iç trafik); istemciler 8443'ü kullanır |
| `trino-worker` | 8080 | HTTP | Trino koordinatörü | koordinatör ile çalışanlar arası iletişim |
| `polaris` | 8181 | HTTP | Trino, Spark, not defterleri, `scripts/polaris-setup.sh` | Iceberg kataloğu |
| `polaris-mgmt` | 8182 | HTTP | izleme | sağlık ve ölçüm uçları |
| `keycloak-service` | 8080, 9000 | HTTP | Trino, Superset, JupyterHub, Zeppelin, router | giriş (8080); yönetim ve sağlık (9000) |
| `keycloak-discovery` | 7800 | TCP | Keycloak pod'ları | Keycloak'ın kendi iç iletişimi |
| `superset-web-server` | 8088 | HTTP | router | Superset arayüzü |
| `proxy-public` | 80 | HTTP | router | JupyterHub girişi |
| `proxy-api` | 8001 | HTTP | JupyterHub hub | JupyterHub'ın iç yönetimi |
| `hub` | 8081 | HTTP | JupyterHub proxy'si ve not defteri pod'ları | JupyterHub API'si |
| `zeppelin` | 8080 | HTTP | router | Zeppelin arayüzü |
| `maven-proxy` | 8080 | HTTP | spark-operator, Spark işleri | küme içi Maven önbelleği (Spark kütüphaneleri) |
| `lakehouse-kafka-bootstrap` | 9091, 9093 | TCP | Kafka Connect, Spark, izleme | Kafka'ya bağlantı. İstemciler 9093'ü kullanır (TLS + kullanıcı adı/parola); 9091 Kafka'nın kendi iç trafiğidir |
| `lakehouse-kafka-brokers` | 8443, 9090, 9091, 9093 | TCP | Kafka sunucuları, Strimzi operatörü | Kafka sunucuları arası ve operatör trafiği |
| `lakehouse-kafka-external-bootstrap`, `lakehouse-dual-role-0/1/2` | 9094 | TCP | Kafka'nın dış adresleri (§1) | yalnız Kafka'nın dış adresi açıksa |
| `connect-connect-api` | 8083 | HTTP | Strimzi operatörü, sorun araştırma | Kafka Connect REST API'si |
| `connect-connect` | 8083 | HTTP | Kafka Connect pod'ları | Connect pod'ları arası iletişim |
| `polaris-db-rw` / `-ro` / `-r` | 5432 | TCP | Polaris | katalog veritabanı (yazma / okuma kopyası / herhangi bir kopya) |
| `keycloak-db-rw` / `-ro` / `-r` | 5432 | TCP | Keycloak | kimlik veritabanı |
| `superset-db-rw` / `-ro` / `-r` | 5432 | TCP | Superset | Superset'in kendi veritabanı |
| `spark-operator-webhook-svc` | 9443 | HTTPS | Kubernetes API sunucusu | spark-operator'ün denetim ucu |
| `superset-operator-metrics` | 8443 | HTTPS | izleme | Superset operatörünün ölçümleri |
| `keycloak-operator` | 80 | HTTP | Kubernetes API sunucusu | Keycloak operatörünün servisi |

Spark işlerinin her koşusu için ayrıca adı `-ui-svc` ile biten, 4040 portlu geçici bir
servis görünür. Bunu spark-operator yaratır ve koşuyla birlikte siler; güvenlik duvarı
açısından bir anlamı yoktur.

Bazı ölçüm uçları servis değil **pod** düzeyindedir: Kafka sunucuları, Kafka ölçüm
toplayıcısı ve Kafka Connect pod'ları 9404 portunu açar ve OpenShift'in izlemesi bunları
doğrudan okur ([40-kurulum-sonrasi](../40-kurulum-sonrasi.md) §6).

Listeyi kendi kümenizden almak için:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get svc \
  -o custom-columns='AD:.metadata.name,TIP:.spec.type,PORT:.spec.ports[*].port' \
  | grep -v -- '-ui-svc'
```

**Beklenen çıktı** (örnek bir kurulumdan seçilmiş satırlar):

```text
AD                                                  TIP         PORT
connect-connect                                     ClusterIP   8083
connect-connect-api                                 ClusterIP   8083
keycloak-service                                    ClusterIP   8080,9000
lakehouse-kafka-bootstrap                           ClusterIP   9091,9093
lakehouse-kafka-brokers                             ClusterIP   9090,9091,8443,9093
maven-proxy                                         ClusterIP   8080
polaris                                             ClusterIP   8181
polaris-mgmt                                        ClusterIP   8182
superset-web-server                                 ClusterIP   8088
trino                                               ClusterIP   8080,8443
zeppelin                                            ClusterIP   8080
```

**Ters giderse:** beklenen bir servis listede yoksa ilgili uygulama henüz eşitlenmemiştir
(`oc -n "$ARGOCD_NS" get applications`).

---

## 3. Kümeden dışarıya (egress)

Bu bağlantılar güvenlik duvarında açılmalıdır. Kapalı ağda bunların yerine kurumun iç
aynaları kullanılır.

| Kimden | Kime | Port / protokol | Ne için | Zorunlu mu |
|---|---|---|---|---|
| Kafka Connect, Spark | kaynak PostgreSQL | 5432/TCP | değişiklik okuma | kaynak varsa |
| Kafka Connect, Spark | kaynak SQL Server | 1433/TCP | değişiklik okuma | kaynak varsa |
| Kafka Connect, Spark | kaynak MongoDB | 27017/TCP | değişiklik okuma | kaynak varsa |
| Polaris, Kafka Connect, Spark, not defterleri | S3 (`$S3_ENDPOINT`) | 443/HTTPS | Iceberg verisi | **evet** |
| PostgreSQL yedekleri | yedek S3 (`$S3_BUCKET_BACKUP`) | 443/HTTPS | veritabanı yedekleri | **evet** |
| Keycloak, Trino, Zeppelin | Active Directory (`$LDAP_URL`) | 636/LDAPS | kimlik ve grup okuma | **evet** |
| Kafka Connect (imaj üretimi), Maven önbelleği (`maven-proxy`), Zeppelin | Maven Central | 443/HTTPS | bağlayıcı ve sürücü dosyaları; Spark kütüphaneleri yalnız önbellekte yoksa (yeni bir sürümün ilk koşusu) | **evet** (ya da iç ayna) |
| JupyterHub not defterleri, dbt örneği | PyPI | 443/HTTPS | `pyiceberg`, `trino`, `dbt-trino` | not defteri ya da dbt kullanılacaksa |
| bütün bileşenler | quay.io, ghcr.io, docker.io, registry.k8s.io, `downloads.apache.org` | 443/HTTPS | imaj ve kurulum paketleri | **evet** (ya da iç ayna) |

Alan adlarının ayrıntılı listesi [20-on-kosullar](../20-on-kosullar.md) madde 10'dadır.

---

## 4. Dışarıdan kümeye (ingress)

| Kimden | Kime | Port / protokol | Ne için |
|---|---|---|---|
| tarayıcılar | router üzerinden Keycloak, Trino, Superset, JupyterHub, Zeppelin adresleri | 443/HTTPS | kullanıcı arayüzleri |
| dış SQL araçları (JDBC/ODBC) | Trino adresi | 443/HTTPS | SQL |
| Fluent Bit ajanları, olay üreticileri | Kafka'nın dış adresleri | 443/TLS | nginx günlükleri, uygulama ve IoT olayları (isteğe bağlı) |
| yönetim makinesi | Kubernetes API sunucusu | 6443/HTTPS | `oc` komutları |

---

## 5. Ağ politikaları

Ürün, `$LAKEHOUSE_NS` ad alanına şu ağ politikalarını yazar:

| Politika | Ne yapar |
|---|---|
| `default-deny-ingress` | ad alanındaki bütün pod'lara gelen trafiği varsayılan olarak kapatır |
| `allow-same-namespace` | aynı ad alanındaki pod'ların birbirine erişmesine izin verir |
| `allow-platform-namespaces` | ürünün ihtiyaç duyduğu platform ad alanlarından gelen trafiği açar: PostgreSQL operatörü (`cnpg-system`), ArgoCD (`argocdNamespace` ayarındaki ad alanı), OpenShift router'ı, OpenShift izlemesi; `velero.enabled` açıksa yedekleme (OADP) ad alanı |
| `notebook-s3-egress` | yalnız `s3.egressCidrs` ayarı doluysa: not defterlerinin kurumun iç S3'üne çıkmasına izin verir |

İlk üç politika yalnız **gelen** trafiği sınırlar; dışarıya giden trafik (kaynak
veritabanları, S3, Active Directory, Maven) serbesttir.

`argocdNamespace` ayarı `$ARGOCD_NS` ile aynı olmalıdır (OpenShift GitOps için
`openshift-gitops`). Farklı olursa kurulum yine çalışır, ama ArgoCD'den pod'lara doğrudan
erişim kapalı kalır.

Ad alanında bunların yanında operatörlerin kendi yarattığı politikalar da görünür (Kafka,
Kafka Connect, Keycloak ve JupyterHub için). JupyterHub'ın politikaları not defterlerinin
dışarıya çıkışını da daraltır: küme içinde yalnız Trino (8443) ve Polaris (8181) açıktır,
internet açık kalır.

Kümedeki politikaları görmek için:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get networkpolicy -o name
```

**Beklenen çıktı** (örnek bir kurulumdan; `notebook-s3-egress` yalnız `s3.egressCidrs`
doluysa vardır):

```text
networkpolicy.networking.k8s.io/allow-platform-namespaces
networkpolicy.networking.k8s.io/allow-same-namespace
networkpolicy.networking.k8s.io/connect-connect
networkpolicy.networking.k8s.io/default-deny-ingress
networkpolicy.networking.k8s.io/hub
networkpolicy.networking.k8s.io/keycloak-network-policy
networkpolicy.networking.k8s.io/lakehouse-entity-operator
networkpolicy.networking.k8s.io/lakehouse-kafka-exporter
networkpolicy.networking.k8s.io/lakehouse-network-policy-kafka
networkpolicy.networking.k8s.io/notebook-s3-egress
networkpolicy.networking.k8s.io/proxy
networkpolicy.networking.k8s.io/singleuser
```

Bir bağlantı sessizce zaman aşımına uğruyorsa nedenin ağ politikası olup olmadığını
anlamak için [50-isletme/sorun-giderme.md](../50-isletme/sorun-giderme.md) §5'e bakın.

---

## Kontrol listesi

- [ ] Altı arayüz adresi `oc get route` çıktısında ve DNS'te çözülüyor.
- [ ] Trino adresinin TLS türü bilerek seçildi (`reencrypt` ya da `passthrough`).
- [ ] §3'teki zorunlu çıkış bağlantıları güvenlik duvarında açık.
- [ ] Kafka'nın dış adresi yalnız nginx ya da olay akışı kullanılıyorsa açık.
- [ ] Ürünün ağ politikaları ad alanında var (`oc -n "$LAKEHOUSE_NS" get networkpolicy`).

## Sonraki bölüm

[10-planlama.md](../10-planlama.md) §4 — planlama özeti ve DNS/sertifika kararları.
Bileşen sürümleri: [surumler-ve-lisanslar.md](surumler-ve-lisanslar.md).
