# 90 — Teslim öncesi kontrol listesi (OpenShift kümesinde)

Bu sayfa, ürün müşteriye teslim edilmeden önce **gerçek OpenShift kümesinde** kanıtlanan
maddelerin tek listesidir. Her madde bir kutudur ve bir **komut + beklenen sonuç** taşır.

**Ne kadar sürer:** listenin tamamı 1–2 gün. Bekleyen maddeler çoğunlukla platform ve AD
ekiplerine bağlıdır.

**Hangi yetkiler gerekir:** kümede **cluster-admin**; AD ve depolama maddelerinde ilgili
ekiplerin desteği.

**Komutlar nerede çalışır:** `[bastion]` — `oc login` ile kümeye bağlı ve Git deposunun bir
kopyasının (`~/lakehouse`) bulunduğu yönetim makinesi. Madde 1.9'un ilk komutu
`[nginx ajan sunucusu]` üzerinde çalışır.

**Nasıl kullanılır:**

1. Kurulumu [30-kurulum](../30-kurulum.md) ile kurumun değerleriyle
   (`platform/values/site/`) yapın.
2. Listeyi **baştan sona** izleyin. Her maddeyi, kutusunu işaretleyerek **ve ölçtüğünüz
   çıktıyı yanına yazarak** kapatın. Kanıt çıktının kendisidir; "bakıldı" kanıt değildir.
3. Kapanış: `scripts/acceptance.sh` **9/9** ([kabul-testleri.md](kabul-testleri.md)) ve bu
   listede açık kutu kalmaması.

Listede **43 kutu** vardır. Bütün komutlar `install/lakehouse.env` yüklenmiş bir terminalde
çalıştırılır:

`[bastion]`

```bash
cd ~/lakehouse
set -a; . install/lakehouse.env; set +a
```

---

## 0. Küme ve araçlar

Bu beş madde [20-on-kosullar](../20-on-kosullar.md) bölümündeki denetimlerin gerçek kümede
tekrarıdır.

- [ ] **0.1 Küme sürümü ve cluster-admin**

  `[bastion]`

  ```bash
  oc version
  oc auth can-i '*' '*' --all-namespaces
  ```

  Beklenen: sunucunun Kubernetes sürümü **1.33 ya da üstü** (OpenShift 4.20 ve üstü) ve son
  satır `yes`. Ölçülen sürümü buraya yazın.
  *Kaynak: [20-on-kosullar](../20-on-kosullar.md) madde 1.*

- [ ] **0.2 GitOps operatörü kurulu ve `Succeeded`**

  `[bastion]`

  ```bash
  oc get csv -n "$ARGOCD_NS"
  ```

  Beklenen: GitOps operatörünün satırında `PHASE` **`Succeeded`**. Sürüm numarasını buraya
  yazın.
  *Kaynak: [20-on-kosullar](../20-on-kosullar.md) madde 2.1.*

- [ ] **0.3 GitOps denetleyicisinin küme geneli yetkisi**

  `[bastion]`

  ```bash
  oc auth can-i create customresourcedefinitions \
    --as=system:serviceaccount:"$ARGOCD_NS":openshift-gitops-argocd-application-controller
  ```

  Beklenen: `yes`. Servis hesabının adı GitOps sürümüne göre değişebilir; gerçek adı buraya
  yazın.
  *Kaynak: [20-on-kosullar](../20-on-kosullar.md) madde 2.2.*

- [ ] **0.4 DNS ve joker sertifika**

  `[bastion]`

  ```bash
  getent hosts "trino-$LAKEHOUSE_NS.$APPS_DOMAIN"
  echo | openssl s_client -connect "trino-$LAKEHOUSE_NS.$APPS_DOMAIN:443" \
    -servername "trino-$LAKEHOUSE_NS.$APPS_DOMAIN" 2>/dev/null \
    | openssl x509 -noout -issuer -dates
  ```

  Beklenen: ad çözülüyor; router'ın sunduğu sertifika kurumun güvendiği bir kökten geliyor
  ve süresi geçerli.
  *Kaynak: [20-on-kosullar](../20-on-kosullar.md) madde 7.*

- [ ] **0.5 Yönetim makinesinde araçlar tam**

  `[bastion]`

  ```bash
  for t in oc kubectl aws ldapsearch getent curl openssl python3 git htpasswd jq; do
    command -v "$t" >/dev/null && echo "OK    $t" || echo "EKSIK $t"
  done
  ```

  Beklenen: on bir satırın hepsi `OK`. Eksik olan aracın hangi adımda gerektiği
  [20-on-kosullar](../20-on-kosullar.md) girişindeki araç tablosundadır.
  *Kaynak: [20-on-kosullar](../20-on-kosullar.md) araç tablosu.*

---

## 1. Kimlik ve ağ

- [ ] **1.1 Tarayıcıdan OIDC girişi (Superset, Trino, JupyterHub, Zeppelin)**

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" get route superset trino jupyterhub zeppelin keycloak \
    -o custom-columns='AD:.metadata.name,HOST:.spec.host,TLS:.spec.tls.termination'
  ```

  Beklenen: beş Route da kurumun alan adıyla listeleniyor. Tarayıcıdan her birine girişte
  Keycloak'a yönlenme, geri dönüşte oturum açılması ve grup → rol eşlemesi çalışıyor
  (`lakehouse-analysts` → Superset `Alpha`, JupyterHub izinli gruplar, Zeppelin `analyst`).
  Superset SQL Lab'da `select 1` sonuç döndürüyor.
  *Kaynak: [40-kurulum-sonrasi](../40-kurulum-sonrasi.md) §5.2 ve §5.3.*

- [ ] **1.2 Trino Route'u `reencrypt`**

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" get route trino -o jsonpath='{.spec.tls.termination}{"\n"}'
  oc -n "$LAKEHOUSE_NS" get route trino \
    -o jsonpath='{.spec.tls.destinationCACertificate}' | head -1
  curl -sSI "https://$(oc -n "$LAKEHOUSE_NS" get route trino -o jsonpath='{.spec.host}')/v1/info"
  ```

  Beklenen: `reencrypt`; ikinci komut `-----BEGIN CERTIFICATE-----` ile başlıyor; üçüncü
  komut gerçek bir HTTP yanıtı veriyor (TLS hatası değil). `tls.caBundle` boş bırakılırsa
  Route `passthrough` olur ve **tarayıcının** ürünün iç kök sertifikasına güvenmesi gerekir.
  *Kaynak: [30-kurulum](../30-kurulum.md) §5.11;
  [values-anahtarlari.md](values-anahtarlari.md) → `tls.caBundle`.*

- [ ] **1.3 JupyterHub hub'ı Keycloak'a güveniyor**

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" get deploy hub \
    -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="REQUESTS_CA_BUNDLE")]}{"\n"}'
  KC=$(oc -n "$LAKEHOUSE_NS" get route keycloak -o jsonpath='{.spec.host}')
  oc -n "$LAKEHOUSE_NS" exec deploy/hub -- python -c \
    "import requests;print(requests.get('https://$KC/realms/lakehouse/.well-known/openid-configuration').status_code)"
  ```

  Beklenen: ilk komut **boş** (bu değişken hub pod'unda bilerek yoktur; Keycloak Route'unun
  sertifikası kurumsal ya da genel bir CA ile doğrulanır) ve ikinci komut **200**.
  *Kaynak: [40-kurulum-sonrasi](../40-kurulum-sonrasi.md) §5.6, ikinci madde.*

- [ ] **1.4 Trino grup sağlayıcısı AD'ye karşı çalışıyor**

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" exec deploy/trino-coordinator -- \
    grep -c group-provider.name=ldap /etc/trino/group-provider.properties
  oc -n "$LAKEHOUSE_NS" logs deploy/trino-coordinator | grep -i "ldap\|group" | tail -20
  ```

  Beklenen: `1`. Ardından bir AD kullanıcısı kendi kimliğiyle `SHOW SCHEMAS FROM lakehouse`
  çalıştırır ve grup kuralları uygulanır. Trino grubu **CN** olarak alır; AD grubunun CN
  değeri `lakehouse-analysts` gibi olmalıdır.
  *Kaynak: [50-isletme/kullanici-ve-yetki.md](../50-isletme/kullanici-ve-yetki.md) §2 ve §4.1.*

- [ ] **1.5 Keycloak AD federasyonu ve bağlanma parolası**

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" get keycloakrealmimport lakehouse \
    -o jsonpath='{.status.conditions[?(@.type=="Done")].status}{"\n"}'
  oc -n "$LAKEHOUSE_NS" get secret keycloak-clients -o jsonpath='{.data.ldap-bind}' | wc -c
  ```

  Beklenen: `True`; ikinci komut sıfırdan büyük bir sayı yazıyor (Secret anahtarı dolu).
  Keycloak yönetim arayüzünde **User Federation** bağlantısı çalışıyor ve kullanıcılar
  görünüyor.
  *Kaynak: [30-kurulum](../30-kurulum.md) §5.6.*

- [ ] **1.6 Sertifika yenilemesi kesintisiz**

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" get certificate trino-tls \
    -o custom-columns='HAZIR:.status.conditions[0].status,BITIS:.status.notAfter,YENILEME:.status.renewalTime'
  oc -n "$LAKEHOUSE_NS" get pod -l app.kubernetes.io/name=trino \
    -o custom-columns='POD:.metadata.name,YENIDEN:.status.containerStatuses[0].restartCount'
  ```

  Beklenen: sertifika kendiliğinden yenilenir; yenileme anında Trino pod'larının
  **yeniden başlatma sayısı artmaz** ve açık oturumlar düşmez. Denemek için sertifikayı
  elle yeniletip sayacı yeniden okuyun.
  *Kaynak:
  [50-isletme/gunluk-haftalik-kontroller.md](../50-isletme/gunluk-haftalik-kontroller.md) §4.3;
  [50-isletme/sorun-giderme.md](../50-isletme/sorun-giderme.md) §5.*

- [ ] **1.7 Ağ politikaları: API sunucusu → webhook yolu**

  `[bastion]`

  ```bash
  oc get validatingwebhookconfiguration,mutatingwebhookconfiguration \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{range .webhooks[*]}{.clientConfig.service.namespace}/{.clientConfig.service.name}{" "}{end}{"\n"}{end}'
  oc -n "$LAKEHOUSE_NS" get networkpolicy
  oc -n "$LAKEHOUSE_NS" apply --dry-run=server -f test/e2e/cnpg-restore.yaml
  oc -n "$LAKEHOUSE_NS" get sparkapplication
  ```

  Beklenen: `$LAKEHOUSE_NS` ad alanına bakan bir webhook servisi **yok** (webhook'lar
  operatörlerin kendi ad alanlarındadır); sunucu tarafı deneme (`--dry-run=server`) **kabul
  edilir** ve Spark işleri normal çalışır. Reddedilirse ürün ekibine iletin.
  *Kaynak: [port-ve-servisler.md](port-ve-servisler.md) §5.*

- [ ] **1.8 Kafka dış adresi (nginx ya da JSON akışı kullanılacaksa)**

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" get kafka lakehouse \
    -o jsonpath='{range .status.listeners[*]}{.name}{"\t"}{.bootstrapServers}{"\n"}{end}'
  oc -n "$LAKEHOUSE_NS" get route -l strimzi.io/cluster=lakehouse
  ```

  Beklenen: dış adres için bir bootstrap Route'u (TLS geçişli) ve broker başına birer
  Route; dışarıdan `SCRAM-SHA-512` ile bağlanılabiliyor.
  *Kaynak: [20-on-kosullar](../20-on-kosullar.md) madde 8;
  [50-isletme/yeni-kaynak-ve-pipeline.md](../50-isletme/yeni-kaynak-ve-pipeline.md) §10.*

- [ ] **1.9 Günlük ajanı gerçek nginx sunucusunda**

  `[nginx ajan sunucusu]`

  ```bash
  systemctl status fluent-bit
  ```

  `[bastion]`

  ```bash
  scripts/kafka-cli.sh kafka-consumer-groups.sh --describe --group connect-sink-nginx
  ```

  Beklenen: ajan `active (running)`; tüketici grubunda `LOG-END-OFFSET` artıyor, `LAG`
  sıfıra yakın; ham tabloda satır sayısı artıyor ve `nginx.dlq` boş kalıyor.
  *Kaynak: [50-isletme/yeni-kaynak-ve-pipeline.md](../50-isletme/yeni-kaynak-ve-pipeline.md)
  §10.*

- [ ] **1.10 AD kök sertifikasıyla gerçek LDAPS bağlantısı**

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" exec keycloak-0 -- ls /opt/keycloak/conf/truststores
  oc -n "$LAKEHOUSE_NS" logs keycloak-0 | grep TruststoreBuilder | tail -1
  oc -n "$LAKEHOUSE_NS" logs deploy/trino-coordinator -c s3-truststore | grep "AD CA"
  oc -n "$LAKEHOUSE_NS" exec deploy/zeppelin -c zeppelin -- \
    keytool -list -keystore /truststore/ad-truststore.p12 -storetype PKCS12 \
    -storepass changeit | grep ad-ca
  ```

  Beklenen: dört komut da dolu yanıt verir. **Ardından** gerçek bir AD hesabıyla Keycloak
  eşitlemesi, Trino'da grup çözümü ve Zeppelin girişi **sertifika zinciri hatası olmadan**
  tamamlanır. Şüphede kalırsanız önce `openssl s_client -connect` ile sunucu zincirinin bu
  kökle bittiğini görün ([20-on-kosullar](../20-on-kosullar.md) madde 6).
  *Kaynak: [30-kurulum](../30-kurulum.md) §5.13;
  [secret-listesi.md](secret-listesi.md).*

- [ ] **1.11 JupyterHub ara sunucu jetonunun yenilenmesi**

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" delete secret hub
  oc -n "$ARGOCD_NS" annotate application jupyterhub argocd.argoproj.io/refresh=hard --overwrite
  oc -n "$ARGOCD_NS" patch application jupyterhub --type merge -p '{"operation":{"sync":{}}}'
  oc -n "$LAKEHOUSE_NS" get secret hub -o jsonpath='{.metadata.creationTimestamp}{"\n"}'
  ```

  Beklenen: Secret yeniden üretilir (yeni zaman damgası), hub ayağa kalkar ve bir kullanıcı
  not defteri açabilir. **Yalnız eşitleme yetmez**: `refresh=hard` açıklaması olmadan Secret
  yeniden üretilmez. Bu deneme bir kez yapılır ve sonucu buraya yazılır.
  *Kaynak: [50-isletme/kullanici-ve-yetki.md](../50-isletme/kullanici-ve-yetki.md) §9.*

- [ ] **1.12 Ağ politikasındaki ArgoCD ad alanı**

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" get networkpolicy allow-platform-namespaces \
    -o jsonpath='{range .spec.ingress[0].from[*]}{.namespaceSelector.matchLabels}{"\n"}{end}'
  echo "$ARGOCD_NS"
  ```

  Beklenen: listede görünen ArgoCD ad alanı `echo "$ARGOCD_NS"` çıktısıyla **aynıdır**
  (OpenShift GitOps'ta `openshift-gitops`). Değer `platform/values/site/glue.yaml`
  dosyasındaki `argocdNamespace` satırından gelir; uyuşmuyorsa o satırı `$ARGOCD_NS` ile
  eşitleyip değişikliği gönderin.
  *Kaynak: [port-ve-servisler.md](port-ve-servisler.md) §5;
  [values-anahtarlari.md](values-anahtarlari.md) §1.*

---

## 2. İzleme ve günlükler

- [ ] **2.1 Kullanıcı iş yükü izlemesi açık**

  `[bastion]`

  ```bash
  oc -n openshift-monitoring get cm cluster-monitoring-config -o yaml
  oc -n openshift-user-workload-monitoring get pods
  ```

  Beklenen: ConfigMap'in gövdesinde `enableUserWorkload: true`; izleme pod'ları `Running`.
  **ConfigMap zaten varsa üzerine yazılmaz, düzenlenir**: düz bir uygulama kümenin diğer
  izleme ayarlarını siler.
  *Kaynak: [20-on-kosullar](../20-on-kosullar.md) madde 9.1.*

- [ ] **2.2 İzleme nesneleri toplanıyor**

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" get podmonitor,servicemonitor,prometheusrule -o name
  TOKEN=$(oc whoami -t)
  HOST=$(oc -n openshift-monitoring get route thanos-querier -o jsonpath='{.spec.host}')
  curl -sSk -H "Authorization: Bearer $TOKEN" \
    --data-urlencode "query=up{namespace=\"$LAKEHOUSE_NS\"} == 1" "https://$HOST/api/v1/query" \
    | jq -r '.data.result[].metric.job' | sort | uniq -c
  ```

  **Beklenen çıktı** (örnek bir kurulumdan):

  ```text
  podmonitor.monitoring.coreos.com/kafka-resources-metrics
  podmonitor.monitoring.coreos.com/spark-operator-podmonitor
  servicemonitor.monitoring.coreos.com/polaris
  prometheusrule.monitoring.coreos.com/lakehouse
        6 lakehouse/kafka-resources-metrics
        2 lakehouse/spark-operator-podmonitor
        1 polaris-mgmt
  ```

  `kafka-resources-metrics` 6 hedeftir: üç Kafka broker'ı, Kafka'nın ölçüm aktarıcısı
  (`kafka-exporter`) ve iki Connect işçisi. Trino, Superset, JupyterHub ve Zeppelin
  hedefleri **bilerek yoktur**. `kube-state-metrics` bu listede görünmez: OpenShift'te
  platform izlemesinin parçasıdır; iki Spark alarmı onun serilerini kullanır.
  *Kaynak: [50-isletme/izleme-ve-alarmlar.md](../50-isletme/izleme-ve-alarmlar.md) §2;
  [40-kurulum-sonrasi](../40-kurulum-sonrasi.md) §6.2.*

- [ ] **2.3 Altı alarm kuralı sağlıklı**

  `[bastion]`

  ```bash
  TOKEN=$(oc whoami -t)
  HOST=$(oc -n openshift-monitoring get route thanos-querier -o jsonpath='{.spec.host}')
  curl -sSk -H "Authorization: Bearer $TOKEN" "https://$HOST/api/v1/rules" \
    | jq -r '.data.groups[] | select(.name=="lakehouse") | .rules[] | "\(.name)\t\(.health)"'
  ```

  Beklenen: altı kural da `ok` ve hiçbiri boş yere `firing` değil.
  `monitoring.silverMergeStaleSeconds` gerçek Silver birleştirme aralığına göre ayarlanmış
  olmalı.
  *Kaynak: [50-isletme/izleme-ve-alarmlar.md](../50-isletme/izleme-ve-alarmlar.md) §3 ve §4.*

- [ ] **2.4 Alarm yönlendirmesi ve bildirimi (platformun işi)**

  `[bastion]`

  ```bash
  oc -n openshift-monitoring get secret alertmanager-main \
    -o jsonpath='{.data.alertmanager\.yaml}' | base64 -d | head -40
  ```

  Beklenen: `Lakehouse` ile başlayan alarmları yakalayan bir yönlendirme ve alıcı (e-posta,
  mesajlaşma, çağrı sistemi) tanımlı ve **deneme bildirimi ulaşıyor**.
  *Kaynak: [50-isletme/izleme-ve-alarmlar.md](../50-isletme/izleme-ve-alarmlar.md) §5.*

- [ ] **2.5 Günlük toplama `$LAKEHOUSE_NS` ad alanını kapsıyor**

  `[bastion]`

  ```bash
  oc -n openshift-logging get lokistack,clusterlogforwarder
  ```

  Beklenen: `LokiStack` `Ready`; yönlendirici `lakehouse` ad alanını kapsıyor; konsolun
  **Observe → Logs** sekmesinde örnek sorgular sonuç veriyor. Loki ürünle gelmez; platformun
  parçasıdır ve zorunlu değildir.
  *Kaynak: [50-isletme/sorun-giderme.md](../50-isletme/sorun-giderme.md) §9;
  [20-on-kosullar](../20-on-kosullar.md) madde 9.3.*

- [ ] **2.6 Kabul koşusunun izleme testi geçiyor**

  [kabul-testleri.md](kabul-testleri.md) §3'teki değişkenlerle (`PROM_URL`, `PROM_TOKEN`,
  `GRAFANA_SKIP=1`) çalıştırılan kabul koşusunda izleme testi geçiyor.

  Beklenen: özette `E2E F5 MONITORING OK` satırı; ayrıntılı satırlar
  [kabul-testleri.md](kabul-testleri.md) §6'dadır.
  *Kaynak: [kabul-testleri.md](kabul-testleri.md) §6.*

---

## 3. Yedekleme (OADP)

- [ ] **3.1 `DataProtectionApplication` alan adları kurulu sürümle uyumlu**

  `[bastion]`

  ```bash
  oc explain dataprotectionapplication.spec --recursive | head -60
  oc -n openshift-adp get dpa \
    -o custom-columns='AD:.metadata.name,UYGULANDI:.status.conditions[?(@.type=="Reconciled")].status'
  ```

  Beklenen: örnek tanımdaki alanlar (`backupLocations[].velero.objectStorage`,
  `configuration.velero.defaultPlugins`, `configuration.nodeAgent`) kurulu OADP sürümünde
  var; nesne `True`.
  *Kaynak: [50-isletme/yedek-ve-geri-donus.md](../50-isletme/yedek-ve-geri-donus.md) §7.1.*

- [ ] **3.2 Depolama konumu `Available`, sonra site değerinde açma**

  `[bastion]`

  ```bash
  oc -n openshift-adp get backupstoragelocation
  oc -n openshift-adp get schedules.velero.io lakehouse-daily
  ```

  Beklenen: depolama konumu `Available`; site değeri açıldıktan sonra zamanlı yedek nesnesi
  **`openshift-adp`** ad alanında görünüyor. Sıra ters olursa `glue` uygulaması `Degraded`
  olur.
  *Kaynak: [50-isletme/yedek-ve-geri-donus.md](../50-isletme/yedek-ve-geri-donus.md) §7.2;
  [20-on-kosullar](../20-on-kosullar.md) madde 9.2.*

- [ ] **3.3 Disk içeriği gerçekten yedekleniyor**

  `[bastion]`

  ```bash
  oc get volumesnapshotclass
  oc -n openshift-adp get podvolumebackups.velero.io
  oc -n openshift-adp get backups.velero.io \
    -o custom-columns='AD:.metadata.name,FAZ:.status.phase,OGE:.status.progress.itemsBackedUp'
  ```

  Beklenen: JupyterHub hub diski, kullanıcı diskleri ve Zeppelin diski için dosya sistemi
  yedeği ya da anlık görüntü üretiliyor. Önerilen yol CSI anlık görüntüsüdür.
  *Kaynak: [50-isletme/yedek-ve-geri-donus.md](../50-isletme/yedek-ve-geri-donus.md) §6.3 ve
  §6.5.*

- [ ] **3.4 Yedek dışı bırakılan diskler doğru**

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" get pod lakehouse-dual-role-0 \
    -o jsonpath='{.metadata.annotations.backup\.velero\.io/backup-volumes-excludes}{"\n"}'
  oc -n "$LAKEHOUSE_NS" get pod -l cnpg.io/cluster=polaris-db \
    -o jsonpath='{.items[0].metadata.annotations.backup\.velero\.io/backup-volumes-excludes}{"\n"}'
  ```

  Beklenen: sırasıyla `data-0` ve `pgdata`. Açıklama diskin değil, **pod'daki hacmin** adını
  taşır.
  *Kaynak: [50-isletme/yedek-ve-geri-donus.md](../50-isletme/yedek-ve-geri-donus.md) §2.*

- [ ] **3.5 Veritabanı geri dönüş denemesi kurumun yedek bucket'ından**

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" get cluster \
    -o custom-columns='AD:.metadata.name,KOPYA:.spec.instances,ARSIV:.status.conditions[?(@.type=="ContinuousArchiving")].status'
  oc -n "$LAKEHOUSE_NS" get backups.postgresql.cnpg.io \
    -o custom-columns='AD:.metadata.name,FAZ:.status.phase,YONTEM:.status.method'
  ```

  Beklenen: üç veritabanında da arşivleme `True`, günlük yedekler `completed`. Ardından
  [yedek-ve-geri-donus.md](../50-isletme/yedek-ve-geri-donus.md) §5.1'deki yordamla bir yedek
  **ayrı** bir veritabanı kümesine geri yükleniyor ve tablo sayımı tutuyor. Geri yükleme
  kümesi deneme sonunda **silinir**.
  *Kaynak: [50-isletme/yedek-ve-geri-donus.md](../50-isletme/yedek-ve-geri-donus.md) §5.*

- [ ] **3.6 Kabul koşusunun yedek testi geçiyor**

  [kabul-testleri.md](kabul-testleri.md) §3'teki komutla, `--skip dr` **olmadan** ve
  `--velero-ns openshift-adp` ile çalıştırılan kabul koşusunda yedek testi geçiyor.

  Beklenen: özette `E2E F5 DR OK` ve `KABUL: 9/9 yol geçti`.
  *Kaynak: [kabul-testleri.md](kabul-testleri.md) §3.*

---

## 4. Kaynaklar, imajlar, dış bağımlılıklar

- [ ] **4.1 SQL Server kaynağı çalışıyor**

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" get kafkaconnector \
    -o custom-columns='AD:.metadata.name,HAZIR:.status.conditions[?(@.type=="Ready")].status'
  oc -n "$LAKEHOUSE_NS" get kafkaconnector dbz-erp \
    -o jsonpath='{.spec.config.database\.encrypt}{"\n"}'
  oc -n "$LAKEHOUSE_NS" logs -l strimzi.io/kind=KafkaConnect --tail=500 \
    | grep -i "schema.history\|truststore\|encrypt" | tail
  ```

  Beklenen: bağlayıcı `True`; ikinci komut `true` (bağlantı şifreli). Sertifika doğrulamasını
  kapatan `trustServerCertificate: true` **yalnız** deneme ortamında kabul edilir; üretimde
  truststore verilir. `dbz-erp` yerine kendi kaynağınızın adını yazın.
  *Kaynak: [50-isletme/yeni-kaynak-ve-pipeline.md](../50-isletme/yeni-kaynak-ve-pipeline.md)
  §3.2.*

- [ ] **4.2 Connect imajı iç registry'ye gönderiliyor**

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" get kafkaconnect connect -o jsonpath='{.spec.build.output}{"\n"}'
  oc -n "$LAKEHOUSE_NS" get secret connect-push -o jsonpath='{.type}{"\n"}'
  oc -n "$LAKEHOUSE_NS" get kafkaconnect connect \
    -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}{"\n"}'
  ```

  Beklenen: adres `$INTERNAL_REGISTRY/$LAKEHOUSE_NS/connect:<sürüm>` biçiminde (`:latest`
  **kullanılmaz**); Secret türü `kubernetes.io/dockerconfigjson`; imaj yapımından (~10
  dakika) sonra `True`.
  *Kaynak: [20-on-kosullar](../20-on-kosullar.md) madde 5;
  [50-isletme/yukseltme.md](../50-isletme/yukseltme.md) §4.3.*

- [ ] **4.3 Superset imajı kurumun aynasında ve etiketi sabit**

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" get superset superset -o jsonpath='{.spec.image}{"\n"}'
  oc -n "$LAKEHOUSE_NS" get pod -l instance=superset \
    -o jsonpath='{.items[0].status.containerStatuses[0].imageID}{"\n"}'
  ```

  Beklenen: imaj kurumun aynasından çekiliyor ve etiketi aynada değiştirilemez biçimde
  kilitli. Superset operatörü yalnız `depo:etiket` biçimini kabul eder; etiketin sabit
  kalması aynanın sorumluluğudur. Ölçülen `imageID` değerini buraya yazın.
  *Kaynak: [50-isletme/yukseltme.md](../50-isletme/yukseltme.md) §4.5;
  [surumler-ve-lisanslar.md](surumler-ve-lisanslar.md).*

- [ ] **4.4 Maven önbelleği ve PyPI erişimi ya da iç ayna**

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" get pod -l app=maven-proxy
  oc -n "$LAKEHOUSE_NS" get scheduledsparkapplication silver-merge \
    -o jsonpath='{.spec.template.sparkConf.spark\.jars\.ivySettings}{"\n"}'
  ```

  Beklenen: `maven-proxy` pod'u `Running` ve ikinci komut `/opt/ivy/ivysettings.xml`
  yazıyor. Önbellek Maven Central'a (ya da `mavenProxy.upstream` ile verilen kurum aynasına)
  kurulumda ve sürüm yükseltmesinden sonraki ilk koşuda erişebilmelidir. Zeppelin'in JDBC
  sürücüsü, JupyterHub ve dbt örneğinin paket kurulumları da dışarı çıkar; kapalı ağda iç
  PyPI aynası da gerekir.
  *Kaynak: [50-isletme/sorun-giderme.md](../50-isletme/sorun-giderme.md) §6.2;
  [20-on-kosullar](../20-on-kosullar.md) madde 10.*

- [ ] **4.5 Depolama sınıfları ve boyutlandırma**

  `[bastion]`

  ```bash
  oc get storageclass
  oc -n "$LAKEHOUSE_NS" get pvc \
    -o custom-columns='AD:.metadata.name,SINIF:.spec.storageClassName,BOYUT:.spec.resources.requests.storage'
  ```

  Beklenen: Kafka ve veritabanı diskleri ortamın **CSI** depolama sınıfında (madde 3.3 buna
  bağlıdır); disk boyutları veri hacmine uygun. Hacim küçük kademeyi aşıyorsa büyük kademe
  değerleri açılır.
  *Kaynak: [10-planlama](../10-planlama.md) §2;
  [values-anahtarlari.md](values-anahtarlari.md) §1.*

- [ ] **4.6 Superset uyarı ve rapor özelliği kararı**

  Şu an **kapalı**. Açılacaksa bir kuyruk, ayrı bir işçi ve başsız tarayıcı gerekir; karar
  teslimden önce müşteriyle verilir. Açılırsa kanıtı bu maddenin altına yazılır.
  *Kaynak: [40-kurulum-sonrasi](../40-kurulum-sonrasi.md) §5.6, dördüncü madde.*

- [ ] **4.7 "Tablo sağlığı" panosu içe aktarıldı ve açılıyor**

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" exec deploy/superset-web-server -- python3 -c "
  from superset.app import create_app
  with create_app().app_context():
      from superset import db
      from superset.models.dashboard import Dashboard
      print([(d.dashboard_title, d.slug) for d in db.session.query(Dashboard).all()])" \
    2>/dev/null | tail -1
  ```

  Beklenen: listede `('Tablo sağlığı', 'tablo-sagligi')` var ve pano açıldığında "son durum"
  grafiği **kurumun tablolarını** gösteriyor (ilk pipeline'dan en geç bir saat sonra).
  *Kaynak: [40-kurulum-sonrasi](../40-kurulum-sonrasi.md) §4;
  [50-isletme/veri-metrikleri.md](../50-isletme/veri-metrikleri.md) §4.*

- [ ] **4.8 İç registry adresi ve ImageStream**

  `[bastion]`

  ```bash
  oc registry info --internal
  oc -n "$LAKEHOUSE_NS" get imagestream connect
  ```

  Beklenen: ilk komutun çıktısı `$INTERNAL_REGISTRY` değeriyle **aynı**. `--internal`
  bayrağı şarttır: bayraksız komut, registry dışarı açılmışsa genel adresi yazar.
  `ImageStream` ilk gönderimde kendiliğinden oluşmadıysa elle yaratılır
  (`oc -n "$LAKEHOUSE_NS" create imagestream connect`).
  *Kaynak: [20-on-kosullar](../20-on-kosullar.md) madde 5.*

- [ ] **4.9 Gönderme jetonunun gerçek süresi**

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" get secret connect-push \
    -o jsonpath='{.data.\.dockerconfigjson}' | base64 -d \
    | python3 -c 'import base64, json, sys, datetime
  cfg = json.load(sys.stdin)["auths"]
  tok = list(cfg.values())[0]["password"]
  t = tok.split(".")[1]; t += "=" * (-len(t) % 4)
  print("jeton bitisi:", datetime.datetime.fromtimestamp(
      json.loads(base64.urlsafe_b64decode(t))["exp"], datetime.timezone.utc))'
  ```

  Beklenen: bir yıl istense de küme daha kısa bir üst sınır uygulayabilir; **gerçek** süre
  bu çıktıdan okunur ve kurumun takvimine yazılır.
  *Kaynak: [20-on-kosullar](../20-on-kosullar.md) madde 5;
  [50-isletme/yukseltme.md](../50-isletme/yukseltme.md) §6.*

- [ ] **4.10 Kafka Connect iki işçiyle ve yeterli bellekle çalışıyor**

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" get pod -l strimzi.io/kind=KafkaConnect \
    -o custom-columns='POD:.metadata.name,DURUM:.status.phase,BELLEK:.spec.containers[0].resources.limits.memory'
  ```

  **Beklenen çıktı** (örnek bir kurulumdan; ürünün varsayılanı 2 işçi × 4 GiB):

  ```text
  POD                 DURUM     BELLEK
  connect-connect-0   Running   4Gi
  connect-connect-1   Running   4Gi
  ```

  Pod'ların yeniden başlama sayısı artmıyor olmalıdır
  (`oc -n "$LAKEHOUSE_NS" get pod -l strimzi.io/kind=KafkaConnect` çıktısındaki `RESTARTS`).
  Kaynak ve tablo sayısı büyükse bellek [10-planlama](../10-planlama.md) §2'deki kademeye
  göre artırılır.
  *Kaynak: [10-planlama](../10-planlama.md) §2;
  [50-isletme/sorun-giderme.md](../50-isletme/sorun-giderme.md#connect-oom) §4.1.*

- [ ] **4.11 Elle yaratılmış her Secret'ın belgesi var**

  `[bastion]`

  ```bash
  scripts/nesne-kaynagi.sh --liste secret | awk '$2=="ETİKETSİZ"'
  ```

  Beklenen: **hiçbir satır yok**. Bir satır görünürse o Secret hiçbir belge adımıyla
  yaratılmamıştır; kimin ve neden yarattığını öğrenip ya silin ya da yaratıldığı belge
  adımını gösteren etiketi ekleyin.
  *Kaynak: [50-isletme/degisiklik-nasil-uygulanir.md](../50-isletme/degisiklik-nasil-uygulanir.md)
  §7.*

---

## 5. Kararlar (kanıt değil, gerekçe)

Bu üç başlıkta kutu yoktur; tasarım kararlarının gerekçesini ve yeniden ele alınma
koşulunu kaydeder.

### 5.1 Kesinti bütçeleri (PodDisruptionBudget) operatörlere bırakılır

**Karar:** ürün kendisi hiçbir bileşen için `PodDisruptionBudget` yazmaz.

**Gerekçe.**

1. Gerekli olanları operatörler kendisi yaratır: PostgreSQL kümeleri (iki kopya, CNPG),
   Kafka (üç broker, Strimzi) ve Kafka Connect (iki işçi, Strimzi). Strimzi'nin bütçesi
   Connect işçilerini düğüm boşaltmada **tek tek** indirir; görevler diğer işçiye geçer.
   Elle eklenen bir bütçe bunlarla çakışır.
2. Geri kalan bileşenler **tek kopyalıdır** (Trino koordinatörü, Polaris, Superset,
   JupyterHub hub, Zeppelin). Tek kopyalı bir bileşene "en az biri ayakta kalsın" demek
   **düğüm boşaltmayı kilitler**: yama sırasında boşaltma hiç ilerlemez.
3. Trino çalışanları durumsuzdur; düşen sorgu yeniden çalıştırılır, veri kaybı olmaz.

**Ne zaman yeniden ele alınır:** bir bileşende yüksek erişilebilirlik isteniyorsa önce
**kopya sayısı** artırılır ve bütçe kararı o değişiklikle birlikte verilir. Özet tablo:
[surumler-ve-lisanslar.md](surumler-ve-lisanslar.md) §6.

### 5.2 Kafka verisi yedeklenmez, ikinci site kurulmaz

**Karar:** Kafka konu verisi yedeklenmez ve siteler arası çoğaltma kurulmaz.

**Gerekçe.** Kafka bu mimaride **taşıyıcıdır**, kalıcı kayıt yeri değildir: kalıcı veri
kaynak veritabanlarında ve Iceberg/S3'tedir. Broker verisi kaybolursa doğru dönüş yolu
kaynaktan yeniden okumaktır; bu, yedekten dönen eski konum ve şema geçmişiyle çalışmaktan
daha tutarlıdır. Siteler arası çoğaltma bir yedekleme aracı **değildir**; ikinci bir aktif
küme gerekmeden kurulması ek broker, ayrı yetki ve sürekli çift trafik demektir.

**Ne zaman yeniden ele alınır:** kurum ikinci bir site (aktif-aktif ya da sıcak yedek)
isterse. Kapsam tablosu:
[50-isletme/yedek-ve-geri-donus.md](../50-isletme/yedek-ve-geri-donus.md) §2.

### 5.3 Superset şema geçişinin deneme sayısı yüksek tutulur

**Karar:** Superset'in veritabanı şema geçiş işi için yüksek deneme sayısı korunur; ek bir
bekleme mekanizması yazılmaz.

**Gerekçe.** Operatörün geçiş işi veritabanı hazır olmadan başlar; varsayılan üç deneme
saniyeler içinde tükenir ve iş hata durumunda kalır. Operatör, işi veritabanına bağlayacak
bir alan sunmaz; elimizdeki tek ayar deneme sayısıdır. Yüksek deneme sayısı, taze kümede
veritabanının hazır olma süresini (~4 dakika) rahatça kapsar. Teslim öncesi denetim:
`oc -n "$LAKEHOUSE_NS" get jobs -l instance=superset` → şema geçiş işi **Complete**.

---

## 6. Kabul ve teslim

- [ ] **6.1 Kabul koşusu ve kanıt paketi**

  [kabul-testleri.md](kabul-testleri.md) (madde ↔ kanıt tablosu) ve `scripts/acceptance.sh`
  çıktısı. Koşu [kabul-testleri.md](kabul-testleri.md) §3'teki komutla yapılır; çıktı
  müşteriye teslim edilen kanıt paketine konur.

- [ ] **6.2 Bu listede açık kutu kalmadı**

  Kapatılamayan madde varsa kabulden önce **yazılı** olarak, riskiyle birlikte kayda
  geçirilir.

- [ ] **6.3 Belge denemesi — kurulum belgeleri gerçekten izlenebiliyor mu**

  Kurulumu **hiç yapmamış** bir kişi, yalnız `docs/` altındaki belgeleri okuyarak
  kurulumu baştan sona tamamlayabilmelidir:

  1. Okuyucu 00 → 10 → 20 → 30 → 40 sırasını izler ve her adımda **yalnız** yazılanı yapar.
  2. Takıldığı, tahmin etmek zorunda kaldığı ya da çıktısı belgedekiyle uyuşmayan her
     nokta not edilir.
  3. Notlar belgeye düzeltme olarak işlenir ve deneme düzeltilmiş metinle bir kez daha
     yapılır.

  Beklenen: ikinci turda tahmin gerektiren nokta kalmaz. Bulunan eksikler bu maddenin
  altına yazılır.

> **Bilinen açıklar.** Ayrı bir "bilinen açıklar" tablosu tutulmaz; şu altı madde onun
> yerini alır ve kapanışları burada izlenir: tarayıcıdan OIDC girişi (1.1), Trino
> Route'unun `reencrypt` olması (1.2), AD grup sağlayıcısı (1.4), sertifika yenilemesinin
> kesintisizliği (1.6), Superset imajının aynadaki sabit etiketi (4.3) ve uyarı/rapor
> özelliği kararı (4.6).

---

## Kontrol listesi

- [ ] §0'daki beş madde kurulumdan **önce** kapatıldı.
- [ ] §1–§4'teki bütün kutular işaretlendi ve **ölçülen çıktı** yanlarına yazıldı.
- [ ] §5'teki üç karar müşteriyle paylaşıldı; itiraz varsa yazılı olarak kayda geçti.
- [ ] Kabul koşusu `9/9` geçti ve kanıt paketi hazırlandı.
- [ ] Belge denemesi (6.3) yapıldı ve bulunan eksikler belgeye işlendi.
- [ ] Kapatılamayan madde varsa riski yazılı olarak kayda geçti.

## Sonraki bölüm

Kabul koşusunun ayrıntısı ve madde ↔ kanıt tablosu: [kabul-testleri.md](kabul-testleri.md).
Günlük işletme: `docs/50-isletme/` altındaki belgeler, ilki
[50-isletme/degisiklik-nasil-uygulanir.md](../50-isletme/degisiklik-nasil-uygulanir.md).
