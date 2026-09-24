# 90 — Pre-ship kontrol listesi (gerçek OpenShift kümesinde)

**Bu bölümde:** yalnız **gerçek bir OpenShift kümesinde** kanıtlanabilen maddelerin tek
listesi. Geliştirme kümesinde (kind) karşılığı olmayan ya da onun sınırlarına takılan her şey
buradadır; hepsi kutuludur ve her madde **komut + beklenen sonuç** taşır.
**Süre:** listenin tamamı 1–2 gün (bekleyen maddeler platform ve AD ekiplerine bağlıdır).
**Gereken yetki:** kümede **cluster-admin**; AD ve depolama maddelerinde ilgili ekiplerin
desteği.
**Nerede çalıştırılır:** `[bastion]` — `oc login` ile kümeye girilmiş yönetim makinesi.
Madde 1.9'un ilk komutu `[nginx ajan sunucusu]` üzerinde çalışır.

**Nasıl kullanılır**

1. Pre-ship ortamı hazır olunca kurulumu [30-kurulum](../30-kurulum.md) ile yapın (üretim
   değerleriyle: `platform/values/site/`).
2. Bu listeyi **baştan sona** koşun; her maddeyi kutusunu işaretleyerek **ve ölçülen çıktıyı
   yanına yazarak** kapatın. Kanıt çıktının kendisidir, "bakıldı" değil.
3. Kapanış kanıtı: `scripts/acceptance.sh` **9/9** + bu listede açık kutu kalmaması
   ([kabul-testleri.md](kabul-testleri.md)).

**Kapsam dışı:** geliştirme kümesinde ve CI'da zaten yeşil olan her şey. Bu liste yalnız
**eksik kalan canlı kanıtı** izler. Toplam **40 kutu** vardır.

Komutlarda `oc` ve `kubectl` birbirinin yerine kullanılabilir; OpenShift'e özgü nesnelerde
(`Route`, `DataProtectionApplication`) `oc` yazılmıştır. Bütün komutlar önce
`install/lakehouse.env` yüklenmiş bir kabukta koşturulur ([30-kurulum](../30-kurulum.md) §1).

---

## 0. Küme ve araçlar (ön koşulların canlı teyidi)

Bu beş madde [20-on-kosullar](../20-on-kosullar.md) bölümündeki doğrulamaların **gerçek
kümede** tekrarıdır; geliştirme kümesinde karşılıkları yoktur ya da farklıdır.

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

  Beklenen: GitOps operatörünün satırında `PHASE` **`Succeeded`**. Sürüm numarası kayda
  geçirilir.
  *Kaynak: [20-on-kosullar](../20-on-kosullar.md) madde 2.1.*

- [ ] **0.3 GitOps denetleyicisinin küme geneli yetkisi**

  `[bastion]`

  ```bash
  oc auth can-i create customresourcedefinitions \
    --as=system:serviceaccount:"$ARGOCD_NS":openshift-gitops-argocd-application-controller
  ```

  Beklenen: `yes`. Servis hesabının adı kurulu GitOps sürümüne göre değişebilir; gerçek adı
  buraya yazın.
  *Kaynak: [20-on-kosullar](../20-on-kosullar.md) madde 2.2.*

- [ ] **0.4 DNS ve joker sertifika**

  `[bastion]`

  ```bash
  getent hosts "trino-$LAKEHOUSE_NS.$APPS_DOMAIN"
  echo | openssl s_client -connect "trino-$LAKEHOUSE_NS.$APPS_DOMAIN:443" \
    -servername "trino-$LAKEHOUSE_NS.$APPS_DOMAIN" 2>/dev/null \
    | openssl x509 -noout -issuer -dates
  ```

  Beklenen: ad çözülüyor ve router'ın sunduğu sertifika kurumun güvendiği bir kökten geliyor,
  süresi geçerli. Bu yalnız gerçek ortamda kanıtlanır.
  *Kaynak: [20-on-kosullar](../20-on-kosullar.md) madde 7.*

- [ ] **0.5 Yönetim makinesinde araçlar tam**

  `[bastion]`

  ```bash
  for t in oc kubectl aws ldapsearch getent curl openssl python3 git htpasswd jq; do
    command -v "$t" >/dev/null && echo "OK    $t" || echo "EKSIK $t"
  done
  ```

  Beklenen: on bir satırın hepsi `OK`. Eksik olan varsa hangi bölümde gerektiği
  [20-on-kosullar](../20-on-kosullar.md) girişindeki araç tablosundadır.
  *Kaynak: [20-on-kosullar](../20-on-kosullar.md) araç tablosu.*

---

## 1. Kimlik ve ağ

- [ ] **1.1 Tarayıcıdan OIDC girişi (Superset, Trino web arayüzü, JupyterHub)**

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" get route superset trino jupyterhub zeppelin keycloak \
    -o custom-columns='AD:.metadata.name,HOST:.spec.host,TLS:.spec.tls.termination'
  ```

  Beklenen: beş Route da müşteri alan adıyla listeleniyor; tarayıcıdan her birine girişte
  Keycloak'a yönlenme, geri dönüşte oturum açılması ve grup → rol eşlemesinin tutması
  (`lakehouse-analysts` → Superset `Alpha`, JupyterHub izinli gruplar, Zeppelin `analyst`).
  Superset'te SQL Lab'da `select 1` sorgusu sonuç döndürüyor.
  *Neden yalnız burada: geliştirme kümesinde Keycloak adresi küme içi bir adrestir, tarayıcı
  akışı kanıtlanamaz — [40-kurulum-sonrasi](../40-kurulum-sonrasi.md) §5.2 ve §5.3.*

- [ ] **1.2 Trino Route'u `reencrypt`**

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" get route trino -o jsonpath='{.spec.tls.termination}{"\n"}'
  oc -n "$LAKEHOUSE_NS" get route trino \
    -o jsonpath='{.spec.tls.destinationCACertificate}' | head -1
  curl -sSI "https://$(oc -n "$LAKEHOUSE_NS" get route trino -o jsonpath='{.spec.host}')/v1/info"
  ```

  Beklenen: `reencrypt`; ikinci komut `-----BEGIN CERTIFICATE-----` ile başlıyor; üçüncü komut
  gerçek bir HTTP yanıtı veriyor (TLS hatası değil). `tls.caBundle` boş bırakılırsa Route
  `passthrough` olur ve **tarayıcının** iç kök CA'ya güvenmesi gerekir.
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

  Beklenen: ilk komut **boş** (bu değişken hub pod'unda **bilerek yoktur**; Keycloak
  Route'unun sertifikası kurumsal ya da genel bir CA ile doğrulanır, iç CA'ya daraltmak akışı
  kırar) ve ikinci komut **200**.
  *Kaynak: [40-kurulum-sonrasi](../40-kurulum-sonrasi.md) §5.6, ikinci madde.*

- [ ] **1.4 Trino grup sağlayıcısı AD'ye karşı canlı**

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" exec deploy/trino-coordinator -- \
    grep -c group-provider.name=ldap /etc/trino/group-provider.properties
  oc -n "$LAKEHOUSE_NS" logs deploy/trino-coordinator | grep -i "ldap\|group" | tail -20
  ```

  Beklenen: `1`; ardından bir AD kullanıcısının kendi kimliğiyle `SHOW SCHEMAS FROM lakehouse`
  çalıştırması ve grup kurallarının uygulanması. Trino grubu **CN** olarak alır → AD grubunun
  CN değeri `lakehouse-analysts` gibi olmalıdır.
  *Kaynak: [50-isletme/kullanici-ve-yetki.md](../50-isletme/kullanici-ve-yetki.md) §2 ve §4.1;
  geliştirme kümesinde dosya tabanlı sağlayıcı koşar.*

- [ ] **1.5 Keycloak AD federasyonu ve bağlanma parolası**

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" get keycloakrealmimport lakehouse \
    -o jsonpath='{.status.conditions[?(@.type=="Done")].status}{"\n"}'
  oc -n "$LAKEHOUSE_NS" get secret keycloak-clients -o jsonpath='{.data.ldap-bind}' | wc -c
  ```

  Beklenen: `True`; Secret anahtarı dolu ve realm tanımında düz parola **yok**. Keycloak
  yönetim arayüzünde **User Federation** bağlantısı çalışıyor ve kullanıcılar akıyor.
  *Kaynak: [30-kurulum](../30-kurulum.md) §5.6.*

- [ ] **1.6 Sertifika yenilemesi kesintisiz**

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" get certificate trino-tls \
    -o custom-columns='HAZIR:.status.conditions[0].status,BITIS:.status.notAfter,YENILEME:.status.renewalTime'
  oc -n "$LAKEHOUSE_NS" get pod -l app.kubernetes.io/name=trino \
    -o custom-columns='POD:.metadata.name,YENIDEN:.status.containerStatuses[0].restartCount'
  ```

  Beklenen: sertifika kendiliğinden yenilenir; yenileme anında koordinatör ve çalışanların
  **yeniden başlatma sayısı artmaz** ve açık oturumlar düşmez. Prova için sertifikayı elle
  yeniletip sayacı yeniden okuyun.
  *Kaynak:
  [50-isletme/gunluk-haftalik-kontroller.md](../50-isletme/gunluk-haftalik-kontroller.md) §4.3.*

- [ ] **1.7 Ağ politikaları OVN altında: apiserver → webhook yolu**

  `[bastion]`

  ```bash
  oc get validatingwebhookconfiguration,mutatingwebhookconfiguration \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{range .webhooks[*]}{.clientConfig.service.namespace}/{.clientConfig.service.name}{" "}{end}{"\n"}{end}'
  oc -n "$LAKEHOUSE_NS" get networkpolicy
  oc -n "$LAKEHOUSE_NS" apply --dry-run=server -f test/e2e/cnpg-restore.yaml
  oc -n "$LAKEHOUSE_NS" get sparkapplication
  ```

  Beklenen: `$LAKEHOUSE_NS` ad alanına bakan bir webhook servisi **yok** (webhook'lar
  operatörlerin kendi ad alanlarındadır) → yalnız giriş yönünü kısıtlayan politikalar bu yolu
  etkilemez; sunucu tarafı deneme uygulaması **kabul edilir** ve Spark işleri normal koşar.
  Reddedilirse `glue/templates/networkpolicy.yaml` dosyasına apiserver'ı geçiren bir kural
  eklenir.
  *Kaynak: [port-ve-servisler.md](port-ve-servisler.md) §5.*

- [ ] **1.8 Kafka dış dinleyicisi (nginx akışı kullanılacaksa)**

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" get kafka lakehouse \
    -o jsonpath='{range .status.listeners[*]}{.name}{"\t"}{.bootstrapServers}{"\n"}{end}'
  oc -n "$LAKEHOUSE_NS" get route -l strimzi.io/cluster=lakehouse
  ```

  Beklenen: dış dinleyici için bir bootstrap Route'u (TLS geçişli) ve broker başına birer
  Route; dışarıdan `SCRAM-SHA-512` ile bağlanılabiliyor. Geliştirme kümesinde bu yol düğüm
  portudur ve yalnız varlığı doğrulanır.
  *Kaynak: [20-on-kosullar](../20-on-kosullar.md) madde 8;
  [50-isletme/yeni-kaynak-ve-pipeline.md](../50-isletme/yeni-kaynak-ve-pipeline.md) §10.*

- [ ] **1.9 Günlük ajanı gerçek nginx sunucusunda**

  `[nginx ajan sunucusu]`

  ```bash
  systemctl status fluent-bit
  ```

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" exec lakehouse-dual-role-0 -- bin/kafka-topics.sh \
    --bootstrap-server localhost:9093 --command-config /tmp/client.properties \
    --describe --topic nginx.access
  ```

  Beklenen: ajan dış Route'a bağlanır, `nginx.access` konusuna kayıt düşer, ham tabloda satır
  sayısı artar ve `nginx.dlq` boş kalır.
  *Kaynak: [50-isletme/yeni-kaynak-ve-pipeline.md](../50-isletme/yeni-kaynak-ve-pipeline.md)
  §10.*

- [ ] **1.10 AD kök CA'sı ile gerçek LDAPS el sıkışması**

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" exec keycloak-0 -- ls /opt/keycloak/conf/truststores
  oc -n "$LAKEHOUSE_NS" logs keycloak-0 | grep TruststoreBuilder | tail -1
  oc -n "$LAKEHOUSE_NS" exec deploy/trino-coordinator -- \
    grep ldap.ssl.truststore.path /etc/trino/group-provider.properties
  oc -n "$LAKEHOUSE_NS" exec deploy/zeppelin -c zeppelin -- \
    keytool -list -keystore /truststore/ad-truststore.p12 -storetype PKCS12 \
    -storepass changeit | grep ad-ca
  ```

  Beklenen: dört komut da dolu yanıt verir; **ardından** gerçek bir AD hesabıyla Keycloak
  federasyon eşitlemesi, Trino'da grup çözümü ve Zeppelin girişi **sertifika zinciri hatası
  olmadan** tamamlanır.
  *Yalnız burada kanıtlanabilir: geliştirme kümesinde AD yoktur — orada yalnız bağlama ve
  truststore içeriği doğrulandı, el sıkışması doğrulanmadı. Şüphede kalırsanız önce
  `openssl s_client -connect` ile sunucu zincirinin bu kökle bittiğini görün
  ([20-on-kosullar](../20-on-kosullar.md) madde 6). Tüketici tablosu:
  [30-kurulum](../30-kurulum.md) §5.13; Secret satırı:
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
  not defteri açabilir. **Yalnız eşitleme yetmez**: sert yenileme açıklaması olmadan Secret
  yeniden üretilmez. Prova canlı ortamda bir kez yapılır ve sonucu buraya yazılır.
  *Kaynak: [50-isletme/kullanici-ve-yetki.md](../50-isletme/kullanici-ve-yetki.md) §9.*

- [ ] **1.12 `allow-platform-namespaces` içindeki ArgoCD ad alanı seçicisi**

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" get networkpolicy allow-platform-namespaces \
    -o jsonpath='{range .spec.ingress[0].from[*]}{.namespaceSelector.matchLabels}{"\n"}{end}'
  echo "$ARGOCD_NS"
  ```

  Beklenen: listede `kubernetes.io/metadata.name: argocd` görünür, `$ARGOCD_NS` ise
  `openshift-gitops`'tur — yani seçici **hiçbir ad alanıyla eşleşmez**. Bu bir kod kusurudur:
  `glue/templates/networkpolicy.yaml` ArgoCD ad alanını **sabit `argocd`** yazar, `$ARGOCD_NS`
  değerinden türetmez. Kurulumda sorun çıkarmaz (ArgoCD kube-apiserver ile konuşur,
  `$LAKEHOUSE_NS` pod'larına doğrudan bağlanmaz); ArgoCD'den pod'a doğrudan erişim gerekiyorsa
  ya da politika sıkılaştırılacaksa şablon `$ARGOCD_NS` ile parametreleştirilir (birim testi +
  e2e ister, bu yüzden teslim öncesi **karar kutusu**dur: düzeltilecek mi, kabul mü edilecek).
  *Kaynak: [port-ve-servisler.md](port-ve-servisler.md) §5.*

---

## 2. İzleme ve günlükler

- [ ] **2.1 Kullanıcı iş yükü izlemesi açık**

  `[bastion]`

  ```bash
  oc -n openshift-monitoring get cm cluster-monitoring-config -o yaml
  oc -n openshift-user-workload-monitoring get pods
  ```

  Beklenen: ConfigMap'in gövdesinde `enableUserWorkload: true`; izleme pod'ları `Running`.
  **ConfigMap zaten varsa üzerine yazılmaz, düzenlenir** — düz bir uygulama kümenin diğer
  izleme ayarlarını siler.
  *Kaynak: [20-on-kosullar](../20-on-kosullar.md) madde 9.1.*

- [ ] **2.2 İzleme nesneleri toplanıyor**

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" get podmonitor,servicemonitor,prometheusrule
  TOKEN=$(oc whoami -t)
  HOST=$(oc -n openshift-monitoring get route thanos-querier -o jsonpath='{.spec.host}')
  curl -sSk -H "Authorization: Bearer $TOKEN" \
    --data-urlencode 'query=up{namespace="lakehouse"} == 1' "https://$HOST/api/v1/query" \
    | jq -r '.data.result[].metric.job' | sort | uniq -c
  ```

  Beklenen: iki PodMonitor (`kafka-resources-metrics`, `spark-operator-podmonitor`),
  Polaris'in ServiceMonitor'ü ve **tek** `PrometheusRule/lakehouse` listeleniyor; sorgu **en
  az** şu işleri döndürüyor: `kafka-resources-metrics` (3 hedef),
  `spark-operator-podmonitor`, `polaris-mgmt`. Trino, Superset, JupyterHub ve Zeppelin
  hedefleri **bilerek yoktur**; `kube-state-metrics` de yalnız geliştirme yığınında bulunur,
  burada görünmemesi normaldir.
  *Kaynak: [50-isletme/izleme-ve-alarmlar.md](../50-isletme/izleme-ve-alarmlar.md) §2;
  [40-kurulum-sonrasi](../40-kurulum-sonrasi.md) §6.2.*

- [ ] **2.3 Beş alarm kuralı sağlıklı**

  `[bastion]`

  ```bash
  TOKEN=$(oc whoami -t)
  HOST=$(oc -n openshift-monitoring get route thanos-querier -o jsonpath='{.spec.host}')
  curl -sSk -H "Authorization: Bearer $TOKEN" "https://$HOST/api/v1/rules" \
    | jq -r '.data.groups[] | select(.name=="lakehouse") | .rules[] | "\(.name)\t\(.health)"'
  ```

  Beklenen: beş kural da `ok` ve hiçbiri boş yere `firing` değil.
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
  mesajlaşma, çağrı sistemi) tanımlı ve **test bildirimi ulaşıyor**. Geliştirme kümesinde
  bildirim hiçbir yere gitmez.
  *Kaynak: [50-isletme/izleme-ve-alarmlar.md](../50-isletme/izleme-ve-alarmlar.md) §5.*

- [ ] **2.5 Günlük toplama `$LAKEHOUSE_NS` ad alanını kapsıyor**

  `[bastion]`

  ```bash
  oc -n openshift-logging get lokistack,clusterlogforwarder
  ```

  Beklenen: `LokiStack` `Ready`; yönlendirici girdisi `lakehouse` ad alanını kapsıyor;
  konsolun **Observe → Logs** sekmesinde örnek sorgular sonuç veriyor. **Bu üründe Loki
  dağıtımı yoktur** — kurulum platformundur ve zorunlu değildir.
  *Kaynak: [50-isletme/sorun-giderme.md](../50-isletme/sorun-giderme.md) §9;
  [20-on-kosullar](../20-on-kosullar.md) madde 9.3.*

- [ ] **2.6 Kabul koşusunun izleme yolu OpenShift adlarıyla**

  `[bastion]`

  ```bash
  PATH="$PWD/.venv/bin:$PATH" PROM_STS=prometheus-user-workload \
    PROM_SVC=prometheus-user-workload GRAFANA_SKIP=1 \
    scripts/acceptance.sh --ns "$LAKEHOUSE_NS" \
    --mon-ns openshift-user-workload-monitoring --velero-ns openshift-adp
  ```

  Beklenen: izleme yolu geçiyor. Kullanıcı iş yükü Prometheus'u yetkilendirme istediği için bu
  yol düşebilir; o hâlde izleme kanıtı 2.2 ve 2.3 maddeleriyle elle alınır — kural ve hedef
  adları aynıdır.
  *Kaynak: [kabul-testleri.md](kabul-testleri.md) §6.*

---

## 3. Yedekleme (OADP)

- [ ] **3.1 `DataProtectionApplication` alan adları kurulu sürümle doğrulandı**

  `[bastion]`

  ```bash
  oc explain dataprotectionapplication.spec --recursive | head -60
  oc -n openshift-adp get dpa \
    -o custom-columns='AD:.metadata.name,UYGULANDI:.status.conditions[?(@.type=="Reconciled")].status'
  ```

  Beklenen: örnek tanımdaki alanlar (`backupLocations[].velero.objectStorage`,
  `configuration.velero.defaultPlugins`, `configuration.nodeAgent`) kurulu OADP sürümünde
  birebir var; nesne `True`.
  *Kaynak: [50-isletme/yedek-ve-geri-donus.md](../50-isletme/yedek-ve-geri-donus.md) §7.1.*

- [ ] **3.2 Depolama konumu `Available`, sonra site değerinde açma**

  `[bastion]`

  ```bash
  oc -n openshift-adp get backupstoragelocation
  oc -n openshift-adp get schedules.velero.io lakehouse-daily
  ```

  Beklenen: depolama konumu `Available`; site değeri açıldıktan sonra zamanlı yedek nesnesi
  **`openshift-adp`** ad alanında doğuyor. Sıra ters olursa glue `Degraded` olur.
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
  yedeği ya da veri yükleme nesnesi üretiliyor (geliştirme kümesinde **hiç üretilmiyordu**).
  Tercih edilen yol CSI anlık görüntüsü + veri taşıyıcıdır.
  *Kaynak: [50-isletme/yedek-ve-geri-donus.md](../50-isletme/yedek-ve-geri-donus.md) §6.3 ve
  §6.5.*

- [ ] **3.4 Dışlama açıklamaları canlıda**

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" get pod lakehouse-dual-role-0 \
    -o jsonpath='{.metadata.annotations.backup\.velero\.io/backup-volumes-excludes}{"\n"}'
  oc -n "$LAKEHOUSE_NS" get pod -l cnpg.io/cluster=polaris-db \
    -o jsonpath='{.items[0].metadata.annotations.backup\.velero\.io/backup-volumes-excludes}{"\n"}'
  ```

  Beklenen: sırasıyla `data-0` ve `pgdata` (açıklama **pod hacim adını** alır, disk adını
  değil).
  *Kaynak: [50-isletme/yedek-ve-geri-donus.md](../50-isletme/yedek-ve-geri-donus.md) §2.*

- [ ] **3.5 Veritabanı geri dönüş provası üretim kovasında**

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" get cluster \
    -o custom-columns='AD:.metadata.name,KOPYA:.spec.instances,ARSIV:.status.conditions[?(@.type=="ContinuousArchiving")].status'
  oc -n "$LAKEHOUSE_NS" get backups.postgresql.cnpg.io \
    -o custom-columns='AD:.metadata.name,FAZ:.status.phase,YONTEM:.status.method'
  ```

  Beklenen: üç kümede de arşivleme `True`, günlük yedekler `completed`; ardından
  `test/e2e/cnpg-restore.yaml` deseni üretim yedeğine uyarlanıp **ayrı** bir kümeye geri
  yükleniyor ve tablo sayımı tutuyor. Geri yükleme kümesi prova sonunda **silinir**.
  *Kaynak: [50-isletme/yedek-ve-geri-donus.md](../50-isletme/yedek-ve-geri-donus.md) §5.*

- [ ] **3.6 Uçtan uca kabul koşusu OpenShift ad alanı adlarıyla**

  `[bastion]`

  ```bash
  PATH="$PWD/.venv/bin:$PATH" scripts/acceptance.sh --ns "$LAKEHOUSE_NS" \
    --mon-ns openshift-user-workload-monitoring --velero-ns openshift-adp
  ```

  Beklenen: `KABUL: 9/9 yol geçti`. **Uyarı:** izleme yolu için 2.6 maddesi geçerlidir; yedek
  yolu ad alanını doğru alır ve Velero nesnelerini **tam adla** sorgular.
  *Kaynak: [kabul-testleri.md](kabul-testleri.md) §3.*

---

## 4. Kaynaklar, imajlar, dış bağımlılıklar

- [ ] **4.1 SQL Server kaynağı canlı**

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" get kafkaconnector \
    -o custom-columns='AD:.metadata.name,HAZIR:.status.conditions[?(@.type=="Ready")].status'
  oc -n "$LAKEHOUSE_NS" get kafkaconnector dbz-erp \
    -o jsonpath='{.spec.config.database\.encrypt}{"\n"}'
  oc -n "$LAKEHOUSE_NS" logs deploy/connect-connect \
    | grep -i "schema.history\|truststore\|encrypt" | tail
  ```

  Beklenen: bağlayıcı `True`; `database.encrypt=true` (varsayılan — sertifika doğrulamasını
  atlamak **yalnız** özel CA'lı laboratuvarda kabul edilir, üretimde truststore verilir); şema
  geçmişi istemcisi TLS ve PEM truststore yoluyla çalışıyor. Bağlayıcı adı kendi kaynağınızın
  adıyla değişir.
  *Kaynak: [50-isletme/yeni-kaynak-ve-pipeline.md](../50-isletme/yeni-kaynak-ve-pipeline.md).*

- [ ] **4.2 Connect imajı iç kayıt defterine itiliyor**

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" get kafkaconnect connect -o jsonpath='{.spec.build.output}{"\n"}'
  oc -n "$LAKEHOUSE_NS" get secret connect-push -o jsonpath='{.type}{"\n"}'
  oc -n "$LAKEHOUSE_NS" get kafkaconnect connect \
    -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}{"\n"}'
  ```

  Beklenen: adres `$INTERNAL_REGISTRY/$LAKEHOUSE_NS/connect:<sürüm>` biçiminde (`:latest`
  **yasak**), Secret türü `kubernetes.io/dockerconfigjson`, imaj yapımı (~10 dakika) sonunda
  `True`.
  *Kaynak: [20-on-kosullar](../20-on-kosullar.md) madde 5;
  [50-isletme/yukseltme.md](../50-isletme/yukseltme.md) §4.3.*

- [ ] **4.3 Superset imajı müşteri aynasında ve etiketi sabit**

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" get superset superset -o jsonpath='{.spec.image}{"\n"}'
  oc -n "$LAKEHOUSE_NS" get pod -l instance=superset \
    -o jsonpath='{.items[0].status.containerStatuses[0].imageID}{"\n"}'
  ```

  Beklenen: imaj müşteri aynasından çekiliyor (genel yönlendirici adresinden değil) ve etiket
  aynada **taşınamaz** biçimde kilitli. Operatör sindirim (digest) değeri kabul etmez, yalnız
  `depo:etiket` alır; dolayısıyla etiketin sabit kalması aynanın sorumluluğudur. Ölçülen
  sindirim değeri kayda geçirilir.
  *Kaynak: [50-isletme/yukseltme.md](../50-isletme/yukseltme.md) §4.5;
  [surumler-ve-lisanslar.md](surumler-ve-lisanslar.md).*

- [ ] **4.4 Maven ve PyPI erişimi ya da iç ayna**

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" get configmap spark-ivysettings \
    -o jsonpath='{.data.ivysettings\.xml}' | head
  oc -n "$LAKEHOUSE_NS" get scheduledsparkapplication silver-merge \
    -o jsonpath='{.spec.template.sparkConf.spark\.jars\.ivySettings}{"\n"}'
  ```

  Beklenen: ya Maven Central'a sürekli erişim var (ConfigMap yok), ya da ayna tanımı dolu ve
  bütün Spark işleri iç aynayı gösteriyor. Aynı sınıftan diğer ihtiyaçlar: Zeppelin'in JDBC
  indirmesi, JupyterHub ve dbt örneğinin paket kurulumları → kapalı ağda iç PyPI aynası da
  şarttır.
  *Kaynak: [50-isletme/sorun-giderme.md](../50-isletme/sorun-giderme.md) §6.2;
  [20-on-kosullar](../20-on-kosullar.md) madde 10.*

- [ ] **4.5 Depolama sınıfları ve boyutlandırma**

  `[bastion]`

  ```bash
  oc get storageclass
  oc -n "$LAKEHOUSE_NS" get pvc \
    -o custom-columns='AD:.metadata.name,SINIF:.spec.storageClassName,BOYUT:.spec.resources.requests.storage'
  ```

  Beklenen: Kafka ve veritabanı disk sınıfları ortamın **CSI** sınıfıyla doldurulmuş (madde
  3.3 buna bağlıdır), disk boyutları veri hacmine uygun. Hacim küçük kademeyi aşıyorsa büyük
  kademe değerleri açılır.
  *Kaynak: [10-planlama](../10-planlama.md) §2;
  [values-anahtarlari.md](values-anahtarlari.md) §1.*

- [ ] **4.6 Superset uyarı ve rapor özelliği kararı**

  Şu an **kapalı**. Açılacaksa bir kuyruk, ayrı bir işçi ve başsız tarayıcı gerekir; karar
  pre-ship'te müşteriyle verilir. Açılırsa canlı kanıt bu maddenin altına yazılır.
  *Kaynak: [40-kurulum-sonrasi](../40-kurulum-sonrasi.md) §5.6, dördüncü madde.*

- [ ] **4.7 Veri metrikleri panosu: SQL uyarlandı, içe aktarıldı, açılıyor**

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

  Beklenen: listede `('Iceberg metadata', 'iceberg-metadata')` var ve pano açıldığında hücreler
  **müşterinin gerçek tablolarını** gösteriyor (demo tabloları kalmamış).
  *Kaynak: [40-kurulum-sonrasi](../40-kurulum-sonrasi.md) §4;
  [50-isletme/veri-metrikleri.md](../50-isletme/veri-metrikleri.md) §4.*

- [ ] **4.8 İç kayıt defteri adresi ve ImageStream**

  `[bastion]`

  ```bash
  oc registry info --internal
  oc -n "$LAKEHOUSE_NS" get imagestream connect
  ```

  Beklenen: ilk komutun çıktısı `$INTERNAL_REGISTRY` değeriyle **birebir aynı**. `--internal`
  bayrağı şarttır: bayraksız komut, kayıt defteri dışarı açılmışsa genel adresi basar.
  `ImageStream` nesnesinin ilk itişte kendiliğinden yaratılıp yaratılmadığı küme ayarına
  bağlıdır; yoksa elle yaratılır (`oc -n "$LAKEHOUSE_NS" create imagestream connect`).
  *Kaynak: [20-on-kosullar](../20-on-kosullar.md) madde 5.*

- [ ] **4.9 İtme jetonunun gerçek süresi**

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

  Beklenen: bir yıl istendiği hâlde küme daha kısa bir üst sınır uygulayabilir; **gerçek** süre
  bu çıktıdan okunur ve kurumun takvimine yazılır.
  *Kaynak: [20-on-kosullar](../20-on-kosullar.md) madde 5;
  [50-isletme/yukseltme.md](../50-isletme/yukseltme.md) §6.*

---

## 5. Kararlar (kanıt değil, gerekçe)

Bu üç başlık kutulu değildir; tasarım kararlarının gerekçesini ve yeniden ele alınma koşulunu
kaydeder.

### 5.1 Kesinti bütçesi nesnesi (PodDisruptionBudget) yaratılmaz

**Karar:** ürün hiçbir bileşen için `PodDisruptionBudget` üretmez.

**Gerekçe.** (1) Gerçekten gerekli olanlar zaten operatörlerinden gelir: PostgreSQL kümeleri
üretimde iki kopyayla koşar ve kendi bütçelerini yönetir; Kafka üç kopyayla koşar ve bütçesini
Strimzi yönetir. Elle eklenen bir nesne bunlarla çakışır. (2) Geri kalan her şey **tek
kopyalıdır** (Kafka Connect, Trino koordinatörü, Polaris, Superset, JupyterHub hub, Zeppelin).
Tek kopyalı bir iş yüküne "en az bir tane ayakta kalsın" demek **düğüm boşaltmayı kilitler**:
yama sırasında boşaltma hiç ilerlemez. "En fazla bir tanesi düşebilir" ise zaten mevcut
davranıştır. (3) Trino çalışanları durumsuzdur; düşen sorgu yeniden koşturulur, veri kaybı
olmaz.

**Ne zaman yeniden ele alınır:** bir bileşende yüksek erişilebilirlik isteniyorsa önce **kopya
sayısı** artırılır ve bütçe kararı o değişiklikle **birlikte** verilir. Özet tablo:
[surumler-ve-lisanslar.md](surumler-ve-lisanslar.md) §6.

### 5.2 Kafka verisi yedek kapsamı dışıdır, ikinci site kurulmaz

**Karar:** Kafka konu verisi yedeklenmez ve çoklu-site çoğaltma kurulmaz.

**Gerekçe.** Kafka bu mimaride **taşıyıcıdır**, kayıt sistemi değil: kalıcı gerçek kaynak
veritabanlarında ve Iceberg/S3'tedir. Broker verisi kaybolursa doğru dönüş yolu kaynaktan
yeniden anlık görüntü almaktır; bu, yedekten dönen bayat konum ve şema geçmişiyle çalışmaktan
daha tutarlıdır. Çoklu-site çoğaltma bir yedekleme aracı **değildir**: ikinci bir aktif küme
gereksinimi doğmadan kurulması ek broker, ayrı yetki ve konum çevirisi ve sürekli çift trafik
demektir — tek siteli bir kurulumda maliyeti faydasından büyüktür.

**Ne zaman yeniden ele alınır:** müşteri ikinci bir site (aktif-aktif ya da sıcak yedek)
isterse. Kapsam tablosu:
[50-isletme/yedek-ve-geri-donus.md](../50-isletme/yedek-ve-geri-donus.md) §2.

### 5.3 Superset şema geçişinin deneme sayısı yüksek tutulur

**Karar:** Superset'in şema geçiş görevi için yüksek deneme sayısı korunur; ek bir bekleme
mekanizması yazılmaz.

**Gerekçe.** Operatörün geçiş görevi veritabanı hazır olmadan başlar ve varsayılan üç denemeyi
saniyeler içinde tüketip kalıcı hata durumunda kalır. Operatör API'si görevi veritabanına
bağlayacak bir bağımlılık alanı sunmaz; elimizdeki tek bildirimsel kaldıraç deneme sayısıdır.
Yüksek deneme sayısı taze kümede veritabanının hazır olma süresini (~4 dakika) rahatça kapsar
ve canlı kanıtlanmıştır. Pre-ship teyidi:
`oc -n "$LAKEHOUSE_NS" get jobs -l instance=superset` → şema geçiş işi **Complete**.

---

## 6. Kabul ve teslim

- [ ] **6.1 Kabul koşusu ve kanıt paketi**

  [kabul-testleri.md](kabul-testleri.md) (madde ↔ kanıt tablosu) + `scripts/acceptance.sh`
  çıktısı; 3.6 maddesindeki bayraklarla koşulur ve çıktı müşteriye teslim edilen kanıt
  paketine konur.

- [ ] **6.2 Bu listede açık kutu kalmadı**

  Kapatılamayan madde varsa kabul öncesinde **yazılı** olarak, riskiyle birlikte kayda
  geçirilir.

- [ ] **6.3 Doküman provası — kitap gerçekten izlenebiliyor mu**

  Kurulumu **hiç yapmamış** bir kişi, yalnız `docs/` altındaki kitabı okuyarak pre-ship
  ortamında kurulumu baştan sona tamamlayabilmelidir. Prova şöyle yapılır:

  1. Okuyucu 00 → 10 → 20 → 30 → 40 sırasını izler; her adımda **yalnız** yazılanı yapar.
  2. Takıldığı, tahmin etmek zorunda kaldığı ya da komutun çıktısının belgedekiyle uyuşmadığı
     her nokta not edilir.
  3. Notlar belgeye düzeltme olarak işlenir; prova bir kez daha, düzeltilmiş metinle
     tekrarlanır.

  Beklenen: ikinci turda tahmin gerektiren nokta kalmaz. Prova sırasında bulunan eksikler bu
  maddenin altına yazılır.

> **Bilinen açıkların izi.** Belgelerde ayrıca bir "bilinen açıklar" tablosu tutulmaz; bu
> listedeki altı madde o tablonun yerini alır ve hepsinin kapanışı burada izlenir: tarayıcı
> OIDC akışı (1.1), Trino Route'unun `reencrypt` olması (1.2), AD grup sağlayıcısı (1.4),
> sertifika yenilemesinin kesintisizliği (1.6), Superset imajının aynadaki sabit etiketi (4.3)
> ve uyarı/rapor özelliği kararı (4.6).

---

## Kontrol listesi

- [ ] §0'daki beş ön koşul maddesi kurulum **öncesi** kapatıldı.
- [ ] §1–§4'teki bütün kutular işaretlendi ve **ölçülen çıktı** yanlarına yazıldı.
- [ ] §5'teki üç karar müşteriyle paylaşıldı; itiraz varsa yazılı olarak kayda geçti.
- [ ] Kabul koşusu `9/9` geçti ve kanıt paketi hazırlandı.
- [ ] Doküman provası (6.3) yapıldı ve bulunan eksikler belgeye işlendi.
- [ ] Kapatılamayan madde varsa riski yazılı olarak kayıt altına alındı.

## Sonraki bölüm

Kabul koşusunun ayrıntısı ve madde ↔ kanıt tablosu: [kabul-testleri.md](kabul-testleri.md).
Gün-2 işleri: `docs/50-isletme/` altındaki kılavuzlar, ilki
[50-isletme/degisiklik-nasil-uygulanir.md](../50-isletme/degisiklik-nasil-uygulanir.md).
