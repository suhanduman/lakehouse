# `custom/` — kendi Kubernetes kaynaklarınız (GitOps)

Bu klasör **size** aittir. Ürün chart'ına (`glue/`) ve `platform/` altındaki dosyalara
**dokunmadan** kendi Kubernetes kaynaklarınızı (Spark uygulaması, CronJob, ConfigMap, KafkaTopic…)
kurulumun parçası yaparsınız.

ArgoCD `custom` adlı bir Application ile bu klasörü sürekli izler (`platform/apps/15-custom.yaml`,
sync-wave 4 → ürün bileşenleri kurulduktan sonra). Git'e ne koyarsanız kümede o vardır; elle
`oc apply` gerekmez, elle yapılan değişiklik geri alınır (`selfHeal`), Git'ten sildiğiniz
kaynak kümeden de silinir (`prune`).

## Adım adım: yeni bir kaynak ekleme

1. Dosyanızı bu klasöre koyun, örn. `gunluk-rapor.yaml`.
2. `custom/kustomization.yaml` içindeki `resources:` listesine ekleyin:
   ```yaml
   resources:
   - gunluk-rapor.yaml
   ```
3. Commit + push edin (varsayılan dal: `main`).
4. ArgoCD birkaç dakika içinde uygular. İzlemek için:
   ```sh
   oc -n "$ARGOCD_NS" get application custom
   oc -n "$LAKEHOUSE_NS" get sparkapplication,scheduledsparkapplication,cronjob
   ```
   `SYNC STATUS=Synced`, `HEALTH STATUS=Healthy` beklenen sonuçtur. Takılırsa:
   `oc -n "$ARGOCD_NS" describe application custom | tail -30`
   (Değişkenler `install/lakehouse.env`'den gelir: OpenShift'te `$ARGOCD_NS` =
   `openshift-gitops`, `$LAKEHOUSE_NS` = `lakehouse`. Geliştirme (kind) kümesinde aynı
   komutlar `kubectl` ve `-n argocd` ile koşar.)

Hazır örnekleri olduğu gibi açmak isterseniz `custom/kustomization.yaml`'a `- examples` satırını
ekleyin.

## Örnekler (`custom/examples/`)

| Dosya | Ne yapar |
| --- | --- |
| `ornek_rapor.py` | Silver `shop.orders` → durum bazında sipariş sayısı → `sandbox.ornek_rapor` (yalnız veri mantığı) |
| `spark-tek-seferlik.yaml` | `SparkApplication ornek-rapor` — **tek seferlik** koşu (uygulandığı anda çalışır) |
| `spark-zamanli.yaml` | `ScheduledSparkApplication ornek-rapor-zamanli` — aynı iş, gecelik cron (örnekte `suspend: true`) |
| `dbt-cronjob.yaml` | Spark dışı örnek: `examples/dbt/cronjob.yaml`'ın kopyası (dbt → Gold) |
| `kustomization.yaml` | Yukarıdakilerden hangilerinin uygulanacağı + Python dosyasından ConfigMap üretimi |

Python kodu ayrı bir imaja gömülmez: `kustomization.yaml`'daki `configMapGenerator` `.py`
dosyasından bir ConfigMap üretir, CR onu `/opt/job` altına mount eder ve
`mainApplicationFile: local:///opt/job/ornek_rapor.py` (kendi dosyanızda dosya adını
değiştirirsiniz) onu çalıştırır. **Özel imaj gerekmez** — ürünün resmi Spark imajı kullanılır.

## Bilmeniz gereken 4 şey

1. **Tek seferlik iş GitOps'a uygun değildir.** `SparkApplication` uygulandığı anda koşar; ArgoCD
   altında `timeToLiveSeconds` dolup CR silindiğinde `selfHeal` onu yeniden yaratır → iş tekrar
   tekrar koşar. Bu yüzden `spark-tek-seferlik.yaml` örneği `kustomization.yaml`'da **yorumludur**;
   tekrar eden işler için `ScheduledSparkApplication` (bkz. `spark-zamanli.yaml`) kullanın, tek
   seferlik koşuyu elle yapın:
   ```sh
   # ÖN KOŞUL: CR, Python kodunu ConfigMap `ornek-rapor`tan mount eder — ConfigMap kümede YOKSA pod başlamaz.
   # Prod'da custom/ boştur, yani önce ConfigMap'i uygulayın (ya da tüm örnekleri):
   oc apply -k custom/examples               # ConfigMap ornek-rapor + (askıda) zamanlı örnek
   oc -n "$LAKEHOUSE_NS" delete sparkapplication ornek-rapor --ignore-not-found
   oc apply -f custom/examples/spark-tek-seferlik.yaml
   ```
2. **Başarısız kaynak Application'ı Degraded yapar.** `custom` Application'ın sağlığı içindeki
   kaynakların sağlığıdır; FAILED bir `SparkApplication` uygulamayı Degraded gösterir (kurulumun
   geri kalanını etkilemez).
3. **Sırlar Git'e girmez.** CR'larda Secret'lara yalnız **adlarıyla** referans verin
   (`secretKeyRef: {name: polaris-spark, key: credential}`). Secret'ları kurulumda siz yaratırsınız.
4. **Siteye özel değerler.** Örneklerdeki `sparkConf` satırları dev/kind değerleridir. Prod'da S3
   endpoint, region, delegation header ve katalog `uri`/`warehouse`
   **`platform/values/site/glue.yaml` ile aynı** olmalıdır; ilgili satırlar örneklerde `# SİTE` ile
   işaretlidir.

Ayrıntılı, adım adım anlatım:
[docs/50-isletme/yeni-spark-uygulamasi.md](../docs/50-isletme/yeni-spark-uygulamasi.md).
Bir tabloyu ya da kaynağı verisiyle kaldırmak:
[docs/50-isletme/kaynak-veya-tablo-silme.md](../docs/50-isletme/kaynak-veya-tablo-silme.md).
