# Kurulum (F1 kapsamı: platform + Polaris)

## Ön koşullar
- Kubernetes ≥ 1.33 (OpenShift veya vanilla), `kubectl`, `helm` ≥ 3.14, internet (Maven Central, Docker Hub, quay.io, downloads.apache.org, GitHub).
- S3 uyumlu depolama + bucket (`lakehouse`). Kind/dev için `components.minio=true` küme içi MinIO kurar.
- Secret'lar (Git'e girmez; kurulumdan önce `lakehouse` ns'inde): `polaris-root` (`clientId`, `clientSecret`), `keycloak-admin` (`username`, `password`), `connect-push` (`kubernetes.io/dockerconfigjson`; Connect build çıktısını registry'ye iter — `connect.buildPushSecret`), `s3-creds` (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`) — her ortamda aynı isim: dev'de `components.minio=true` iken glue chart'ın MinIO şablonu tarafından üretilir, prod'da kurulumdan önce operatör tarafından oluşturulur.
- Kaynak DB kimlik bilgileri (F2): `<source>-db` Secret'ları (`username`, `password`).
- **Spark işleri her koşuda `spark.jars.packages`'i Maven Central'dan (`repo1.maven.org`) çözer** (Iceberg runtime + AWS bundle; driver/executor pod'ları `/tmp/.ivy2`'ye indirir, pod ömürlük). Yani `silver-merge`, `mongo-bronze` ve 3 bakım işi için **sürekli dışarı erişim** gerekir; kapalı ağda işler `UnresolvedAddressException`/`Ivy` hatasıyla FAILED olur. Kısıtlı ağ seçenekleri (F5 işi, bu sürümde uygulanmadı): iç Maven aynası (`spark.jars.ivySettings` ile) ya da `spark.jars.ivy`'yi kalıcı bir PVC'ye alıp tek seferlik ısıtma.

## Adımlar
1. `platform/values/glue.yaml` ve `polaris.yaml`'ı ortama göre düzenle (hostname, S3 endpoint, `platform`, `route`, **`connect.buildImage`** = Connect build çıktısının gerçek `registry/repo:tag`'i — zorunlu, `:latest` kullanma; gerekiyorsa `connect.buildPushSecret`; HA/depolama için `cnpg.*.instances` ve `kafka.storageClass`/`cnpg.*.storageClass`); `platform/polaris/setup.yaml`'da S3 endpoint'i.
   Boyutlandırma: varsayılan `spark.*` değerleri küçük tier içindir (driver 2g/1 core, 1 executor, `shufflePartitions: 8`); tablolar/veri büyüdüğünde `platform/values/glue.yaml`'daki yorumlu büyük tier bloğunu aç (driver 4g/2 core, 2×4g executor, `shufflePartitions: 200`).
2. `bootstrap/bootstrap.sh --env prod` (ArgoCD `v3.5.2` kurar; `quay.io/strimzi-helm` OCI Helm repository'sini ArgoCD'ye kaydeder; kök + alt Application'ları — Strimzi, CNPG, keycloak-operator, **spark-operator**, glue, Polaris — uygular). İzle: `kubectl -n argocd get applications` → hepsi `Synced/Healthy`. Connect imaj build'i ~10 dk.
3. Polaris kataloğu: `pip install 'apache-polaris==1.7.0'` → `runbooks/scripts/polaris-setup.sh` (katalog `lakehouse`, namespace'ler, roller, principal'lar; `polaris-connect/-spark/-trino/-notebooks` Secret'ları yazılır).
   - S3'te STS yoksa `setup.yaml`'da `sts_unavailable: true`; istemcilerde vending kapalı (Connect `iceberg.catalog.header.X-Iceberg-Access-Delegation=none` + `s3.*` anahtarları; Trino `iceberg.rest-catalog.vended-credentials-enabled=false`; Spark `header.X-Iceberg-Access-Delegation=none`).
4. Doğrulama: `test/e2e/polaris-smoke/job.yaml` (küme içi pyiceberg yaz/oku) — `test/e2e/run.sh` aynı adımları otomatik yapar.
5. Kaynak ekleme: `runbooks/add-source.md`

## Lokal geliştirme (kind + Podman/Docker)
`test/e2e/kind.sh && bootstrap/bootstrap.sh --env dev --mode helm` (yerel chart, ArgoCD'siz) veya `--mode argocd --revision <dal>` (ArgoCD GitHub'dan çeker → değişiklikler push'lu olmalı).
Not: `--repo/--revision` kök Application'a uygulanır ve kök, `spec.source.kustomize.patches` ile alt Application'ların (`glue`, `keycloak-operator`, `polaris`) `repoURL`/`targetRevision`'ını **her reconcile'da** aynı repo/revizyona sabitler — yani bootstrap edilen revizyon (PR'da commit SHA'sı) uçtan uca test edilir. `platform/root-app.yaml` belgelenen statik varsayılandır (dev@v2); bootstrap.sh onu değil, parametreli eşdeğerini uygular.
Podman: kind ≥ 0.33 (Podman 6 uyumu); `kind.sh` düğüm pids limitini yükseltir (Spark için).

## Sorun giderme
- Connect `Build` uzun/başarısız: `kubectl -n lakehouse logs -l strimzi.io/kind=KafkaConnect --tail=100`.
- Connector task FAILED / Bronze boş: `kubectl -n lakehouse get kafkaconnector <ad> -o jsonpath='{.status.connectorStatus.tasks[0].trace}'`; task başarısızlığından sonra consumer konumu ileri kalabilir → `spec.state: stopped` → `kafka-consumer-groups.sh --group connect-<sink> --reset-offsets --to-earliest --execute` → `running`.
- NetworkPolicy prod'da açık; pre-ship OpenShift'te doğrulanmadan önce `networkPolicy.enabled=false` ile kur, sonra aç.
- Polaris health `:8182/q/health`, API `:8181`; bootstrap Job log'u `kubectl -n lakehouse logs job/polaris-bootstrap`.
- Spark işi FAILED: `kubectl -n lakehouse get sparkapplication`; `kubectl -n lakehouse logs <ad>-driver`.
- `silver-merge` `SchemaConflict`: Silver kolon tipi güvenli genişletilemiyor → manuel `ALTER TABLE` ya da yeni kolon.
- `mongo-bronze` `KAFKA_JAAS` yok → KafkaUser spark yalnız mongodb kaynağı varken oluşur.
- nginx: `nginx.dlq` doluysa `ts` dönüşümü başarısız (Fluent Bit lua filtresi eksik).
