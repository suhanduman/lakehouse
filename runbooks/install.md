# Kurulum (F1 kapsamı: platform + Polaris)

## Ön koşullar
- Kubernetes ≥ 1.33 (OpenShift veya vanilla), `kubectl`, `helm` ≥ 3.14, internet (Maven Central, Docker Hub, quay.io, downloads.apache.org, GitHub).
- S3 uyumlu depolama + bucket (`lakehouse`). Kind/dev için `components.minio=true` küme içi MinIO kurar.
- Secret'lar (Git'e girmez; kurulumdan önce `lakehouse` ns'inde): `polaris-root` (`clientId`, `clientSecret`), `keycloak-admin` (`username`, `password`), `s3-creds` (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`) — her ortamda aynı isim: dev'de `components.minio=true` iken glue chart'ın MinIO şablonu tarafından üretilir, prod'da kurulumdan önce operatör tarafından oluşturulur.
- Kaynak DB kimlik bilgileri (F2): `<source>-db` Secret'ları (`username`, `password`).

## Adımlar
1. `platform/values/glue.yaml` ve `polaris.yaml`'ı ortama göre düzenle (hostname, S3 endpoint, `platform`, `route`); `platform/polaris/setup.yaml`'da S3 endpoint'i.
2. `bootstrap/bootstrap.sh --env prod` (ArgoCD `v3.5.2` kurar; `quay.io/strimzi-helm` OCI Helm repository'sini ArgoCD'ye kaydeder; kök + alt Application'ları uygular). İzle: `kubectl -n argocd get applications` → hepsi `Synced/Healthy`. Connect imaj build'i ~10 dk.
3. Polaris kataloğu: `pip install apache-polaris` → `runbooks/scripts/polaris-setup.sh` (katalog `lakehouse`, namespace'ler, roller, principal'lar; `polaris-connect/-spark/-trino/-notebooks` Secret'ları yazılır).
   - S3'te STS yoksa `setup.yaml`'da `sts_unavailable: true`; istemcilerde vending kapalı (Connect `iceberg.catalog.header.X-Iceberg-Access-Delegation=none` + `s3.*` anahtarları; Trino `iceberg.rest-catalog.vended-credentials-enabled=false`; Spark `header.X-Iceberg-Access-Delegation=none`).
4. Doğrulama: `test/e2e/polaris-smoke/job.yaml` (küme içi pyiceberg yaz/oku) — `test/e2e/run.sh` aynı adımları otomatik yapar.

## Lokal geliştirme (kind + Podman/Docker)
`test/e2e/kind.sh && bootstrap/bootstrap.sh --env dev --mode helm` (yerel chart, ArgoCD'siz) veya `--mode argocd --revision <dal>` (ArgoCD GitHub'dan çeker → değişiklikler push'lu olmalı).
Not: `--repo/--revision` kök Application'a uygulanır; alt Application'lar `platform/apps/*.yaml`'daki `repoURL`/`targetRevision: v2` ile kök tarafından yeniden üretilir, yani dalın **son commit'ini** izler (standart app-of-apps). PR'da farklı bir revizyonu alt uygulamalarla test etmek için `platform/apps/*.yaml`'daki `targetRevision`'ı o dalda değiştir.
Podman: kind ≥ 0.33 (Podman 6 uyumu); `kind.sh` düğüm pids limitini yükseltir (Spark için).

## Sorun giderme
- Connect `Build` uzun/başarısız: `kubectl -n lakehouse logs -l strimzi.io/kind=KafkaConnect --tail=100`.
- Connector task FAILED / Bronze boş: `kubectl -n lakehouse get kafkaconnector <ad> -o jsonpath='{.status.connectorStatus.tasks[0].trace}'`; task başarısızlığından sonra consumer konumu ileri kalabilir → `spec.state: stopped` → `kafka-consumer-groups.sh --group connect-<sink> --reset-offsets --to-earliest --execute` → `running`.
- Polaris health `:8182/q/health`, API `:8181`; bootstrap Job log'u `kubectl -n lakehouse logs job/polaris-bootstrap`.
