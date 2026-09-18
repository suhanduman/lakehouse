# Grafana dashboard'ları (upstream, değiştirilmemiş)

Bu dizindeki JSON dosyaları upstream projelerden **birebir** indirilmiştir (özel imaj/hack yasağı ile aynı ilke:
Grafana içerikleri de fork'lanmaz). `glue/templates/monitoring.yaml` bunları `grafana_dashboard: "1"` etiketli
ConfigMap'lere gömer; kube-prometheus-stack Grafana sidecar'ı (`searchNamespace: ALL`,
`platform/values/monitoring-dev.yaml`) bu ns'i (lakehouse) tarar ve dashboard'ları otomatik yükler.

| Dosya | Kaynak | Etiket/revizyon | Lisans | Dashboard başlığı |
|---|---|---|---|---|
| `strimzi-kafka.json` | github.com/strimzi/strimzi-kafka-operator, `examples/metrics/grafana-dashboards/strimzi-kafka.json` | tag `1.2.0` | Apache-2.0 | "Strimzi Kafka" |
| `strimzi-kafka-connect.json` | github.com/strimzi/strimzi-kafka-operator, `examples/metrics/grafana-dashboards/strimzi-kafka-connect.json` | tag `1.2.0` | Apache-2.0 | "Strimzi Kafka Connect" |
| `trino-20208.json` | grafana.com/grafana/dashboards/20208 (`/api/dashboards/20208/revisions/latest/download`) | latest revizyon (2026-09-18 itibarıyla) | dashboard sayfasında lisans belirtilmemiş — grafana.com genel kullanım şartları | "Trino Cluster JMX" |

Superset için dashboard YOK: Superset 6.1.0 imajında `/metrics` uç noktası yok (Task 1 sapma 2,
`glue/templates/superset.yaml` notu) — üretilecek bir metrik serisi olmadığından dashboard da anlamsız olurdu.

## Datasource notu (`${DS_...}` girdileri)

- `strimzi-kafka.json` / `strimzi-kafka-connect.json`: paneller `${DS_PROMETHEUS}` değişkenine referans verir
  (`__inputs[0].name == "DS_PROMETHEUS"`, `type: datasource`, `pluginId: prometheus`). kube-prometheus-stack
  Grafana sidecar'ı (`sidecar.dashboards`) bu tür `__inputs` içeren dashboard JSON'larını **otomatik import
  ederken datasource girdisini çözmez** — sidecar ConfigMap'i doğrudan Grafana dashboard API'sine gönderir,
  import sihirbazı devreye girmez. Sonuç: dashboard `/api/search` içinde GÖRÜNÜR (e2e bunu doğrular) ama panel
  sorguları `${DS_PROMETHEUS}` değişkenini çözemeyen bir Grafana'da veri göstermeyebilir. Bu kurulumda tek bir
  Prometheus datasource'u (kube-prometheus-stack'in kendi `Prometheus` datasource'u) var ve Grafana çoğu zaman
  tek datasource'u örtük olarak eşler; doğrulanmadıysa (bu görevde `/api/search` ile sınırlı kalındı) panelin
  boş geldiği görülürse: dashboard ayarlarından "Prometheus" datasource'unu manuel seçip kaydetmek yeterlidir.
- `trino-20208.json`: panel seviyesinde `${DS_ALIYUN-MIMIR}` (dashboard'un ihraç edildiği kaynağın adı) referansı
  var, `__inputs[0].name == "DS_ALIYUN-MIMIR"`; ayrıca ayrı bir `templating.list` değişkeni olarak
  `datasource` (tip `datasource`, sorgu `prometheus`) tanımlı. İkisi birbirini otomatik çözmez — aynı manuel
  datasource seçimi burada da geçerlidir. Dashboard adı "Trino Cluster JMX"tir (grafana.com sayfasındaki
  "20208" başlığından farklı), e2e araması `Trino` alt dizesiyle eşleşir.

## Güncelleme

Bu dosyalar `curl` ile indirilmiştir, elle değiştirilmemiştir. Sürüm yükseltmede aynı URL'lerden yeniden
indirilip commit edilmelidir; diff küçükse review'da görünür.
