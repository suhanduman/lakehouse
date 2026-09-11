# nginx erişim logu ajanı (Fluent Bit 5.1)

Akış: `tail` → `parser nginx` (zaman ayrıştırması ajanda) → `lua` (`ts` = epoch ms) → Kafka `nginx.access` (SASL_SSL/SCRAM, dış listener) → Iceberg sink → `nginx_raw.access_log` (`day(ts)`), ham IP saklanır (şartnamede KVKK maddesi yok; istenirse maskeleme sink SMT'siyle eklenir).

1. Küme tarafı: `platform/values/glue.yaml` → `nginx.enabled: true`, `kafka.externalListener: true` → commit/push → ArgoCD (`KafkaTopic/nginx.access`, `KafkaUser/fluentbit`, `KafkaConnector/sink-nginx`).
2. Bağlantı bilgileri (kümeden):
   - bootstrap: OpenShift `kubectl -n lakehouse get kafka lakehouse -o jsonpath='{.status.listeners[?(@.name=="external")].bootstrapServers}'` (Route host:443); vanilla nodeport: `<düğüm-IP>:$(kubectl -n lakehouse get svc lakehouse-kafka-external-bootstrap -o jsonpath='{.spec.ports[0].nodePort}')`.
   - CA: `kubectl -n lakehouse get secret lakehouse-cluster-ca-cert -o jsonpath='{.data.ca\.crt}' | base64 -d > /etc/fluent-bit/lakehouse-ca.crt`
   - parola: `kubectl -n lakehouse get secret fluentbit -o jsonpath='{.data.password}' | base64 -d`
3. Sunucuda: Fluent Bit 5.1 paketi (fluentbit.io/install); `agents/fluent-bit/fluent-bit.conf` + `parsers.conf` → `/etc/fluent-bit/`; `/etc/default/fluent-bit`:
   `KAFKA_BOOTSTRAP=<host:port>` `KAFKA_PASSWORD=<parola>` `KAFKA_CA=/etc/fluent-bit/lakehouse-ca.crt` `NGINX_ACCESS_LOG=/var/log/nginx/access.log` `READ_FROM_HEAD=off`; `systemctl enable --now fluent-bit`.
4. Doğrulama: `kubectl -n lakehouse get kafkaconnector sink-nginx` Ready; ≤ 5 dk içinde `nginx_raw.access_log` (Trino F4 / pyiceberg). Ajan logu: `journalctl -u fluent-bit`.
Notlar: disk tamponu `storage.type filesystem` (Kafka kesintisinde 1G'a kadar); log formatı `combined` dışındaysa `parsers.conf` regex'i güncelle; TR-locale sorunu yok (ay adları ajanda `%b` ile çözülür).
