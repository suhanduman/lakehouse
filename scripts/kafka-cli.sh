#!/usr/bin/env bash
# Kafka yönetim araçlarını (bin/kafka-*.sh) broker pod'unun İÇİNDE, yönetici kimliğiyle koşturur.
#   scripts/kafka-cli.sh <araç> [argümanlar]
#   ör. scripts/kafka-cli.sh kafka-topics.sh --list
#       scripts/kafka-cli.sh kafka-consumer-groups.sh --describe --group connect-sink-shop
#
# Neden: üretimde kimliksiz `plain` dinleyici yoktur (glue kafka.plainListener: false) -> broker pod'unda bile
# `--bootstrap-server localhost:9092` cevap vermez. Araç burada TLS 9093 + SCRAM ile, `kafka-admin` kimliğiyle
# (glue/templates/kafka-admin.yaml, Kafka superUsers) bağlanır; geliştirme kümesinde de aynı komut çalışır.
# --bootstrap-server ve --command-config bu betik verir; siz yalnız aracın kendi argümanlarını yazarsınız.
# Parola ve CA, oc/kubectl ile okunup stdin'den geçirilir; pod'da geçici dizine yazılır ve iş bitince silinir.
#
# Ortam: LAKEHOUSE_NS (varsayılan lakehouse), KAFKA_POD (varsayılan lakehouse-dual-role-0).
set -euo pipefail
[[ $# -ge 1 ]] || { sed -n '2,5p' "$0" | sed 's/^# \{0,1\}//'; exit 2; }
NS="${LAKEHOUSE_NS:-lakehouse}"
POD="${KAFKA_POD:-lakehouse-dual-role-0}"
K="$(command -v oc || command -v kubectl)" || { echo "oc ya da kubectl gerekli" >&2; exit 1; }
TOOL="$1"; shift

JAAS="$("$K" -n "$NS" get secret kafka-admin -o jsonpath='{.data.sasl\.jaas\.config}' | base64 -d)" \
  || { echo "kafka-admin Secret'ı okunamadı (glue sürümü eski mi? KafkaUser kafka-admin Ready mi?)" >&2; exit 1; }
CA_B64="$("$K" -n "$NS" get secret lakehouse-cluster-ca-cert -o jsonpath='{.data.ca\.crt}')"

# stdin: 1. satır CA (base64, tek satır), sonrası istemci ayarları
{ printf '%s\n' "$CA_B64"
  printf '%s\n' "security.protocol=SASL_SSL" "sasl.mechanism=SCRAM-SHA-512" "ssl.truststore.type=PEM" \
    "sasl.jaas.config=$JAAS"
} | "$K" -n "$NS" exec -i "$POD" -c kafka -- sh -c '
  d=$(mktemp -d); trap "rm -rf \"$d\"" EXIT
  read -r ca; printf "%s" "$ca" | base64 -d > "$d/ca.crt"
  cat > "$d/admin.properties"; echo "ssl.truststore.location=$d/ca.crt" >> "$d/admin.properties"
  t=$1; shift
  bin/"$t" --bootstrap-server lakehouse-kafka-bootstrap:9093 --command-config "$d/admin.properties" "$@"
' sh "$TOOL" "$@"
