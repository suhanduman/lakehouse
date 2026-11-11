#!/usr/bin/env bash
# Kendi Spark işiniz için kullanıma hazır bir tanım dosyası üretir (docs/50-isletme/yeni-spark-uygulamasi.md §4).
# Kümeye özel bütün ayarlar (imaj, sürümler, S3 adresi ve sertifikası, katalog, Maven önbelleği, kimlikler)
# kümede çalışan ürün işinden (silver-merge) kopyalanır; elle düzeltilecek satır kalmaz.
#
#   scripts/spark-is-tanimi.sh --ad gunluk-ozet --dosya gunluk_ozet.py                    > tek seferlik iş
#   scripts/spark-is-tanimi.sh --ad gunluk-ozet --dosya gunluk_ozet.py --zaman "0 4 * * *" > zamanlı iş
#
#   --ad        işin adı (küçük harf, rakam, tire); sürücü pod'u <ad>-driver olur
#   --dosya     çalışacak Python dosyasının adı (ConfigMap'in içindeki dosya adı)
#   --configmap kodu taşıyan ConfigMap'in adı (varsayılan: --ad ile aynı)
#   --zaman     verilirse zamanlı iş (ScheduledSparkApplication) üretilir; cron biçimi, saatler UTC
#
# Çıktı standart çıktıya yazılır; bir dosyaya yönlendirin. Ürün yükseltmesinden sonra aynı komutu tekrar
# çalıştırmak dosyayı yeni sürümlerle yeniden üretir.
# Ortam: LAKEHOUSE_NS (varsayılan lakehouse). Gereken araçlar: oc, jq.
set -euo pipefail
NS="${LAKEHOUSE_NS:-lakehouse}"
AD=""; DOSYA=""; CM=""; ZAMAN=""
while [[ $# -gt 0 ]]; do case "$1" in
  --ad) AD="$2"; shift 2;; --dosya) DOSYA="$2"; shift 2;; --configmap) CM="$2"; shift 2;;
  --zaman) ZAMAN="$2"; shift 2;; -h|--help) sed -n '2,16p' "$0" | sed 's/^# \{0,1\}//'; exit 0;;
  *) echo "bilinmeyen argüman: $1 (yardım: $0 --help)" >&2; exit 2;; esac; done
[[ -n "$AD" && -n "$DOSYA" ]] || { echo "HATA: --ad ve --dosya zorunludur (yardım: $0 --help)" >&2; exit 2; }
[[ "$AD" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ && ${#AD} -le 40 ]] \
  || { echo "HATA: --ad yalnız küçük harf, rakam ve tire içerebilir, en çok 40 karakter: $AD" >&2; exit 2; }
CM="${CM:-$AD}"

SABLON=$(oc -n "$NS" get scheduledsparkapplication silver-merge -o json 2>/dev/null) \
  || { echo "HATA: kümede silver-merge işi okunamadı (oc oturumu ve ad alanı $NS doğru mu?)" >&2; exit 1; }

# Ürün işinin tanımından yalnız işe özgü üç alan değişir: çalışacak dosya, argümanlar (yok) ve kod ConfigMap'i.
TANIM=$(jq --arg dosya "$DOSYA" --arg cm "$CM" '
  .spec.template
  | del(.arguments)
  | .mainApplicationFile = "local:///opt/job/\($dosya)"
  | .volumes = [.volumes[] | if .name == "job" then {name: "job", configMap: {name: $cm}} else . end]
' <<<"$SABLON")

if [[ -n "$ZAMAN" ]]; then
  NESNE=$(jq --arg ad "$AD" --arg ns "$NS" --arg zaman "$ZAMAN" '{
    apiVersion: "sparkoperator.k8s.io/v1beta2", kind: "ScheduledSparkApplication",
    metadata: {name: $ad, namespace: $ns},
    spec: {schedule: $zaman, concurrencyPolicy: "Forbid", successfulRunHistoryLimit: 3,
           failedRunHistoryLimit: 3, template: .}}' <<<"$TANIM")
  TUR="zamanlı iş ($ZAMAN, UTC) — Git'e konur: custom/ klasörü + configMapGenerator"
else
  NESNE=$(jq --arg ad "$AD" --arg ns "$NS" '{
    apiVersion: "sparkoperator.k8s.io/v1beta2", kind: "SparkApplication",
    metadata: {name: $ad, namespace: $ns}, spec: .}' <<<"$TANIM")
  TUR="tek seferlik iş — Git'e KONMAZ, oc apply -f ile elle uygulanır"
fi

IMAJ=$(jq -r '.spec.template.image' <<<"$SABLON")
echo "# $AD: $TUR"
echo "# Kod: ConfigMap '$CM' içindeki $DOSYA. Ayarlar kümedeki silver-merge işinden kopyalandı (imaj $IMAJ)."
echo "# scripts/spark-is-tanimi.sh ile üretildi; ürün yükseltmesinden sonra aynı komutla yeniden üretin."
oc create --dry-run=client -o yaml -f - <<<"$NESNE" | grep -v '^  creationTimestamp: null$'
