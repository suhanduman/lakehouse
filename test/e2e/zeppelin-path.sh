#!/usr/bin/env bash
# e2e F4 Zeppelin yolu: Shiro login (kind: dev kullanıcıları, üretim: AD test hesapları — lib.sh ACC_*) -> jdbc interpreter READY (trino-jdbc Maven'den iner) -> not + %jdbc paragrafı
# -> senkron çalıştır (Trino'ya TLS + servis hesabı ile) -> sonuç 3
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"; NS=lakehouse
# shellcheck source=/dev/null
source "$ROOT/test/e2e/lib.sh"
kubectl -n "$NS" rollout status deploy/zeppelin --timeout=900s
kubectl -n "$NS" port-forward svc/zeppelin 18081:8080 >/dev/null 2>&1 & PF=$!; trap 'kill $PF 2>/dev/null' EXIT; sleep 3
CJ=$(mktemp)
curl -sS -c "$CJ" --data-urlencode "userName=$ACC_ANALYST_USER" --data-urlencode "password=$ACC_ANALYST_PASS" localhost:18081/api/login | jq -e '.status=="OK"' >/dev/null || { echo "HATA zeppelin shiro login $ACC_ANALYST_USER"; exit 1; }
echo "OK zeppelin shiro login $ACC_ANALYST_USER"
[[ "$(curl -sS -o /dev/null -w '%{http_code}' --data-urlencode "userName=$ACC_USER_USER" -d 'password=yanlis-parola' localhost:18081/api/login)" == "403" ]] || { echo "HATA zeppelin wrong password 403 beklendi"; exit 1; }
echo "OK zeppelin wrong password 403"
# Rol kapısı ([urls] son kuralı anyofroles[admin, analyst, user]): kimlik doğrulamak YETMEZ. Rolsüz dev
# kullanıcısı nogroup1 giriş yapabilir ama hiçbir veri uç noktasına erişemez (aksi halde paylaşımlı `zeppelin`
# Trino servis hesabıyla tüm kataloğu okurdu — final review C1).
NJ=$(mktemp)
curl -sS -c "$NJ" --data-urlencode "userName=$ACC_NOGROUP_USER" --data-urlencode "password=$ACC_NOGROUP_PASS" localhost:18081/api/login | jq -e '.status=="OK"' >/dev/null || { echo "HATA zeppelin $ACC_NOGROUP_USER (grupsuz hesap) girişi başarısız (hesap var mı, parola doğru mu?)"; exit 1; }
NC=$(curl -sS -b "$NJ" -o /dev/null -w '%{http_code}' localhost:18081/api/notebook)
[[ "$NC" == "401" ]] || { echo "HATA zeppelin rol kapısı: rolsüz kullanıcı /api/notebook için 401 beklendi, gelen $NC"; exit 1; }
echo "OK zeppelin rol kapısı: rolsüz kullanıcı 401 (giriş yapsa bile veri göremez)"
# Bağımlılık indirmesi SUNUCU AÇILIŞINDA başlar ve asenkrondur; status READY olmadan paragraf
# "Interpreter Setting 'jdbc' is not ready, its status is DOWNLOADING_DEPENDENCIES" ile düşer.
# Üretimde /api/interpreter/** yalnız admin rolüne açıktır (examples/zeppelin/shiro-ad.ini; analist 401 alır —
# ocpphy kabulü 2026-11-09). O durumda durum sorgulanamaz; hazır olma, aşağıda paragrafın yeniden denenmesiyle
# beklenir. Geliştirme Shiro dosyası bu uçları analist rolüne açar (dev'de admin kullanıcısı yok) -> eski yol.
ISC=$(curl -sS -b "$CJ" -o /dev/null -w '%{http_code}' localhost:18081/api/interpreter/setting)
ST=""
[[ "$ISC" == "200" ]] || { ST=SKIP; echo "OK zeppelin yorumlayıcı ayarları $ACC_ANALYST_USER için kapalı ($ISC; üretimde yalnız admin)"; }
for _ in $(seq 1 90); do
  [[ "$ST" == "SKIP" ]] && break
  ST=$(curl -sS -b "$CJ" localhost:18081/api/interpreter/setting | jq -r '.body[] | select(.name=="jdbc") | .status')
  [[ "$ST" == "READY" ]] && break
  [[ "$ST" == "DOWNLOADING_DEPENDENCIES" ]] || { echo "HATA jdbc interpreter durumu: $ST"; curl -sS -b "$CJ" localhost:18081/api/interpreter/setting | jq -r '.body[] | select(.name=="jdbc") | .errorReason'; exit 1; }
  sleep 10
done
[[ "$ST" == "READY" || "$ST" == "SKIP" ]] || { echo "HATA jdbc bağımlılıkları 15 dk içinde inmedi (Maven Central erişimi?)"; exit 1; }
[[ "$ST" == "READY" ]] && echo "OK zeppelin jdbc interpreter READY (io.trino:trino-jdbc Maven'den indi)"
# Zeppelin 0.12.1 POST /api/notebook'ta "name" alanını YOK SAYAR: not her zaman "/Untitled Note" olur.
# Önceki (yarıda kalmış) koşudan artan not ikinci koşuda "Note '/Untitled Note' existed" ile çakışır -> önce temizle.
for ID in $(curl -sS -b "$CJ" localhost:18081/api/notebook | jq -r '.body[] | select(.path=="/Untitled Note") | .id'); do
  curl -sS -b "$CJ" -X DELETE "localhost:18081/api/notebook/$ID" >/dev/null
done
NOTE=$(curl -sS -b "$CJ" -H 'Content-Type: application/json' -d '{"paragraphs":[{"title":"count","text":"%jdbc select count(*) c from shop.orders"}]}' localhost:18081/api/notebook | jq -r .body)
[[ -n "$NOTE" && "$NOTE" != "null" ]] || { echo "HATA zeppelin not yaratılamadı"; exit 1; }
PARA=$(curl -sS -b "$CJ" "localhost:18081/api/notebook/$NOTE" | jq -r '.body.paragraphs[0].id')
# senkron run (ilk sorgu interpreter JVM'ini başlatır)
# Bağımlılıklar henüz inmediyse ("… is not ready, its status is DOWNLOADING_DEPENDENCIES") bekleyip yeniden dene
for _ in $(seq 1 90); do
  OUT=$(curl -sS -b "$CJ" --max-time 900 -X POST "localhost:18081/api/notebook/run/$NOTE/$PARA")
  grep -q "DOWNLOADING_DEPENDENCIES" <<<"$OUT" || break
  sleep 10
done
echo "$OUT" | jq -e '.body.code=="SUCCESS"' >/dev/null || { echo "HATA zeppelin %jdbc paragrafı"; echo "$OUT" | head -c 2000; kubectl -n "$NS" logs deploy/zeppelin --tail=60; exit 1; }
echo "$OUT" | jq -e '.body.msg[0].type=="TABLE"' >/dev/null || { echo "HATA zeppelin sonucu TABLE değil"; echo "$OUT" | head -c 2000; exit 1; }
# TABLE verisi "c\n3\n": başlık satırı c, ikinci satır sayım
[[ "$(echo "$OUT" | jq -r '.body.msg[0].data' | sed -n 2p)" == "3" ]] || { echo "HATA zeppelin shop.orders sayımı 3 değil"; echo "$OUT" | head -c 2000; exit 1; }
echo "OK zeppelin %jdbc -> trino shop.orders == 3"
curl -sS -b "$CJ" -X DELETE "localhost:18081/api/notebook/$NOTE" >/dev/null
echo "E2E F4 ZEPPELIN OK"
