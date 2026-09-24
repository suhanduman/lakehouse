#!/usr/bin/env bash
# Belge kapısı: bağlantı/yol, bash sözdizimi, yer tutucu, site anahtarları, yasak sözcükler.
# Kapsam: README.md, custom/README.md, examples/**/*.md, docs/**/*.md (docs/internal/** hariç).
# Çıkış 0 = temiz, 1 = en az bir HATA satırı basıldı. bash gerektirir (mapfile/readarray KULLANILMAZ,
# böylece macOS'un öntanımlı bash 3.2'sinde de çalışır); CI (ubuntu-latest) bash 5 kullanır.
set -uo pipefail

if [ -z "${BASH_VERSION:-}" ]; then
  echo "HATA: bu betik bash gerektirir; 'sh scripts/check-docs.sh' değil 'bash scripts/check-docs.sh' ile çalıştırın." >&2
  exit 1
fi

cd "$(dirname "$0")/.."

# Kapsamdaki dosyalar (mapfile yerine taşınabilir while-read döngüsü; bash 3.2 uyumlu)
FILES=()
while IFS= read -r f; do
  [ -z "$f" ] && continue
  FILES+=("$f")
done < <( { ls README.md custom/README.md 2>/dev/null; find docs examples -name '*.md' -not -path 'docs/internal/*' 2>/dev/null; } | sort -u )

fail=0
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

for f in "${FILES[@]}"; do
  [ -e "$f" ] || continue

  # 1) bağlantı ve yol atıfları: `](yol)` ve backtick içindeki '/' geçen bilinen uzantılı yollar
  while read -r p; do
    [ -z "$p" ] && continue
    [ -e "$(dirname "$f")/$p" ] || [ -e "$p" ] || { echo "HATA $f: bağlantı yok: $p"; fail=1; }
  done < <(grep -oE '\]\([^)#]+' "$f" | sed 's/](//' | grep -vE '^https?://|^mailto:' || true)

  while read -r p; do
    [ -z "$p" ] && continue
    [ -e "$p" ] || { echo "HATA $f: yol yok: $p"; fail=1; }
  done < <(grep -oE '`[A-Za-z0-9_./-]+\.(md|yaml|yml|sh|py|env\.example|ini|json)`' "$f" | tr -d '`' | grep '/' | sort -u || true)

  # 2) ```bash / ```sh blokları bash -n ile sözdizimi kontrolünden geçmeli
  awk -v D="$tmp" -v B="$(basename "$f")" \
    '/^```(bash|sh)[ \t]*$/{inb=1;n++;out=D"/"B"_"n".sh";next} /^```/{inb=0;next} inb{print > out}' "$f"
  for b in "$tmp"/"$(basename "$f")"_*.sh; do
    [ -e "$b" ] || continue
    bash -n "$b" 2>"$tmp/err" || { echo "HATA $f: bash bloğu sözdizimi ($(basename "$b")):"; cat "$tmp/err"; fail=1; }
    rm -f "$b"
  done

  # 3) yer tutucu: <...> deseni izin listesinde değilse HATA (allow-list kaçış yoludur)
  while IFS=: read -r ln tok; do
    [ -z "$tok" ] && continue
    grep -qxF "$tok" scripts/check-docs-allow.txt || { echo "HATA $f:$ln: yer tutucu $tok izin listesinde değil"; fail=1; }
  done < <(grep -noE '<[A-Za-z_çğıöşüÇĞİÖŞÜ -]{2,}>' "$f" || true)
done

# 5) yasak sözcükler (eski isimler / yarım bırakılmış not işaretleri)
if [ "${#FILES[@]}" -gt 0 ] && grep -nE 'lakehouse-students|student1|\bTODO\b|\bTBD\b' "${FILES[@]}"; then
  echo "HATA: yasak sözcük"
  fail=1
fi

# 4) site anahtarları docs/90-referans/values-anahtarlari.md'de belgelenmiş mi (dosya yoksa atla)
if [ -f docs/90-referans/values-anahtarlari.md ]; then
  python3 - <<'PY' || fail=1
import yaml, sys
doc = open("docs/90-referans/values-anahtarlari.md", encoding="utf-8").read()
miss = []

def leaves(d, pre=""):
    for k, v in (d or {}).items():
        p = f"{pre}.{k}" if pre else k
        if isinstance(v, dict) and v:
            yield from leaves(v, p)
        else:
            yield p

for name in ["glue", "trino", "jupyterhub"]:
    data = yaml.safe_load(open(f"platform/values/site/{name}.yaml", encoding="utf-8"))
    for k in leaves(data):
        if f"`{k}`" not in doc:
            miss.append(f"{name}: {k}")

for m in miss:
    print("HATA values-anahtarlari.md eksik:", m)
sys.exit(1 if miss else 0)
PY
fi

# 6) satır uzunluğu: 100 Unicode karakteri aşan satır HATA.
# MUAF: fenced kod bloğu içi (```), tablo satırları (| ile başlar) ve gerçek çıktı taşıyan
# alıntı satırları (> ile başlar). Ölçüm python3 ile: UTF-8 bayt değil KARAKTER sayılır.
printf '%s\n' "${FILES[@]}" > "$tmp/files.txt"
python3 - "$tmp/files.txt" <<'PY6' || fail=1
import sys
bad = 0
for path in open(sys.argv[1], encoding="utf-8").read().split():
    infence = False
    for n, line in enumerate(open(path, encoding="utf-8"), 1):
        line = line.rstrip("\n")
        st = line.lstrip()
        if st.startswith("```"):
            infence = not infence
            continue
        if infence or st.startswith("|") or st.startswith(">"):
            continue
        if len(line) > 100:
            print(f"HATA {path}:{n}: satir {len(line)} karakter (>100)")
            bad += 1
sys.exit(1 if bad else 0)
PY6

# 7) glue/templates/monitoring.yaml'daki her `runbook: <dosya>.md#<çapa>` değeri hedef dosyada
# `<a id="<çapa>"></a>` olarak bulunmalı. Böylece alarm annotation'ı ile belge çapası ÇİFT yönlü
# kilitlenir (glue/tests/monitoring_test.yaml annotation değerinin kendisini kilitler).
if [ -f glue/templates/monitoring.yaml ]; then
  python3 - <<'PY7' || fail=1
import re, sys
src = open("glue/templates/monitoring.yaml", encoding="utf-8").read()
miss = []
for f, anchor in sorted(set(re.findall(r"""runbook:\s*([^\s,}"']+\.md)#([A-Za-z0-9_-]+)""", src))):
    try:
        doc = open(f, encoding="utf-8").read()
    except OSError:
        miss.append(f"{f}#{anchor} (dosya yok)")
        continue
    if f'<a id="{anchor}"></a>' not in doc:
        miss.append(f"{f}#{anchor}")
for m in miss:
    print("HATA runbook capasi yok:", m)
sys.exit(1 if miss else 0)
PY7
fi

[ "$fail" = 0 ] && echo "check-docs: OK"
exit "$fail"
