#!/usr/bin/env bash
# Bir nesnenin nereden geldiğini söyler: Git'ten mi (ArgoCD yönetir), Git'teki bir nesneden türetilmiş mi
# (operatör üretir), yoksa elle mi yaratıldı (ve hangi belgenin hangi adımında).
#
#   scripts/nesne-kaynagi.sh secret/universite-db kafkaconnect/connect
#   scripts/nesne-kaynagi.sh --liste secret          # ad alanındaki bütün Secret'lar
#
# Sonuç sütunu:
#   GIT        ArgoCD yönetir. Elle değiştirilir ya da silinirse birkaç dakikada Git'teki hâline döner.
#   TÜREV      Bir operatör, Git'teki bir nesneden üretir (zincir yazılır). Silinirse operatör yeniden üretir.
#   PLATFORM   OpenShift'in kendisi üretir (ör. servis hesaplarının imaj çekme Secret'ları).
#   ELLE       Bir kurulum ya da işletme belgesindeki adımla elle yaratıldı (etiket: lakehouse.io/yonetim).
#              Silinirse kimse geri getirmez; o adım tekrarlanır.
#   ETİKETSİZ  Hiçbiri değil: belgelerin dışında elle yaratılmış bir nesne.
#
# Ortam: LAKEHOUSE_NS (varsayılan lakehouse). Gereken araçlar: oc (ya da kubectl), jq.
set -euo pipefail
NS="${LAKEHOUSE_NS:-lakehouse}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
KC=$(command -v oc || command -v kubectl)

belge() {   # "manual-30-kurulum-5.3" -> "docs/30-kurulum.md §5.3 — <başlık>"
  local rest="${1#manual-}" f b sec h
  while IFS= read -r f; do
    b=$(basename "$f" .md)
    if [[ "$rest" == "$b" || "$rest" == "$b-"* ]]; then
      sec="${rest#"$b"}"; sec="${sec#-}"
      if [[ -n "$sec" ]]; then
        h=$(grep -m1 -E "^#+ ${sec//./\\.}[ .]" "$f" | sed -E 's/^#+ [0-9.]+ *//' || true)
        echo "${f#"$ROOT"/} §${sec}${h:+ — $h}"
      else
        echo "${f#"$ROOT"/}"
      fi
      return
    fi
  done < <(find "$ROOT/docs" -name '*.md' -not -path '*/internal/*' | awk '{print length, $0}' | sort -rn | cut -d' ' -f2-)
  echo "(belge bulunamadı: $1)"
}

kaynak() {   # $1 = tür/ad  ->  "SONUÇ<TAB>açıklama"
  local ref="$1" derin="${2:-0}" j tid yon okind oname oapi grp ust usonuc uacik
  if ! j=$("$KC" -n "$NS" get "$ref" -o json 2>/dev/null); then
    printf 'YOK\tkümede bulunamadı\n'; return
  fi
  tid=$(jq -r '.metadata.annotations["argocd.argoproj.io/tracking-id"] // empty' <<<"$j")
  yon=$(jq -r '.metadata.labels["lakehouse.io/yonetim"] // empty' <<<"$j")
  if [[ -n "$tid" ]]; then
    printf 'GIT\tArgoCD uygulaması: %s\n' "${tid%%:*}"; return
  fi
  if [[ -n "$yon" ]]; then
    printf 'ELLE\t%s\n' "$(belge "$yon")"; return
  fi
  local cert; cert=$(jq -r '.metadata.annotations["cert-manager.io/certificate-name"] // empty' <<<"$j")
  if [[ -n "$cert" && "$derin" -lt 6 ]]; then        # cert-manager Secret'a sahiplik yazmaz, adını not eder
    ust=$(kaynak "certificate.cert-manager.io/$cert" $((derin + 1)))
    printf 'TÜREV\tCertificate/%s -> %s\n' "$cert" "${ust#*$'\t'}"; return
  fi
  if [[ "$ref" == secret/spark-operator-webhook-certs ]]; then   # operatör açılışta kendisi yazar, iz bırakmaz
    printf 'TÜREV\tspark-operator açılışta üretir -> ArgoCD uygulaması: spark-operator\n'; return
  fi
  okind=$(jq -r '.metadata.ownerReferences[0].kind // empty' <<<"$j")
  if [[ "$okind" == ServiceAccount && -n "$(jq -r '.metadata.annotations["openshift.io/internal-registry-auth-token.service-account"] // empty' <<<"$j")" ]]; then
    printf 'PLATFORM\tOpenShift: servis hesabının imaj çekme Secret'"'"'ı\n'; return
  fi
  if [[ -n "$okind" && "$derin" -lt 6 ]]; then
    oname=$(jq -r '.metadata.ownerReferences[0].name' <<<"$j")
    oapi=$(jq -r '.metadata.ownerReferences[0].apiVersion' <<<"$j")
    grp="${oapi%/*}"; [[ "$grp" == "$oapi" ]] && grp=""
    ust=$(kaynak "$(tr '[:upper:]' '[:lower:]' <<<"$okind")${grp:+.$grp}/$oname" $((derin + 1)))
    usonuc="${ust%%$'\t'*}"; uacik="${ust#*$'\t'}"
    case "$usonuc" in
      GIT|TÜREV) printf 'TÜREV\t%s/%s -> %s\n' "$okind" "$oname" "$uacik" ;;
      *)         printf 'TÜREV\t%s/%s (%s: %s)\n' "$okind" "$oname" "$usonuc" "$uacik" ;;
    esac
    return
  fi
  printf 'ETİKETSİZ\tGit, operatör ya da belge kaydı yok\n'
}

if [[ "${1:-}" == "--liste" ]]; then
  [[ -n "${2:-}" ]] || { echo "kullanım: $0 --liste <tür>   (ör. secret)"; exit 2; }
  REFS=$("$KC" -n "$NS" get "$2" -o name)
elif [[ $# -gt 0 ]]; then
  REFS=$(printf '%s\n' "$@")
else
  sed -n '2,15p' "$0" | sed 's/^# \{0,1\}//'; exit 2
fi
while IFS= read -r r; do
  [[ -n "$r" ]] || continue
  out=$(kaynak "$r"); son="${out%%$'\t'*}"
  case "$son" in           # Türkçe harfler iki bayt: printf genişliği bayt sayar, elle hizalanır
    TÜREV) son="TÜREV     " ;; ETİKETSİZ) son="ETİKETSİZ " ;; *) son=$(printf '%-10s' "$son") ;;
  esac
  printf '%-44s %s %s\n' "$r" "$son" "${out#*$'\t'}"
done <<<"$REFS"
