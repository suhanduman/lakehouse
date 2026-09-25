#!/usr/bin/env bash
# MinIO ve mc DEV imajlarının kaynaktan derlenmiş GHCR aynasını üretir.
#
# NEDEN VAR?
# MinIO Inc. 2026-09-24 tarihinde topluluk container imajlarını geri çekti: `quay.io/minio/minio`,
# `quay.io/minio/mc` (ve Docker Hub karşılıkları) anonim çekişte 401 döndürüyor. GitHub'daki
# `minio/minio` ve `minio/mc` depoları arşivlendi ama OKUNABİLİR durumda ve sürüm etiketlerini
# hâlâ taşıyor. Bu üründe MinIO YALNIZCA dev/e2e S3'üdür (üretimde müşterinin S3'ü kullanılır),
# ama kind e2e koşusu imajı çekemeyince düşüyordu. Karar: aynı etiketleri KAYNAKTAN derleyip
# kendi AGPL-3.0 aynamız olarak GHCR'ye yayımlamak.
#
# AGPL-3.0 YÜKÜMLÜLÜKLERİ — bu betik "Corresponding Source" hikâyesinin parçasıdır:
#   * Kaynak: derleme, fork'ların ETİKETLİ commit'inden yapılır ve imaj etiketleri
#     (`org.opencontainers.image.source` / `.revision` / `.version`) o fork'u gösterir.
#   * Lisans metni: kaynak ağacındaki `LICENSE` ve `CREDITS` imajın içinde `/licenses/`
#     altında KORUNUR (upstream `Dockerfile.release` ile aynı yer).
#   * Upstream kaynağa YAMA UYGULANMAZ; derleme bayrakları upstream `Makefile` ve
#     `buildscripts/gen-ldflags.go` ile birebir aynıdır.
#   * Ayna RESMİ DEĞİLDİR ve MinIO Inc. ile ilişkisi yoktur; etiketler bunu açıkça yazar.
#
# UPSTREAM TARİFİNDEN BİLİNÇLİ SAPMALAR (hepsi kaydedilmiştir):
#   1. Upstream `Dockerfile.release` ikili dosyaları KAYNAKTAN DERLEMEZ; `dl.min.io`dan hazır
#      ikili indirip minisign ile doğrular. dl.min.io bizim için bir dağıtım kaynağı değildir
#      (ve AGPL "Corresponding Source" hikâyesini zayıflatır), bu yüzden `make build` hedefinin
#      go derleme satırını kullanırız — ldflags upstream `gen-ldflags.go` ÇIKTISIDIR.
#   2. Upstream imajdaki statik `curl` (`dockerscripts/download-static-curl.sh`) EKLENMEZ:
#      üçüncü taraf bir ikiliyi ağdan çekip yeniden dağıtmak lisans hikâyesini bulandırır ve
#      bu üründe MinIO pod'unda curl kullanan hiçbir yol yoktur.
#   3. Upstream `RUN chmod -R 777 /usr/bin` yapar (OpenShift'in rastgele UID'si için). Bizim
#      Dockerfile'ımız yalnız COPY içerir (emülasyonsuz çapraz derleme için), bu yüzden ikili
#      dosyalara host'ta 0755 verilir — rastgele UID için okuma+çalıştırma yeterlidir.
#   4. CA demeti `ubi-minimal`den KOPYALANIR (upstream `microdnf install` ile kurar); ilgili
#      dosya `ubi-minimal:9.6` içinde zaten hazırdır, `RUN` gerekmez.
#   5. `minio` imajı upstream'de olduğu gibi `mc` ikilisini de taşır: docs/50-isletme/
#      kaynak-veya-tablo-silme.md `oc exec deploy/minio -- sh -c 'mc ...'` çağırır.
#   6. Upstream `MINIO_UPDATE_MINISIGN_PUBKEY` ortam değişkenini kurar; biz onun yerine
#      `MINIO_UPDATE=off` veririz. Bizim ikilimiz MinIO'nun minisign anahtarıyla İMZALI
#      DEĞİLDİR (olamaz da); `minio update` yolu açık kalsaydı aynayı sessizce upstream
#      ikilisiyle değiştirmeye çalışıp düşerdi.
#
# GO SÜRÜMÜ — TEK GÖRÜNÜR FARK: kaynak ağaçların `go.mod` dosyaları `toolchain` satırı taşır
# (minio go1.24.2, mc go1.23.10) ve upstream resmî ikiliyi golang:1.24 ile derler. GOTOOLCHAIN=auto
# YALNIZCA YUKARI çıkar: yereldeki Go beyan edilenden yeniyse o kullanılır, indirme olmaz. Bu
# yüzden ayna ikilisi `Runtime: go1.26.5` yazar, resmî ikili `go1.24.x` yazardı. `version`,
# `ReleaseTag` ve `commit-id` BİREBİR AYNIDIR; fark yalnız derleyici sürümüdür ve güvenlik
# yamaları açısından yeni Go lehinedir. Upstream'in beyan ettiği zinciri birebir istersek:
#   GOTOOLCHAIN=go1.24.2 scripts/build-minio-mirror.sh
#
# KULLANIM
#   scripts/build-minio-mirror.sh              # derle + GHCR'ye it
#   PUSH=0 scripts/build-minio-mirror.sh       # yalnız yerelde derle
# Gerekenler: git, go, podman; PUSH=1 için `podman login ghcr.io` yapılmış olmalıdır.

set -euo pipefail

export GOTOOLCHAIN="${GOTOOLCHAIN:-auto}"

MINIO_TAG="${MINIO_TAG:-RELEASE.2025-09-07T16-13-09Z}"
MC_TAG="${MC_TAG:-RELEASE.2025-08-13T08-35-41Z}"
REGISTRY="${REGISTRY:-ghcr.io/suhanduman}"
PUSH="${PUSH:-1}"

# Etiketlerin İŞARET ETTİĞİ commit'ler. DİKKAT: GitHub API'sinde `git/ref/tags/<etiket>` bu
# depolarda ANNOTATED TAG NESNESİNİN sha'sını döndürür (minio 01ce918d…, mc d6541ea2…); aşağıdaki
# değerler o etiketin çözümlendiği COMMIT'tir ve `gen-ldflags.go`un `commit-id`ye yazdığı değerdir.
MINIO_COMMIT="${MINIO_COMMIT:-07c3a429bfed433e49018cb0f78a52145d4bedeb}"
MC_COMMIT="${MC_COMMIT:-7394ce0dd2a80935aded936b09fa12cbb3cb8096}"

MINIO_REPO="${MINIO_REPO:-https://github.com/suhanduman/minio.git}"
MC_REPO="${MC_REPO:-https://github.com/suhanduman/mc.git}"
MINIO_SOURCE_URL="${MINIO_SOURCE_URL:-https://github.com/suhanduman/minio}"
MC_SOURCE_URL="${MC_SOURCE_URL:-https://github.com/suhanduman/mc}"

# Taban imajlar SOMUT etikete sabitlenir (`:latest` kullanılmaz): ayna yeniden üretilebilir olmalı.
UBI_MICRO="${UBI_MICRO:-registry.access.redhat.com/ubi9/ubi-micro:9.6}"
UBI_MINIMAL="${UBI_MINIMAL:-registry.access.redhat.com/ubi9/ubi-minimal:9.6}"
ARCHES="${ARCHES:-amd64 arm64}"

WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/minio-mirror.XXXXXX")"
trap 'rm -rf "$WORKDIR"' EXIT
OUT="$WORKDIR/out"
mkdir -p "$OUT"

say() { printf '\n==> %s\n' "$*"; }

# --- 1) Kaynağı etiketten al ve commit'i DOĞRULA -----------------------------------------------
clone_at_tag() {
  local url="$1" tag="$2" dir="$3" want="$4"
  say "klon: $url @ $tag"
  git clone --quiet --depth 1 --branch "$tag" "$url" "$dir"
  local got
  got="$(git -C "$dir" rev-parse HEAD)"
  if [ "$got" != "$want" ]; then
    echo "HATA: $url $tag commit'i beklenenden farklı: $got != $want" >&2
    exit 1
  fi
  if [ "$(git -C "$dir" describe --tags)" != "$tag" ]; then
    echo "HATA: $dir describe --tags '$tag' değil" >&2
    exit 1
  fi
  echo "    commit doğrulandı: $got"
}

clone_at_tag "$MINIO_REPO" "$MINIO_TAG" "$WORKDIR/minio" "$MINIO_COMMIT"
clone_at_tag "$MC_REPO" "$MC_TAG" "$WORKDIR/mc" "$MC_COMMIT"

# --- 2) ldflags: upstream buildscripts/gen-ldflags.go ÇIKTISI ----------------------------------
# `make build` bu betiği ARGÜMANSIZ çağırır -> sürüm = commit zamanı (RFC3339). Ön ek ortam
# değişkeninden gelir: verilmezse "DEVELOPMENT" olur, upstream sürüm hattı "RELEASE" verir.
say "ldflags üretimi (upstream gen-ldflags.go)"
MINIO_LDFLAGS="$(cd "$WORKDIR/minio" && MINIO_RELEASE=RELEASE go run buildscripts/gen-ldflags.go)"
MC_LDFLAGS="$(cd "$WORKDIR/mc" && MC_RELEASE=RELEASE go run buildscripts/gen-ldflags.go)"
echo "    minio: $MINIO_LDFLAGS"
echo "    mc   : $MC_LDFLAGS"
case "$MINIO_LDFLAGS" in *"ReleaseTag=$MINIO_TAG "*) ;; *)
  echo "HATA: minio ldflags ReleaseTag=$MINIO_TAG taşımıyor" >&2; exit 1 ;; esac
case "$MC_LDFLAGS" in *"ReleaseTag=$MC_TAG "*) ;; *)
  echo "HATA: mc ldflags ReleaseTag=$MC_TAG taşımıyor" >&2; exit 1 ;; esac

# --- 3) Çapraz derleme (QEMU YOK; Go'nun kendi çapraz derleyicisi) -----------------------------
# Derleme satırları upstream Makefile `build` hedefinden birebir alınmıştır; `checks` /
# `build-debugging` ön hedefleri (golangci-lint, dlv) atlanır — ikiliyi ETKİLEMEZLER.
for arch in $ARCHES; do
  say "derleme: minio linux/$arch"
  ( cd "$WORKDIR/minio" && CGO_ENABLED=0 GOOS=linux GOARCH="$arch" \
      go build -tags kqueue -trimpath --ldflags "$MINIO_LDFLAGS" -o "$OUT/minio-$arch" )
  say "derleme: mc linux/$arch"
  ( cd "$WORKDIR/mc" && GO111MODULE=on CGO_ENABLED=0 GOOS=linux GOARCH="$arch" \
      go build -trimpath -tags kqueue --ldflags "$MC_LDFLAGS" -o "$OUT/mc-$arch" )
  go version -m "$OUT/minio-$arch" | grep -q "GOARCH=$arch" || {
    echo "HATA: minio-$arch ikilisi GOARCH=$arch değil" >&2; exit 1; }
  go version -m "$OUT/mc-$arch" | grep -q "GOARCH=$arch" || {
    echo "HATA: mc-$arch ikilisi GOARCH=$arch değil" >&2; exit 1; }
done
chmod 0755 "$OUT"/minio-* "$OUT"/mc-*

# Lisans metinleri ve giriş betiği imaj bağlamına kopyalanır (AGPL: LICENSE + CREDITS kalır).
cp "$WORKDIR/minio/LICENSE" "$OUT/minio-LICENSE"
cp "$WORKDIR/minio/CREDITS" "$OUT/minio-CREDITS"
cp "$WORKDIR/mc/LICENSE" "$OUT/mc-LICENSE"
cp "$WORKDIR/mc/CREDITS" "$OUT/mc-CREDITS"
cp "$WORKDIR/minio/dockerscripts/docker-entrypoint.sh" "$OUT/docker-entrypoint.sh"
chmod 0755 "$OUT/docker-entrypoint.sh"

# --- 4) Dockerfile'lar: YALNIZ COPY (RUN yok) --------------------------------------------------
# RUN olmaması, `podman build --platform linux/amd64` komutunun arm64 bir Mac'te QEMU
# emülasyonu OLMADAN çalışmasını sağlar: hiçbir hedef-mimari komut çalıştırılmaz.
MINIO_DESC="MinIO $MINIO_TAG — AGPL-3.0, kaynaktan derlenmiş resmi olmayan ayna; MinIO Inc. ile ilişkisi yoktur; kaynak: $MINIO_SOURCE_URL/tree/$MINIO_TAG"
MC_DESC="MinIO mc $MC_TAG — AGPL-3.0, kaynaktan derlenmiş resmi olmayan ayna; MinIO Inc. ile ilişkisi yoktur; kaynak: $MC_SOURCE_URL/tree/$MC_TAG"
VENDOR="suhanduman (resmi olmayan ayna)"

cat >"$OUT/Dockerfile.minio" <<EOF
FROM $UBI_MINIMAL AS certs
FROM $UBI_MICRO
ARG TARGETARCH
LABEL org.opencontainers.image.source="$MINIO_SOURCE_URL" \\
      org.opencontainers.image.version="$MINIO_TAG" \\
      org.opencontainers.image.revision="$MINIO_COMMIT" \\
      org.opencontainers.image.licenses="AGPL-3.0-only" \\
      org.opencontainers.image.vendor="$VENDOR" \\
      org.opencontainers.image.title="minio" \\
      org.opencontainers.image.base.name="$UBI_MICRO" \\
      org.opencontainers.image.description="$MINIO_DESC"
ENV MINIO_ACCESS_KEY_FILE=access_key \\
    MINIO_SECRET_KEY_FILE=secret_key \\
    MINIO_ROOT_USER_FILE=access_key \\
    MINIO_ROOT_PASSWORD_FILE=secret_key \\
    MINIO_KMS_SECRET_KEY_FILE=kms_master_key \\
    MINIO_UPDATE=off \\
    MINIO_CONFIG_ENV_FILE=config.env \\
    MC_CONFIG_DIR=/tmp/.mc
COPY --from=certs /etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem /etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem
COPY --from=certs /etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem /etc/ssl/certs/ca-certificates.crt
COPY minio-\${TARGETARCH} /usr/bin/minio
COPY mc-\${TARGETARCH} /usr/bin/mc
COPY minio-CREDITS /licenses/CREDITS
COPY minio-LICENSE /licenses/LICENSE
COPY mc-CREDITS /licenses/mc/CREDITS
COPY mc-LICENSE /licenses/mc/LICENSE
COPY docker-entrypoint.sh /usr/bin/docker-entrypoint.sh
EXPOSE 9000
VOLUME ["/data"]
ENTRYPOINT ["/usr/bin/docker-entrypoint.sh"]
CMD ["minio"]
EOF

cat >"$OUT/Dockerfile.mc" <<EOF
FROM $UBI_MINIMAL AS certs
FROM $UBI_MICRO
ARG TARGETARCH
LABEL org.opencontainers.image.source="$MC_SOURCE_URL" \\
      org.opencontainers.image.version="$MC_TAG" \\
      org.opencontainers.image.revision="$MC_COMMIT" \\
      org.opencontainers.image.licenses="AGPL-3.0-only" \\
      org.opencontainers.image.vendor="$VENDOR" \\
      org.opencontainers.image.title="mc" \\
      org.opencontainers.image.base.name="$UBI_MICRO" \\
      org.opencontainers.image.description="$MC_DESC"
ENV MC_CONFIG_DIR=/tmp/.mc
COPY --from=certs /etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem /etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem
COPY --from=certs /etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem /etc/ssl/certs/ca-certificates.crt
COPY mc-\${TARGETARCH} /usr/bin/mc
COPY mc-CREDITS /licenses/CREDITS
COPY mc-LICENSE /licenses/LICENSE
ENTRYPOINT ["mc"]
EOF

# --- 5) Mimari başına imaj + manifest listesi --------------------------------------------------
build_and_manifest() {
  local name="$1" tag="$2" dockerfile="$3"
  local remote="$REGISTRY/$name:$tag"
  # Yerel manifest listesi AYRI bir ad altında kurulur: `$remote` adı yerel depoda upstream'den
  # çekilmiş bir imaja bağlı olabilir ve `manifest create` "name already in use" ile düşer.
  local list="localhost/$name-mirror:$tag"
  say "imaj: $remote  (yerel liste: $list)"
  podman manifest rm "$list" >/dev/null 2>&1 || true
  podman manifest create "$list" >/dev/null
  for arch in $ARCHES; do
    podman build --platform "linux/$arch" --build-arg "TARGETARCH=$arch" \
      -f "$dockerfile" -t "$list-linux-$arch" "$OUT"
    podman manifest add "$list" "containers-storage:$list-linux-$arch" >/dev/null
  done
  if [ "$PUSH" = "1" ]; then
    say "itme: $remote"
    podman manifest push --all "$list" "docker://$remote"
  else
    echo "    PUSH=0 -> itilmedi"
  fi
}

build_and_manifest minio "$MINIO_TAG" "$OUT/Dockerfile.minio"
build_and_manifest mc "$MC_TAG" "$OUT/Dockerfile.mc"

say "bitti"
echo "  $REGISTRY/minio:$MINIO_TAG  (commit $MINIO_COMMIT)"
echo "  $REGISTRY/mc:$MC_TAG  (commit $MC_COMMIT)"
