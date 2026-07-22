#!/usr/bin/env bash
# SCM-agnostik CI doğrulaması — Console'un pipeline repo'suna açtığı PR'lardaki
# manifestleri, MERGE'DEN ÖNCE doğrular. SCM/CI kararı henüz belirsiz olduğundan
# mantık burada; GitLab CI (.gitlab-ci.yml) / Tekton (Task) / GitHub Actions
# yalnızca bu script'i sarmalar. iceberg-tools imajı içinde çalıştırın
# (pyiceberg + pyyaml + create_iceberg_table.py hazır):
#   images/iceberg-tools/Dockerfile
#
# Adımlar:
#   1) yamllint (varsa)
#   2) her descriptor ConfigMap için create_iceberg_table.py --dry-run
#      → şema + identifier→kolon eşlemesini Nessie/S3'e DOKUNMADAN doğrular
#        (identifier kolonu şemada yoksa / boşsa dry-run patlar → CI kırmızı).
#
# Kullanım:  gitops/ci/validate-pipeline.sh <pipeline-manifest-dizini>
set -euo pipefail
DIR="${1:-.}"
SCRIPT="${CREATE_ICEBERG_TABLE:-/opt/tools/create_iceberg_table.py}"
[ -f "$SCRIPT" ] || SCRIPT="tools/create_iceberg_table.py"  # repo checkout fallback

echo ">> [1/2] yamllint"
if command -v yamllint >/dev/null 2>&1; then
  yamllint "$DIR"
else
  echo "   (yamllint bulunamadı — atlandı; CI imajına eklenmesi önerilir)"
fi

echo ">> [2/2] descriptor dry-run (identifier + tip eşleme)"
python3 - "$DIR" "$SCRIPT" <<'PY'
import sys, glob, os, subprocess, tempfile
import yaml
directory, script = sys.argv[1], sys.argv[2]
count = 0
for f in sorted(glob.glob(os.path.join(directory, "**", "*.yaml"), recursive=True)):
    with open(f) as fh:
        for doc in yaml.safe_load_all(fh):
            if not doc or doc.get("kind") != "ConfigMap":
                continue
            desc = (doc.get("data") or {}).get("descriptor.yaml")
            if not desc:
                continue
            count += 1
            name = doc.get("metadata", {}).get("name", "?")
            print(f"   -- {name}  ({f})")
            with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False) as t:
                t.write(desc)
                path = t.name
            try:
                rc = subprocess.run(
                    [sys.executable, script, "--descriptor", path, "--dry-run"]
                ).returncode
            finally:
                os.unlink(path)
            if rc != 0:
                sys.exit(f"FAIL: descriptor dry-run başarısız → {name} ({f})")
print(f">> OK — {count} descriptor doğrulandı" if count else ">> (descriptor ConfigMap yok)")
PY
echo ">> DOĞRULAMA GEÇTİ"
