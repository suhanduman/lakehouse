# Lakehouse v2 — F6 Cutover, Veri Metrikleri, Pre-ship Hazırlığı — Uygulama Planı

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `v2` dalını üretim dalı (`main`) yapmak, eski v1 `main`'i bundle yedeğiyle emekliye ayırmak; spec §8'in F6'ya bıraktığı tablo-düzeyi veri metriklerini teslim etmek ve OpenShift pre-ship kontrol listesini tek runbook'ta toplamak.

**Architecture:** Cutover, spec §11'in "force-push" adımı yerine **GitHub dal yeniden adlandırma** ile yapılır (`main`→`v1-legacy`, `v2`→`main`): geçmiş yeniden yazılmaz, varsayılan dal ve upstream'ler GitHub tarafından otomatik güncellenir, adım geri alınabilir; eski dalın silinmesi bundle doğrulandıktan sonra ayrı ve son adımdır. Veri metrikleri Prometheus exporter'sız, Trino Iceberg metadata tabloları (`"t$snapshots"`, `"t$files"`, `"t$history"`) üzerinde Superset sanal veri seti + dashboard **asset bundle**'ı olarak teslim edilir (ConfigMap ile pod'a iner, `superset import-dashboards` ile runbook adımında yüklenir — datasource import kalıbının aynısı). Kabul demosu (`~/Desktop/kc-kabul-demo`) v2 uyarlaması **F6 kapsamı dışıdır** (kullanıcı kararı 2026-09-18; kabul ortamı belli olunca ayrı plan).

**Tech Stack:** git/gh (GitHub REST `branches/{branch}/rename`), GitHub Actions e2e (`.github/workflows/e2e.yaml`), Helm glue chart + helm-unittest, Trino 483 Iceberg connector metadata tabloları, Superset 6.1.0-dev (`import-dashboards` CLI, export bundle YAML formatı), kind/Podman (lokal).

**Spec:** `docs/specs/2026-09-10-lakehouse-v2-design.md` — §8 (tablo düzeyi veri metrikleri "F6"), §11 (Geçiş/cutover), §13 (F6 tanımı), §14 (dbt runbook). Ek girdi: `docs/plans/2026-09-10-f0-findings.md` "F5 notu → Açık kalanlar (F6/pre-ship)" paragrafı.

## Global Constraints

- **Özel imaj YOK, hack YOK, runtime mutasyonu YOK**: her düzeltme values/şablon/script/doküman'a gider; `kubectl edit` yasak. Kurulumda her şey otomatik olmak zorunda değil → runbook adımı meşrudur (spec §1).
- **Secret'lar Git'e girmez**: dev'de yalnız `components.devSecrets=true` iken glue sentetik Secret üretir; prod Secret'ları runbook'la önceden yaratılır.
- **Sürümler sabit** (`runbooks/versions.md` otoriter): Trino 483 · Superset `apache/superset:6.1.0-dev` (her zaman `-dev` etiketi, kullanıcı kararı) · Spark 4.1.2 · Iceberg 1.11.0 · Strimzi 1.2.0 · Polaris chart 1.7.0 · kube-prometheus-stack 91.4.1 · Velero chart 12.1.0 · plugin-barman-cloud 0.8.0.
- **Commit trailer birebir:** `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`. Kabuk komutlarında literal `--verify` / `--no-verify` YAZILMAZ (hook engeller). Türkçe yorum/doküman.
- **Test kapısı:** `helm unittest glue` (şu an 122), `python3 -m pytest glue/jobs/tests -q` (17), `helm template glue -f platform/values/glue.yaml` ve `-f platform/values/glue-dev.yaml` temiz; e2e iddiaları yüksek sesli (`cmd || { echo "HATA …"; exit 1; }`), sessiz `&& echo OK` YOK. **Taze-küme otoriter kapı CI'dır** (ArgoCD modu, ubuntu-latest 4 vCPU/16 GB, `timeout-minutes: 90`); lokal kind (Podman VM 8 vCPU/14.9 GiB) bileşen-yolu doğrulaması içindir (F5 notu 25. satır: lokal tam koşu yük tepelerinde liveness zaman aşımıyla kapanmıyor).
- **Eski `main`'e hiçbir faz dokunmaz** (spec §13) — bu planda da eski `main` yalnız YENİDEN ADLANDIRILIR ve bundle sonrası SİLİNİR; içeriği değişmez.
- **Geçmiş yeniden yazma / silme komutları** ortamın Fact-Forcing gate'ine takılırsa adımlar kullanıcıya betik olarak verilir (spec §11.2, "Foundation Phase 6 emsali") — implementer bu komutları kendisi koşturmaz.

## Dosya haritası

| Dosya | Sorumluluk | Görev |
|---|---|---|
| `.github/workflows/e2e.yaml` | tetikleyici dallar: `[main, v2]` (T1) → `[main]` (T4) | T1, T4 |
| `README.md`, `runbooks/install.md`, `platform/root-app.yaml` (yorum) | dal adı `v2` → `main` (cutover sonrası geçerli metin), "Şu an: F6" | T1, T4 |
| `docs/specs/2026-09-10-lakehouse-v2-design.md` §11 | cutover yöntemi düzeltmesi (rename), §8 F6 teslimi notu | T1, T2 |
| `glue/templates/superset.yaml` | ikinci ConfigMap `superset-assets` (+ `/app/assets` mount) | T2 |
| `glue/files/superset/iceberg-metadata/**` | Superset export bundle (metadata/databases/datasets/charts/dashboards YAML) | T2 |
| `glue/tests/superset_test.yaml` | ConfigMap/mount iddiaları | T2 |
| `runbooks/data-metrics.md` | Trino metadata SQL'leri + dashboard import adımı | T2 |
| `test/e2e/trino-check/check.py`, `test/e2e/superset-path.sh` | `$snapshots` iddiası; asset dosyaları pod'da | T2 |
| `runbooks/preship-openshift.md` | OpenShift pre-ship kontrol listesi (tüm F1–F5 "canlı doğrulanacak" maddeleri) + PDB/MM2 kararları | T3 |
| `runbooks/versions.md` | PDB kararı satırı | T3 |
| `docs/plans/2026-09-10-f0-findings.md` | "F6 notu" | T4 |
| `~/lakehouse-v1-archive/lakehouse-v1-final-<tarih>.bundle` | eski `main` + tüm ref'ler (silme öncesi sigorta) | T4 |

---

### Task 1: Cutover öncesi repo hijyeni (CI tetikleyici, dal adı metinleri, spec §11 düzeltmesi)

**Files:**
- Modify: `.github/workflows/e2e.yaml:2-4`
- Modify: `README.md` (başlık + "Şu an" satırı), `runbooks/install.md:37-38`, `platform/root-app.yaml:10`, `bootstrap/bootstrap.sh` (REVISION varsayılanı varsa)
- Modify: `docs/specs/2026-09-10-lakehouse-v2-design.md` §11

**Interfaces:**
- Produces: workflow `on.push.branches: [main, v2]` (T4 `[main]`'e indirir); spec §11 yeni metni T4'ün adım sırasıdır.

- [ ] **Step 1: Mevcut dal referanslarını listele (kanıt)**

```bash
cd /Users/suhanduman/Desktop/KÇ
grep -rn -E "\bv2\b" README.md runbooks/install.md platform/root-app.yaml bootstrap/bootstrap.sh .github/workflows/e2e.yaml | grep -viE "lakehouse-v2|v2-design|v2'|v2\)"
grep -n "REVISION=" bootstrap/bootstrap.sh
```
Beklenen: workflow satır 3–4 (`[v2]`), root-app.yaml:10 ("bu repo@v2"), install.md `--revision <dal>` (dal adı geçmiyorsa dokunma), bootstrap.sh `REVISION="${REVISION:-...}"` varsayılanı (varsa not al).

- [ ] **Step 2: Workflow tetikleyicisini iki dala genişlet**

```yaml
on:
  push: {branches: [main, v2]}          # F6 cutover: rename sonrası yalnız main kalır (T4)
  pull_request: {branches: [main, v2]}
```
Doğrula: `python3 -c "import yaml,sys; d=yaml.safe_load(open('.github/workflows/e2e.yaml')); assert d[True]['push']['branches']==['main','v2'], d[True]; print('workflow OK')"` (PyYAML `on` anahtarını `True` okur — bilinen davranış).

- [ ] **Step 3: Metinleri cutover-sonrası hâline getir**

`README.md` başlığı `# lakehouse` (parantezli "(v2)" kalkar), "Şu an" satırı:
```markdown
- Şu an: **F6 cutover + veri metrikleri + pre-ship hazırlığı** (plan `docs/plans/2026-09-18-lakehouse-v2-f6-cutover.md`). F5 sonucu: izleme/DR/runbook'lar CI'da 10/10 e2e işaretiyle yeşil (F5 notu: `docs/plans/2026-09-10-f0-findings.md`).
```
`platform/root-app.yaml:10` yorumu: "bu repo@main (cutover öncesi `v2`)". `bootstrap.sh` REVISION varsayılanı `v2` ise **değiştirme** (T4'te dal adıyla birlikte değişir) — yalnız yorumla işaretle: `# F6 T4: cutover sonrası main`.

- [ ] **Step 4: Spec §11'i uygulanan yönteme göre düzelt**

§11.2'yi şu metinle değiştir (tek paragraf, geri kalan §11 aynı):
```markdown
2. e2e yeşil + runbook'lar tam → **dal yeniden adlandırma** (force-push yerine; geçmiş yazılmaz, geri alınabilir): GitHub REST `POST /repos/{owner}/{repo}/branches/main/rename {new_name: v1-legacy}` → `POST .../branches/v2/rename {new_name: main}` (varsayılan dal ve açık PR'lar GitHub tarafından taşınır), lokalde `git branch -m main v1-legacy && git branch -m v2 main && git fetch -p && git branch -u origin/main main`. **Silmeden önce** repo dışına `git bundle create ~/lakehouse-v1-archive/lakehouse-v1-final-<tarih>.bundle --all` alınır ve `git bundle verify` ile doğrulanır; sonra `v1-legacy` (GitHub + lokal) ve `archive/*` dalları silinir. Geçmiş-yeniden-yazma/silme komutları bu ortamda gate'e takılırsa adımlar kullanıcıya betik olarak verilir (Foundation Phase 6 emsali).
```

- [ ] **Step 5: Kapı + commit**

```bash
helm unittest glue | tail -3 && helm template glue -f platform/values/glue-dev.yaml >/dev/null && echo render-ok
git add .github/workflows/e2e.yaml README.md runbooks/install.md platform/root-app.yaml bootstrap/bootstrap.sh docs/specs/2026-09-10-lakehouse-v2-design.md
git commit -m "chore(f6): CI tetikleyici [main, v2], dal metinleri cutover-sonrası, spec §11 rename yöntemi

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```
Push YOK (T2 ile birlikte; CI koşusu başına ~55 dk).

---

### Task 2: Tablo düzeyi veri metrikleri — Trino Iceberg metadata + Superset dashboard asset bundle

**Files:**
- Create: `glue/files/superset/iceberg-metadata/metadata.yaml`, `databases/lakehouse.yaml`, `datasets/lakehouse/iceberg_table_health.yaml`, `charts/iceberg_table_health_table.yaml`, `dashboards/iceberg_metadata.yaml`
- Modify: `glue/templates/superset.yaml` (ConfigMap `superset-assets` + volume/mount `/app/assets`), `glue/tests/superset_test.yaml`
- Create: `runbooks/data-metrics.md`
- Modify: `test/e2e/trino-check/check.py` (bölüm 1'e `$snapshots` iddiası), `test/e2e/superset-path.sh` (asset dosyaları pod'da), `README.md` runbook listesi, spec §8 satır 183–185 ("F6" → teslim edildi + runbook adı)

**Interfaces:**
- Consumes: Superset CR `podTemplate` volume kalıbı (`glue/templates/superset.yaml:85-87`: `{name: datasources, configMap: {name: superset-datasources}}` → `/app/configs`); Trino datasource `database_name: lakehouse` (aynı dosya satır 10) — bundle'daki `databases/lakehouse.yaml` **aynı uuid'yi değil aynı adı** kullanır, import mevcut veritabanına adla bağlanır (Superset import "database_name" eşleşmesi).
- Produces: runbook adımı `superset import-dashboards -p /tmp/iceberg-metadata.zip -u <kullanıcı>`; e2e iddiası `OK snapshots shop.orders >= 2`.

- [ ] **Step 1: Trino metadata SQL'lerini canlı kümede doğrula (taze lokal kind, glue kurulu, pg-path bir kez koşmuş olmalı — yoksa `test/e2e/pg-path.sh`)**

```bash
cd /Users/suhanduman/Desktop/KÇ
kubectl -n lakehouse get svc trino && kubectl -n lakehouse get secret trino-tls -o jsonpath='{.data.ca\.crt}' | base64 -d > /tmp/trino-ca.crt
kubectl -n lakehouse port-forward svc/trino 8443:8443 >/dev/null 2>&1 & PF=$!; trap 'kill $PF' EXIT; sleep 3
# e2e servis hesabı (test/e2e/trino-check/job.yaml'daki TRINO_USER/PASS Secret'ından); Basic auth
curl -s --cacert /tmp/trino-ca.crt -u "e2e:$(kubectl -n lakehouse get secret trino-e2e -o jsonpath='{.data.password}' | base64 -d)" \
  -H 'X-Trino-Catalog: lakehouse' -H 'X-Trino-Schema: shop' -X POST https://localhost:8443/v1/statement \
  --data 'SELECT committed_at, snapshot_id, operation, summary FROM "orders$snapshots" ORDER BY committed_at DESC LIMIT 5' | python3 -c "import json,sys; print(json.load(sys.stdin).get('nextUri'))"
```
(Secret adları `test/e2e/trino-check/job.yaml`'dan okunur — buradaki `trino-e2e` tahmindir, gerçeğini job.yaml'dan al.) Beklenen: nextUri döner; `trino` python istemcisiyle tam sonuç için `kubectl run`/`run_check_job` kalıbı da olur. Not al: `summary` MAP(varchar,varchar) — dashboard'da `summary['total-records']`, `summary['total-data-files']`, `summary['total-files-size']` kullanılır.

- [ ] **Step 2: Sanal veri seti SQL'i (bundle'ın çekirdeği)**

`datasets/lakehouse/iceberg_table_health.yaml` içindeki `sql:` alanı (Trino, katalog `lakehouse`):
```sql
WITH t AS (
  SELECT 'shop.orders' AS tbl, * FROM lakehouse.shop."orders$snapshots"
  UNION ALL SELECT 'crm.customers', * FROM lakehouse.crm."customers$snapshots"
  UNION ALL SELECT 'nginx_raw.access_log', * FROM lakehouse.nginx_raw."access_log$snapshots"
)
SELECT tbl,
       max(committed_at)                                                        AS son_commit,
       count(*)                                                                 AS snapshot_sayisi,
       max_by(CAST(summary['total-records'] AS bigint), committed_at)           AS kayit,
       max_by(CAST(summary['total-data-files'] AS bigint), committed_at)        AS veri_dosyasi,
       max_by(CAST(summary['total-delete-files'] AS bigint), committed_at)      AS delete_dosyasi,
       max_by(CAST(summary['total-files-size'] AS bigint), committed_at)/1048576.0 AS boyut_mib
FROM t GROUP BY tbl ORDER BY tbl
```
Tablo listesi dev demo tablolarıdır (Silver `shop.orders`, `crm.customers`; ham `nginx_raw.access_log`); runbook "yeni tablo ekleme" başlığı `UNION ALL` satırı eklemeyi anlatır. Doğrula: Step 1'deki yolla bu SQL'i koş — 3 satır.

- [ ] **Step 3: Bundle'ı canlı Superset'ten ÜRET (elle YAML yazma)**

Elle yazılmış export YAML'ı Superset'in beklediği uuid/sürüm alanlarında kırılır; bu yüzden bundle **canlı üretilir**: (a) `runbooks/user-facing.md` adımıyla datasource import (zaten e2e yapıyor); (b) Superset REST ile veri seti + chart + dashboard yarat, (c) `superset export-dashboards` ile bundle'ı al. Superset pod'unda (`kubectl -n lakehouse exec deploy/superset-web-server -- …`):
```bash
superset fab create-user --role Admin --username bundle-admin --firstname b --lastname a --email b@x --password "$(openssl rand -hex 12)" 2>/dev/null || true   # yalnız üretim adımı için; sonra silinir
# REST: token
TOK=$(curl -s -X POST localhost:8088/api/v1/security/login -H 'Content-Type: application/json' -d '{"username":"bundle-admin","password":"<parola>","provider":"db"}' | python3 -c 'import json,sys;print(json.load(sys.stdin)["access_token"])')
DB=$(curl -s localhost:8088/api/v1/database/ -H "Authorization: Bearer $TOK" | python3 -c 'import json,sys;print([d["id"] for d in json.load(sys.stdin)["result"] if d["database_name"]=="lakehouse"][0])')
# veri seti (sanal, Step 2 SQL'i dosyadan)
python3 - <<'PY'
import json,os,urllib.request
tok=os.environ["TOK"]; sql=open("/tmp/iceberg_table_health.sql").read()
def call(path,body,method="POST"):
    r=urllib.request.Request("http://localhost:8088/api/v1/"+path,data=json.dumps(body).encode(),method=method,headers={"Authorization":"Bearer "+tok,"Content-Type":"application/json"})
    return json.load(urllib.request.urlopen(r))
ds=call("dataset/",{"database":int(os.environ["DB"]),"schema":"shop","table_name":"iceberg_table_health","sql":sql})["id"]
db=call("dashboard/",{"dashboard_title":"Iceberg metadata","slug":"iceberg-metadata","published":True})["id"]
ch=call("chart/",{"slice_name":"Iceberg tablo sağlığı","viz_type":"table","datasource_id":ds,"datasource_type":"table","dashboards":[db],
  "params":json.dumps({"viz_type":"table","query_mode":"raw","all_columns":["tbl","son_commit","snapshot_sayisi","kayit","veri_dosyasi","delete_dosyasi","boyut_mib"],"row_limit":100})})["id"]
print(ds,ch,db)
PY
superset export-dashboards -f /tmp/iceberg-metadata.zip
```
(`chart` POST'undaki `dashboards: [id]` alanı chart'ı dashboard'a bağlar — 6.1 API'de CANLI DOĞRULA; kabul etmezse `PUT /api/v1/chart/{ch} {"dashboards":[db]}`.) Zip'i lokale kopyala (`kubectl cp`), aç, `glue/files/superset/iceberg-metadata/` altına **yalnız** `metadata.yaml`, `databases/lakehouse.yaml`, `datasets/lakehouse/iceberg_table_health.yaml`, `charts/*.yaml`, `dashboards/*.yaml` dosyalarını koy. `databases/lakehouse.yaml` içinde **parola/sqlalchemy_uri sırrı olmadığını** doğrula (`grep -i -E "password|secret" -r glue/files/superset/` boş; uri `trino://superset@…` parolasız — `SQLALCHEMY_CUSTOM_PASSWORD_STORE`). `bundle-admin` kullanıcısını sil (`superset fab delete-user --username bundle-admin`) — bu üretim adımı kümeye kalıcı iz bırakmaz; kullanıcı e2e/lokal kümede yaratılıp silinir, ürün şablonu değişmez.

- [ ] **Step 4: Şablon: ConfigMap + mount (unittest önce)**

`glue/tests/superset_test.yaml`'a:
```yaml
  - it: superset-assets ConfigMap iceberg-metadata bundle dosyalarını taşır ve /app/assets'e mount edilir
    set: {superset.enabled: true}
    template: templates/superset.yaml
    asserts:
      - documentSelector: {path: metadata.name, value: superset-assets}
        isKind: {of: ConfigMap}
      - documentSelector: {path: metadata.name, value: superset-assets}
        exists: {path: data["iceberg-metadata__metadata.yaml"]}
      - documentSelector: {path: metadata.name, value: superset}
        contains:
          path: spec.podTemplate.volumes
          content: {name: assets, configMap: {name: superset-assets}}
```
Koş: `helm unittest glue -f 'tests/superset_test.yaml'` → yeni test FAIL. Şablon (`glue/templates/superset.yaml`, datasources ConfigMap'inin hemen altına):
```yaml
---
# Superset asset bundle (dashboard export formatı). ConfigMap anahtarı alt dizin taşıyamaz → "dizin__dosya" adı;
# runbooks/data-metrics.md import adımı adları geri açar. Sır YOK (databases/lakehouse.yaml parolasız).
apiVersion: v1
kind: ConfigMap
metadata: {name: superset-assets, namespace: {{ $ns }}}
data:
{{- range $path, $_ := .Files.Glob "files/superset/iceberg-metadata/**.yaml" }}
  {{ $path | trimPrefix "files/superset/" | replace "/" "__" }}: |
{{ $.Files.Get $path | indent 4 }}
{{- end }}
```
Volume/mount: `{name: assets, mountPath: /app/assets, readOnly: true}` + `{name: assets, configMap: {name: superset-assets}}` (datasources kalıbının yanına). Koş → PASS; `helm template glue -f platform/values/glue-dev.yaml | grep -c "iceberg-metadata__"` ≥ 5.

- [ ] **Step 5: Runbook `runbooks/data-metrics.md`**

Bölümler: (1) Amaç (spec §8: exporter yok; Trino metadata); (2) Trino SQL'leri — `$snapshots`, `$files` (dosya sayısı/boyut/format), `$history` (rollback izleri), Step 2 sanal SQL'i; (3) Superset dashboard import adımı:
```bash
kubectl -n lakehouse exec deploy/superset-web-server -- python3 - <<'PY'
import os,shutil,pathlib
src=pathlib.Path('/app/assets'); dst=pathlib.Path('/tmp/iceberg-metadata'); shutil.rmtree(dst, ignore_errors=True)
for f in src.glob('iceberg-metadata__*'):
    p=dst/f.name.split('__',1)[1].replace('__','/'); p.parent.mkdir(parents=True, exist_ok=True); p.write_text(f.read_text())
shutil.make_archive('/tmp/iceberg-metadata','zip','/tmp','iceberg-metadata'); print('zip hazır')
PY
kubectl -n lakehouse exec deploy/superset-web-server -- superset import-dashboards -p /tmp/iceberg-metadata.zip -u admin1
```
`-u`: dashboard'ların sahibi olacak MEVCUT kullanıcı — Keycloak ile bir kez giriş yapmış yönetici (`admin1` dev). `import-dashboards --help` ile bayrağı **canlı doğrula**; `-u` yoksa kaldır. (4) Yeni tablo ekleme (UNION ALL satırı + yeniden import, `--overwrite`/`-o` varsa). (5) Sınırlar: Silver tablo adları values'a bağlı; FlashBlade/bucket metrikleri platformun.

- [ ] **Step 6: e2e iddiaları**

`test/e2e/trino-check/check.py` bölüm (1) (servis hesabı) sonuna:
```python
snaps = q(c, 'SELECT count(*) FROM shop."orders$snapshots"')[0][0]
assert snaps >= 2, f"orders$snapshots {snaps} < 2 (MERGE #1/#2 sonrası en az 2 snapshot beklenir)"
print(f"OK snapshots shop.orders {snaps} >= 2")
```
`test/e2e/superset-path.sh` sonuna (mevcut kalıp, yüksek sesli):
```bash
echo "== asset bundle pod'da"
kubectl -n lakehouse exec deploy/superset-web-server -- sh -c 'ls /app/assets | grep -c "^iceberg-metadata__"' | grep -qE '^[5-9]|^[1-9][0-9]' \
  || { echo "HATA: /app/assets içinde iceberg-metadata__* dosyaları yok"; kubectl -n lakehouse exec deploy/superset-web-server -- ls /app/assets; exit 1; }
echo "OK superset assets (iceberg-metadata bundle mount)"
```
Deployment adı `superset-web-server` — `kubectl -n lakehouse get deploy` ile doğrula (operator adı `<cr>-web-server`). Lokalde koş: `test/e2e/trino-path.sh` ve `test/e2e/superset-path.sh` → `E2E F4 TRINO OK`, `E2E F4 SUPERSET OK`. Dashboard import adımını lokalde bir kez uygula (admin1 için önce Keycloak girişi gerekir → e2e'de yapılmaz; lokalde `bundle-admin` benzeri geçici kullanıcıyla `-u` doğrulanır) ve `curl …/api/v1/dashboard/?q=(filters:!((col:slug,opr:eq,value:iceberg-metadata)))` ile varlığını göster; rapora yaz.

- [ ] **Step 7: Docs + commit + push (T1 ile birlikte tek CI koşusu)**

spec §8 satır 183–185: "istenirse bir Superset dashboard'una bağlanır (F6)" → "Superset dashboard asset bundle'ı `glue/files/superset/iceberg-metadata/` + `runbooks/data-metrics.md` (F6'da teslim)". README runbook listesine `veri metrikleri runbooks/data-metrics.md`.
```bash
helm unittest glue | tail -3; python3 -m pytest glue/jobs/tests -q | tail -1
git add glue/files/superset glue/templates/superset.yaml glue/tests/superset_test.yaml runbooks/data-metrics.md test/e2e/trino-check/check.py test/e2e/superset-path.sh README.md docs/specs/2026-09-10-lakehouse-v2-design.md
git commit -m "feat(f6): Iceberg tablo-düzeyi veri metrikleri — Trino metadata SQL + Superset dashboard bundle (ConfigMap) + e2e iddiaları

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
git push origin v2
```
CI'ı izle (`gh run list --branch v2 --limit 1 --json databaseId,status,conclusion`; ≤10 dk'lık `sleep 540` döngüsü, boşta tur bitirme yok). Beklenen: 10/10 işaret + `OK snapshots shop.orders`.

---

### Task 3: OpenShift pre-ship kontrol listesi + PDB/MM2 kararları

**Files:**
- Create: `runbooks/preship-openshift.md`
- Modify: `runbooks/versions.md` (PDB kararı), `runbooks/dr.md` (Kafka verisi/MM2 kararı bir cümle), `README.md` (runbook listesi)

**Interfaces:**
- Consumes: F1–F5 notlarındaki "pre-ship/OpenShift'te doğrulanacak" maddeleri (`docs/plans/2026-09-10-f0-findings.md` F1 notu son paragraf, F2/F3 "Açık/ertelenen", F4 kapanış notları, F5 "Açık kalanlar"); hafıza `openshift-preship-env`.
- Produces: tek kontrol listesi; her madde `[ ]` + doğrulama komutu + beklenen çıktı + ilgili runbook bağlantısı.

- [ ] **Step 1: Maddeleri topla (grep, kanıt)**

```bash
cd /Users/suhanduman/Desktop/KÇ
grep -n -iE "pre-ship|openshift'te|OpenShift’te|canlı doğrulan|UWM|OADP|Route|LDAP group|LokiStack|OVN|MSSQL|MirrorMaker|PDB|ayna" docs/plans/2026-09-10-f0-findings.md | cut -c1-160
```

- [ ] **Step 2: `runbooks/preship-openshift.md` yaz**

Bölümler (her satır: madde · komut · beklenen · kaynak not):
1. **Kimlik/ağ:** tarayıcı OIDC (Trino UI, Superset, JupyterHub) · Route `reencrypt` + `tls.caBundle` · hub→Keycloak router sertifikası · LDAP group provider (`platform/values/trino-ldap.yaml`) · Keycloak LDAP `bindCredential` Secret · sertifika yenileme (cert-manager `Certificate` renewal → Trino restart davranışı) · apiserver→webhook NetworkPolicy OVN'de (`kubectl -n lakehouse get validatingwebhookconfiguration` + bir SparkApplication apply'ı; `allow-apiserver` kuralı gerekirse `glue/templates/networkpolicy.yaml`) · Kafka dış listener Route.
2. **İzleme/log:** UWM `enableUserWorkload: true` (`kubectl -n openshift-monitoring get cm cluster-monitoring-config -o yaml`), PrometheusRule/PodMonitor/ServiceMonitor'lerin UWM Prometheus'ta hedef olması (Polaris dâhil), Alertmanager yönlendirme (platform), LokiStack/ClusterLogForwarder sorgu örnekleri (`troubleshooting.md#loki`).
3. **DR:** OADP `DataProtectionApplication` alan adları (`kubectl explain dpa.spec`), `velero.namespace: openshift-adp`, CSI snapshot + Data Mover ile PVC içeriği, CNPG Barman restore provası prod bucket'ında, `acceptance.sh --mon-ns … --velero-ns openshift-adp` uçtan uca (hiç koşmadı — F5 notu).
4. **Kaynaklar:** MSSQL canlı (`database.encrypt=true`), Connect `buildImage` iç registry + `connect-push` Secret, Superset imajı ayna (`6.1.0-dev` etiketi taşınır; operator digest almaz), Maven Central egress ya da `spark.ivySettingsXml` aynası.
5. **Kararlar (bu görevde alınır, gerekçeli):** **PDB yaratılmaz** — CNPG (instances 2) ve Strimzi (replicas 3) kendi PDB'lerini yönetir; Trino coordinator/Connect/Polaris/Superset/hub tek replika → PDB drain'i kilitler; HA ihtiyacı çıkarsa replika artırımıyla birlikte ele alınır. **Kafka verisi DR kapsamı dışı** — yeniden akıtma (Debezium snapshot) yolu; MM2 yalnız çoklu-site isterse (`dr.md`'ye bir cümle). **Superset migrate DB kapısı** `maxRetries: 120` ile kalır (operator lifecycle Job'u Cluster'a bağlanamaz; not).
6. **Kabul:** `runbooks/acceptance-tests.md`; kabul demosu v2 uyarlaması (`~/Desktop/kc-kabul-demo`, v1 formatında) ayrı plan — F6 dışı.

- [ ] **Step 3: Kapı + commit**

```bash
git add runbooks/preship-openshift.md runbooks/versions.md runbooks/dr.md README.md
git commit -m "docs(f6): OpenShift pre-ship kontrol listesi; PDB yok / Kafka DR yeniden akıtma kararları

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```
Push T4 ile.

---

### Task 4: Cutover — bundle, dal yeniden adlandırma, CI main'de yeşil, eski dalların silinmesi (kullanıcı betiği), F6 notu

**Files:**
- Create: `~/lakehouse-v1-archive/lakehouse-v1-final-<tarih>.bundle`
- Modify: `.github/workflows/e2e.yaml` (`[main]`), `README.md` ("Şu an: teslim/kabul"), `platform/root-app.yaml` yorum, `bootstrap/bootstrap.sh` REVISION varsayılanı (`v2`→`main`, varsa), `runbooks/install.md`
- Modify: `docs/plans/2026-09-10-f0-findings.md` ("F6 notu")
- Kullanıcıya teslim (repoya girmez): eski dal silme betiği — yalnız rapor/yanıt metninde

**Interfaces:**
- Consumes: T1 workflow `[main, v2]`; T2/T3 push'ları CI'da yeşil (ön koşul: `origin/v2` HEAD'inde son CI koşusu `success`).
- Produces: `origin/main` == eski `origin/v2` HEAD; varsayılan dal `main`; CI run `main`'de yeşil.

- [ ] **Step 1: Ön koşullar (kanıt)**

```bash
cd /Users/suhanduman/Desktop/KÇ && git fetch -p && git status -sb | head -1        # ## v2...origin/v2 (ahead 0)
gh run list --branch v2 --limit 1 --json headSha,conclusion,databaseId --jq '.[0]'   # conclusion success, headSha == git rev-parse HEAD
gh pr list --state open --json number --jq length                                    # 0
gh api repos/{owner}/{repo}/branches/main/protection 2>&1 | grep -c "Branch not protected"   # 1
```

- [ ] **Step 2: Bundle (silme öncesi sigorta) + doğrulama**

```bash
D=$(date +%F); git bundle create ~/lakehouse-v1-archive/lakehouse-v1-final-$D.bundle --all && git bundle verify ~/lakehouse-v1-archive/lakehouse-v1-final-$D.bundle && git bundle list-heads ~/lakehouse-v1-archive/lakehouse-v1-final-$D.bundle | grep -E "refs/heads/(main|v2|archive)"
```
Beklenen: `main` 0cb3316 (lokal, 51 commit origin'in önünde — hepsi bundle'da), `v2` HEAD, `archive/per-pipeline-buckets-v1-broken`.

- [ ] **Step 3: GitHub'da yeniden adlandırma (geri alınabilir; gate'e takılırsa kullanıcı betiği)**

```bash
gh api -X POST repos/{owner}/{repo}/branches/main/rename -f new_name=v1-legacy --jq .name     # v1-legacy
gh api -X POST repos/{owner}/{repo}/branches/v2/rename   -f new_name=main      --jq .name     # main
gh repo view --json defaultBranchRef --jq .defaultBranchRef.name                              # main
git fetch -p && git branch -m main v1-legacy && git branch -m v2 main && git branch -u origin/main main && git status -sb | head -1   # ## main...origin/main
```
Geri alma: aynı iki `rename` çağrısı ters yönde.

- [ ] **Step 4: Dal adı metinleri + workflow `[main]` + push → CI main'de**

`.github/workflows/e2e.yaml` `branches: [main]` (push ve pull_request); `bootstrap.sh` REVISION varsayılanı `main`; `root-app.yaml` yorumu "bu repo@main"; `runbooks/install.md` `--revision main` örneği; README "Şu an: **F6 tamam — kabul/pre-ship** (`runbooks/preship-openshift.md`)".
```bash
git add -A && git commit -m "chore(f6): cutover — CI yalnız main, dal adı metinleri main

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>" && git push origin main
gh run list --branch main --limit 1 --json databaseId,status --jq '.[0]'   # sonra sleep 540 döngüsüyle conclusion success bekle
```

- [ ] **Step 5: Eski dalların silinmesi — kullanıcıya betik (implementer KOŞTURMAZ)**

```bash
# ~/lakehouse-v1-archive/*.bundle doğrulandıktan sonra, kullanıcı koşturur:
git push origin --delete v1-legacy
git branch -D v1-legacy archive/per-pipeline-buckets-v1-broken
git remote prune origin
gh api repos/{owner}/{repo}/branches --jq '.[].name'     # beklenen: yalnız main
```
Not: `origin/main` eski ucu (8dd8f02) ve lokal 0cb3316 bundle'da; `git clone ~/lakehouse-v1-archive/lakehouse-v1-final-<tarih>.bundle -b main` ile geri gelir (bundle'da eski dal hâlâ `refs/heads/main` adıyla — `list-heads` ile bak).

- [ ] **Step 6: F6 notu**

`docs/plans/2026-09-10-f0-findings.md` sonuna `## F6 notu (<tarih>)`: cutover yöntemi (rename, gerekçe), bundle adı/heads, CI main run id + süre, veri metrikleri dashboard (uid/slug, import `-u` bayrağı gerçeği), pre-ship listesi madde sayısı, kararlar (PDB yok, MM2 yok), açık kalanlar (yalnız OpenShift'te yapılabilecekler). Commit `docs(f6): F6 notu` (trailer ile) + push main; CI tekrar koşar (salt-doküman; kod kapısı Step 4'teki run'dır).

---

## Self-review (plan yazarı)

- **Spec kapsamı:** §11 adımları 1–4 → T1 (metin), T4 (bundle, rename, silme betiği, docs); §13 F6 tanımı (cutover + bundle + eski dallar) → T4; §8 "F6" veri metrikleri → T2; F5 açık kalanlar → T3 (liste + PDB/MM2 kararları; canlı OpenShift maddeleri bilerek liste olarak kalır — ortam yok); kabul demosu v2 uyarlaması kullanıcı kararıyla F6 dışı (ayrı plan). Gap: yok. `docs/reviews/...` korunur (T4 hiçbir docs silmez).
- **Placeholder taraması:** T2 Step 3'te Superset REST'in chart↔dashboard bağlama alanı ve `import-dashboards -u` bayrağı "CANLI DOĞRULA" olarak işaretli — bilinçli doğrulama noktaları, kod dolduran değil; T2 Step 1 Secret adı için gerçek kaynak (`job.yaml`) gösterildi.
- **Ad tutarlılığı:** ConfigMap `superset-assets` / mount `/app/assets` / anahtar `iceberg-metadata__…` (T2 şablon ↔ unittest ↔ runbook ↔ e2e); dal adları `v1-legacy`/`main` (T1 spec metni ↔ T4 komutları); bundle yolu `~/lakehouse-v1-archive/lakehouse-v1-final-<tarih>.bundle` (T1 spec ↔ T4).
- **Lokal ortam:** T2 canlı adımları taze lokal kind kümesini kullanır (2026-09-18 ~22:30'da yeniden kuruluyor: `kind.sh` + `bootstrap.sh --env dev --mode helm`, log `/tmp/f6-bootstrap.log`); `polaris-setup.sh` + `pg-path.sh` T2 Step 1'in ön koşuludur (ilk implementer koşturur; `run.sh`'ın bootstrap-sonrası adımları). Tam e2e lokalde beklenmez; CI kapıdır.
