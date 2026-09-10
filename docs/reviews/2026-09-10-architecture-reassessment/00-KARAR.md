# Mimari Yeniden Değerlendirme — Karar Dokümanı

**Tarih:** 2026-09-10
**Tetikleyici:** OLake kod karşılaştırması → Silver CoW-MERGE yazma-amplifikasyonu → kullanıcı sorusu: *"overkill olduğumuz yerler var mı, patch'leyip patch'leyip 'eh işte' bir ürün mü yaptık, OSS ürünler konfigürasyonla çalışabilirken biz mi her şeyi kodladık? Sıfırdan mı başlamalıyız?"*
**Yöntem:** 7 bağımsız ajan, bizim spec/plan/comment'lerimizi OKUMADAN, birincil kaynaklara (kod, resmi doküman, GitHub issue/PR, ASF listeleri) karşı doğrulayarak. Raporlar bu dizinde `01..07-*.md`; her iddia VERIFIED / INFERENCE etiketli. Bu doküman yalnız sentez ve karardır; kanıt raporlardadır.
**Repo referansı:** `main@0cb3316`.

---

## 0. Tek cümlelik cevap

**Sıfırdan başlamıyoruz.** Yük taşıyan mimari (Debezium → Kafka → append-only Bronze → Spark MERGE → Silver, Nessie/Trino/dbt) beş bağımsız raporun her birinde alternatiflerinden daha iyi çıktı. Ama iki şey doğru: **(a)** kodun ~⅓'ü hiç yeniden ziyaret edilmemiş üç erken kararın tazminat katmanı, **(b)** test stratejimiz Python'un ne yazdığını ölçüyor, yazılanın çalışıp çalışmadığını değil — ve bunun kanıtı olan **canlı bir kusur** bulundu.

---

## 1. Sorulara cevaplar

### 1.1 "Her şeyi koda döktük mü, konfigürasyonla çözülür müydü?"

Kısmen evet. Rapor 07'nin envanteri (≈27k satır kod, ≈24k satır test):

| Bizim yazdığımız | Konfigürasyon karşılığı | Boyut |
|---|---|---|
| `_target_table` InsertField SMT + `iceberg.tables.dynamic-enabled` + route sahipliği mantığı + CR'dan ns/tablo geri-çözümleme | Per-tablo sink zaten tek topic→tek tablo: statik `iceberg.tables=<ns>_raw.<t>` | ~400 LOC + Bronze'a sızan `_target_table` kolonu |
| DirectoryConfigProvider + chart'ta sabit volume mount'ları + "LOAD-BEARING do not rename" başlığı | Strimzi `KubernetesSecretConfigProvider` (`${secrets:ns/name:key}`, mount yok, tek RBAC Role) | ~100 LOC + **canlı kusur (§2)** |
| pyiceberg ön-oluşturma alt sistemi: `iceberg_service.py` + `create_iceberg_table.py` + `images/iceberg-tools` + tablo başına 2×(ConfigMap+PreSync Job) + `merge_cdc` py4j identifier-reflection | MERGE anahtarı bir tablo özelliği (`app.merge-key`); Bronze partition sink'in `iceberg.tables.default-partition-by=day(__ts_ms)`; Silver'ı `merge_cdc` ilk çalışmada Spark DDL ile yaratır | ~1.100 LOC + ~650 test |
| Runtime-sahipli `connect` ACL'leri: seed Job + `ensure/remove_user_acl` + orchestrator acl/producer adımları | Per-source KafkaUser + `consumer.override.*`; ACL'ler chart'a geri döner | ~330 LOC |
| Saatte bir tablo başına `ALTER TABLE SET TBLPROPERTIES gc.enabled=true` | Oluşturmada bir kez | tablo×saat gereksiz commit |
| Trino/Superset/JupyterHub/Nessie/Grafana/Kafka-UI/Gitea/MinIO **elle şablonlanmış** Deployment'lar | Resmi/community Helm sub-chart'ları (`trinodb/charts`, `apache/superset`, `zero-to-jupyterhub`, `projectnessie`, …) | ~4.5k LOC şablon → ~2.65k silinebilir + ~155 render testi + `helm-check.py`'nin bir kısmı |
| `nginx_streaming.py` + `14-nginx-ingest.yaml` (tek topic için kalıcı Spark driver) | Shipper-tarafı parse+hash (Vector/FluentBit) + mevcut `stream/kafka` lane'i (ikinci Iceberg KC sink) — KVKK açısından da güçlü (ham IP Kafka'ya girmez) | ~300 LOC + kalıcı driver |
| İki yerde kopya SQL→Iceberg tip haritası; iki motorda namespace oluşturma; `tools/templates/*` (Console'un ürettiğinin elle kopyası); commit'lenmiş 188 KB `helm template` çıktısı | Tek modül; tek motor; sil | ~600 LOC + drift yüzeyi |

**Gerçekten gerekli olan ve konfigürasyona indirgenemeyen:** connector-config üreticisi (13 CR şekli, 485 anahtar, 7 SMT zinciri — Debezium UI 2025-09'da arşivlendi, Debezium Platform Strimzi Connect'i değil Debezium Server'ı yönetiyor), kaynak-DB başına tek connector + restoratif geri alma (R2), onaylı incremental-snapshot backfill, GitOps yazma yolu, domain modeli/validasyon. Bunlar "hot couture" değil; ürünün kendisi.

### 1.2 "Overkill olduğumuz yerler var mı?"

Evet, üç erken karar kodun yaklaşık üçte birini doğurmuş (Rapor 07 §I):

1. **MERGE anahtarını Iceberg identifier-field üzerinden taşımak** → pyiceberg ön-oluşturma, PreSync Job/CM, ayrı image, py4j reflection. Identifier field'ı CoW Spark MERGE de Trino da tüketmiyor.
2. **Paylaşımlı-sink döneminden kalan** dinamik routing + DirectoryConfigProvider mount sözleşmesi — sink'ler tablo-başına olduktan sonra gereksizleşti, kaldırılmadı; mount sözleşmesi per-source secret'larla **tutarsız** hâle geldi.
3. **Üçüncü-parti uygulamaları elle şablonlamak** — bu, 785 satırlık özel bir render linter'ını ve 392 render assertion'ının büyük kısmını da gerekçelendirdi.

Ek: per-pipeline S3 bucket'ları (namespace `location` aynı izolasyonu verir), iki notebook yığını (Zeppelin + JupyterHub), Kafka sinyal kanalı + notification tüketicisi (onay gerekli, ama Kafka *kanalı* topic/ACL/JAAS sürüklüyor; source-table kanalı zaten zorunlu).

### 1.3 "Patch'leyip patch'leyip 'eh işte' mi?"

Hayır ve evet. **Hayır:** yük taşıyan mimari tutarlı ve doğruluk düğümleri (tip modları, `publication.autocreate.mode=filtered`, dedup ORDER BY + `__deleted` CAST, restoratif rollback, byte[] Camel sink-side SMT) canlı olaylarla bedeli ödenmiş, korunmalı (Rapor 07 §G "must-not-touch"). **Evet:** üstünde kalın bir tazminat katmanı var ve test yığını (~%85 fake'e karşı render-shape assertion'ı; Console'un ürettiği connector CI'da hiç canlı Connect'e çarpmadı; E2E stage-2 manuel + el yazması fixture) tam da R1/R2/B3/R5'te canlıda bulunmak zorunda kalınan sınıfı **yapısal olarak göremiyor**. Bir sonraki sessiz-kayıp da CI'da değil canlıda bulunacak — §2 bunun bugünkü örneği.

### 1.4 "Sıfırdan başlayalım mı?"

**Hayır.** Beş rapor birbirinden bağımsız olarak aynı sonuca vardı:

| Alternatif omurga | Hüküm (rapor) |
|---|---|
| Debezium Server + memiiso Iceberg sink (Kafka'sız/Spark'sız) | Tek-maintainer, 2026-05'te sessiz veri-kaybı bugı dışarıdan düzeltildi, `decimal=double` varsayılanı (B4'ün aynısı), uyumsuz DDL tüm DB'yi durdurur, at-least-once, DLQ yok, HA yok; equality-delete → maliyet Trino'ya ve zorunlu compaction'a kayar. **Omurga değil; opsiyonel "lite" profil.** (01) |
| Flink CDC 3.x + Paimon / Flink→Iceberg | MongoDB pipeline kaynağı YOK, SQL Server yayınlanmamış, gömülü Debezium **1.9.8 (2022)**, Trino'da Paimon connector yok (46★ dış plugin), Paimon S3'te Nessie'siz lock ister; Flink→Iceberg equality-delete yazar ve **Iceberg V4 equality delete'i yasakladı (oy 2026-08-18)**. **İnandırıcı değil; 12–18 ay sonra 3 kapıyla tekrar bak.** (02) |
| Iceberg KC sink'te upsert / "Iceberg topics" | Sink **PMC kararıyla** append-only (Tabular delta writer'ları bağışta çıkarıldı; her yeniden-ekleme PR'ı reddedildi); Tableflow cloud-only, Redpanda enterprise+append-only, Bufstream satıldı, AutoMQ = Kafka'yı fork'la değiştir. **Spark MERGE'i kaldıran Kafka-native yol yok.** (03) |
| Trino MERGE / dbt-trino ile Spark'sız Silver | Trino MERGE yalnız MoR, CoW PR'ı 2023'ten beri açık, **#26853 "conflict corrupts table" AÇIK**, artımlı okuma yok (#8780), `table_changes` delete-file'lı tabloda çalışmaz, dbt-trino merge makrosunda DELETE dalı yok, Trino EXECUTE'ta `rewrite_position_delete_files` yok, dangling delete Trino 483'te hâlâ var. **Spark kalsın.** (06) |

Yeniden yazma, "must-not-touch" listesindeki bedeli ödenmiş doğruluğu çöpe atıp, alternatiflerin hiçbirinin daha iyi olmadığı bir omurgayı yeniden inşa etmek olurdu. Doğru program: **yeniden-ayar + sadeleştirme + test stratejisini değiştirme.**

---

## 2. 🔴 Canlı kusur (bu değerlendirmede bulundu, kodla doğrulandı)

`render_service._cred` (`render_service.py:92-98`) her kimlik bilgisini `${directory:/mnt/external-configuration/<source>:<key>}` olarak yazar. Chart yalnız sabit adları mount eder (`12-kafka-connect.yaml:148-186`: `mssql/pg/mongo/s3/debezium-src/kafka-ca/…`). Orchestrator `<source>` adlı bir Secret yaratır (`orchestrator.py:628`) ama **hiçbir kod KafkaConnect pod'una volume eklemez**. Ve `pg`/`mssql`/`mongo` adları kullanıcıya **yasak** (`models.py:15-19` `_RESERVED_SOURCE_NAMES`). Sonuç: **Console'dan eklenen her kimlik-bilgili kaynak, gerçek kümede çözümlenemeyen bir placeholder'la doğar** → connector başlangıçta FAILED (INFERENCE: DirectoryConfigProvider fırlatır; verify adımı rollback yapar). 721 yeşil test bunu göremez. `docs/POC-DOGRULA-cluster-checklist.md:177` sorunu zaten açık madde olarak listeliyor. Canlı-doğrulanan pipeline'lar el yazması fixture veya elle mount ile çalışmış olmalı.

**Düzeltme (config):** Strimzi `KubernetesSecretConfigProvider` — worker config'e bir satır, Secrets üzerine tek RBAC Role, `_cred` → `${secrets:<ns>/<source>:<key>}`, mount sözleşmesi ve başlık yorumu silinir.

---

## 3. Yazma rejimi — "4.8 TB/gün" meselesinin gerçek boyutu (Rapor 04)

- Doğrulanmış mekanik: CoW MERGE eşleşen satır içeren her veri dosyasını yeniden yazar; dokunulan oran `1 − e^(−N/F)`; N ≥ 3F'te tablonun tamamı. Apple VLDB'24 (kodun yazarları): *"impractical for sparse changes at scale"*. `bucket(16, pk)` yazma-amplifikasyonuna **hiçbir şey yapmaz** (join'i hızlandırır).
- Sayısal: bugünkü rejim ≈ **tablo boyutunun 96 katı/gün** (2 GB → 192 GB/gün; 20 GB → 1.9 TB/gün ve iş 15 dk'lık slotta sürekli çalışır; 200 GB → slota sığmaz, schedule çöker).
- **Asıl kaldıraç mod değil, katlama kadansı:** bakım işi `delete-file-threshold=1` ile *her saat* silme dosyalı her dosyayı yeniden yazıyor → MoR'a geçmek tek başına yalnız 4× kazandırır. MoR + 4–6 saatlik compaction ≈ 4–6×/gün (96 yerine). Ryan Blue: *"compaction once every 10 MERGE commits already reduces write amplification by about 10x."*
- Kendi kodumuz çelişiyor: `iceberg_maintenance.py:74-76` yorumu ve zinciri MoR varsayıyor, tablolar CoW yaratılıyor.
- Spark 3.5 + Iceberg 1.9 MoR **yalnız position delete** yazar, dosya granülaritesinde (kaynak-doğrulandı) → Trino-dostu. v3 deletion-vector'lar OSS Trino'da doğrulanmamış → v2'de kal.
- **Nessie tek branch çekişme sorunu DEĞİL** (B11 düşer): çakışma tespiti tablo-bazında, sunucu-tarafı CAS retry, ~333 commit/s tasarımına karşı bizde <10/s. Tablo-başına branch karmaşıklık ekler, hiçbir şey kazandırmaz.
- Sink 60 s commit = varsayılanın (300 s) 5 katı, Silver 15 dk'da yenilendiği için hiçbir şey kazandırmaz. `metadata.json` büyümesi orphan-cleanup sayesinde ~3 günle sınırlı (M2 "sonsuz" değil) ama `delete-after-commit=true` yine de açılmalı. `rewrite_manifests` krizi yok (M3 hafifledi).
- Amoro: doğru tasarım, incubating, kendi DB + optimizer filosu → **şimdi değil**, ölçek yol haritası.

**Hüküm (04):** *"acceptable default with wrong knob settings"* — ~20 GB'da görünür, ~200 GB'da ölümcül ölçek uçurumu; mimari değil ayar.

---

## 4. Katalog — Nessie yanlış bahis (Rapor 05)

- Dremio 2024-10-29 (yazılı): *"we will merge Nessie into Polaris… at which time Project Nessie will be retired."* Migratör 2025-06'da Polaris'e bağışlandı; Polaris 2026-02-19 ASF TLP oldu, duyuruda Nessie geçmiyor. Nessie 0.108.8 (2026-09-09), 0.x, son 6 ayda commit'lerin ~%93'ü iki kişiden, blog 2024-08'den beri sessiz, roadmap yok. **Bakılıyor, geliştirilmiyor.**
- Kaynak-doğrulanmış teknik bulgu: Nessie REST sunucusu `gc.enabled=false` enjekte eder **ve Iceberg metadata'sını tek snapshot'la üretir** (metadata-log/snapshot-log yazımı yorum satırında) → motor tarafı `expire_snapshots`/time-travel'ın silecek geçmişi yok; `remove_orphan_files` geçmişi Nessie commit-log'unda yaşayan dosyaları "sahipsiz" sayar. Bizim `gc.enabled=true` ALTER'ımız bu gizli semantiğin üstünde çalışıyor. Zaman-yolculuğu kullanmadığımız için felaket değil, ama anlamı gizli bir workaround.
- Tek ayırt edici özellik (branch/tag) bizde kullanılmıyor.
- **Hüküm:** GA öncesi **Apache Polaris**'e geç (ASF TLP, aylık sürüm, Postgres, Keycloak external-IdP, RBAC+OPA, multi-table commit); MinIO-STS'siz S3 için açık bug #3742 önce bizim depoda doğrulanmalı. Yedek: **Lakekeeper** (tek Rust binary, OpenShift-sertifikalı; tek-kişilik startup riski). İstemciler zaten REST → istemci tarafı config; geçiş authz/vending yeniden modellemesi yüzünden **2–4 haftalık proje**. Ne kadar geç, o kadar çok geçmiş çöpe (tek-snapshot metadata).

---

## 5. Karar tablosu — bileşen bazında

| Bileşen / karar | Hüküm | Gerekçe (rapor) |
|---|---|---|
| Debezium → Kafka → append-only Bronze → Spark MERGE → Silver (iki-hop) | **KEEP** | Alternatiflerin hepsi daha kötü (01/02/03/06); Iceberg PMC'nin "sink'te upsert yapma" kararıyla aynı hizada (03) |
| Kaynak-DB başına tek connector + restoratif undo + onaylı backfill (R2) | **KEEP** | must-not-touch (07) |
| Debezium tip modları, `publication.autocreate.mode=filtered`, dedup ORDER BY, `__deleted` CAST, byte[] Camel SMT, prebuilt image'lar | **KEEP** | bedeli ödenmiş doğruluk (07) |
| Spark (MERGE, şema-uzlaştırma, bakım, S3 batch) | **KEEP** | Trino/dbt yolu tutmuyor (06) |
| Silver `write.merge/update/delete.mode` | **RECONFIGURE** → merge-on-read (v2), küçük tablolarda CoW seçilebilir | 04 |
| Compaction | **RECONFIGURE** → `rewrite_data_files` 4–6 saatte bir, `delete-file-threshold` 5–10, `remove-dangling-deletes=true`, `partial-progress`; `rewrite_position_delete_files` saatlik; `expire_snapshots`+`remove_orphan_files` günlük; Bronze TTL günlük partition-hizalı; sıra düzelt | 04 |
| Sink commit aralığı | **RECONFIGURE** 60 s → 300 s; 7 sabit tuning değeri values'a | 03, 04, 07 |
| `write.metadata.delete-after-commit.enabled` / snapshot yaşı | **RECONFIGURE** → true; Bronze 1 gün, Silver 3–5 gün | 04 |
| Kimlik bilgisi taşıma (DirectoryConfigProvider + mount) | **REPLACE** → `KubernetesSecretConfigProvider` — **canlı kusuru kapatır** | 07, §2 |
| `_target_table` dinamik routing | **REPLACE** → statik `iceberg.tables`; SMT + Bronze kolon sızıntısı + geri-çözümleme kodu gider | 07 |
| pyiceberg ön-oluşturma alt sistemi | **REPLACE** → tablo özelliği anahtar + sink `default-partition-by` + Silver'ı merge ilk çalışmada Spark DDL ile; **önce canlı doğrula** ("Nessie REST set-identifier-fields 400" iddiası doğrulanmadı) | 07 (INFERENCE işaretli) |
| Runtime-sahipli `connect` ACL'leri | **REPLACE** → per-source consumer kimliği, ACL'ler chart'ta | 07 |
| nginx Spark-streaming lane | **REPLACE** → shipper parse/hash + ikinci Iceberg KC sink | 06, 07 |
| Per-pipeline S3 bucket'ları | **SIMPLIFY** → layer başına bucket + namespace `location` prefix (M15 de çözülür) | 07 |
| Elle şablonlanmış Trino/Superset/JupyterHub/Nessie/Grafana/Kafka-UI/Gitea/MinIO | **REPLACE** → sub-chart'lar, bileşen-bileşen (Trino/Superset en riskli: SCC, çift katalog, JWT sidecar) | 07 |
| Zeppelin + JupyterHub (iki notebook yığını) | **KARAR GEREKİR** → biri | 07 |
| Katalog: Nessie | **REPLACE** → Apache Polaris (yedek Lakekeeper), GA öncesi | 05 |
| Nessie tek branch (B11) | **KEEP / bulgu düşer** — çekişme yok | 04, 05 |
| Kafka sinyal kanalı + notification tüketicisi | **KEEP onayı, KANAL'ı yeniden değerlendir** (source-table kanalı zaten zorunlu) | 07 |
| Amoro, Iceberg v3 DV, Paimon, sink-upsert (PR #18003) | **YOL HARİTASI** — bugün değil; kapılar: OSS Trino DV doğrulaması, Amoro graduation, Trino'da Paimon connector, #18003 4/4 merge | 02, 03, 04 |
| DLQ beklentisi | **DÜZELT (bilgi)** — sink'te `ErrantRecordReporter` yok: DLQ yalnız converter/SMT hatası yakalar, writer hatası task'ı durdurur → stalled-task alert'i (R6/M7) şart | 03 |
| Test stratejisi | **DEĞİŞTİR** — Console-üretimi connector'ın CI'da canlı Connect'e çarptığı bir stage-2 (kind + Strimzi + gerçek Debezium + gerçek sink) zorunlu; render testleri regresyon için kalır ama "çalışır" kanıtı sayılmaz | 07 |

---

## 6. Denetim backlog'una düzeltmeler

- **B11** (Nessie tek-branch commit serileştirme) → **düşer**; gerçek sorun Nessie'nin proje ömrü + gc/tek-snapshot semantiği (§4).
- **M1/M2** → kök neden 60 s commit + `delete-file-threshold=1`; M2 "sonsuz" değil, ~3 gün sınırlı.
- **M3** → `rewrite_position_delete_files` VAR; eksik yalnız `rewrite_manifests` (Silver'da haftalık, opsiyonel).
- **M15** (bin bucket) → per-pipeline bucket sadeleştirmesiyle kendiliğinden çözülür.
- **M22** (bucket(16) size-unaware) → yazma-amplifikasyonu açısından alakasız; join için kalır.
- **YENİ 🔴 SEC/LIVE-1:** per-source credential placeholder çözümsüz (§2).
- **YENİ:** DLQ yalnız converter/SMT katmanını korur (03).

---

## 7. Önerilen program (sıra önerisi; kullanıcı onayı bekliyor)

Her dilim TDD + bağımsız review; her dilimin **canlı kanıtı** (kind/Podman + Strimzi) dev'de, UAT'ye bırakılmaz.

| # | Dilim | İçerik | Neden bu sırada |
|---|---|---|---|
| **P0** | Canlı kusur | `KubernetesSecretConfigProvider`; `_cred` değişimi; mount sözleşmesi silinir; **CI stage-2: Console-üretimi connector gerçek Connect'te RUNNING** | Ürünün ana akışı bugün canlıda çalışmıyor; ayrıca test stratejisi değişiminin ilk somut adımı |
| **P1** | Yazma rejimi | Silver MoR varsayılanı (+ mevcut tablolar ALTER), compaction kadansı/eşikleri/sırası, sink 300 s, `delete-after-commit`, snapshot yaşları, gc.enabled oluşturmada; yazma-amplifikasyonu **ölçümü** test ortamında (önce/sonra `$files`/`$snapshots`) | Ölçek uçurumu; tümü ayar; R6/R7'nin yarısını kapatır |
| **P2** | Routing + tuning sadeleştirme | Statik `iceberg.tables`, `_target_table` ve geri-çözümleme kodunun kaldırılması, sink tuning'in values'a alınması | Küçük, düşük risk, P0 ile aynı dosyalar |
| **P3** | Katalog kararı | Polaris'i bizim S3'te (MinIO/Ceph, STS'siz) spike ile doğrula (#3742); Lakekeeper yedek; geçiş planı + `nessie-gc` ara önlemi | GA öncesi; her ay gecikme = çöpe giden geçmiş |
| **P4** | Ön-oluşturma alt sistemini kaldırma | Önce spike: Spark DDL ile bucket/sort/identifier Nessie(veya Polaris) REST'te çalışıyor mu; sonra tablo-özelliği anahtar + sink partition + merge-first-run DDL; PreSync Job/CM, image, py4j silinir | En büyük LOC kazancı (~1.100 + 650 test), ama INFERENCE'lı → spike şart |
| **P5** | ACL + bucket + nginx sadeleştirmeleri | Per-source consumer kimliği; layer-bucket + prefix; nginx → shipper + KC sink; `tools/templates`, `manual-install` artefaktı, kopya tip haritası silinir | Bağımsız, orta |
| **P6** | Chart sub-chart'lama | Gitea/MinIO/Grafana/Kafka-UI (düşük risk) → Nessie/JupyterHub → Superset/Trino (yüksek risk; SCC/Route/tier sözleşmesi) | Bileşen-bileşen, her adım helm-unittest + kind'da render+çalışma |
| **R4** | Mongo raw-JSON Bronze | P1–P2 sonrası **yeniden karar**: Spark kalıyor → D (micro-batch) geçerli; ama P4 ile Bronze ön-oluşturma kalkarsa D'nin keşif mekanizması (tablo özelliği) yeniden şekillenir | Park edilen kararlar `architecture-reassessment` hafızasında |

**Kapsam dışı / bilinçli reddedilen:** yeniden yazma; Flink/Paimon; memiiso omurga; Trino/dbt Silver; AutoMQ/Redpanda; Amoro (şimdilik); tablo-başına Nessie branch.

---

## 8. Doğrulanmamış (INFERENCE) ve önce spike isteyenler

- Çözümsüz `${directory:…}` placeholder'ının tam runtime davranışı (FAILED mi, sessiz mi) — P0 canlı testinde görülür.
- "Nessie REST `SET IDENTIFIER FIELDS` 400 döner" (bizim docstring) vs "Spark DDL çalışır" (07) — P4 spike.
- OSS Trino v3 deletion-vector okuma/yazma durumu — yol haritası kapısı.
- Polaris #3742 (STS'siz S3) bizim depoda — P3 spike.
- Trino `EXECUTE expire/orphan`'ın Nessie `gc.enabled=false` altında davranışı — Trino'ya bakım taşınmayacağı için önemsiz.
- Rapor 04'ün sayısal modeli standart kombinatorik; satır/delete boyutları varsayım.
