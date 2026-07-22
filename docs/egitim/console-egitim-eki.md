# Self-Service Console — Eğitim Eki

**Kapsam:** `console/backend` (FastAPI) + `console/frontend` (React/Vite) +
gömülü Kafka UI (`kafbat-ui`). Bu doküman `docs/egitim/ingest-omurgasi-egitim.md`
("İngest Omurgası — Eğitmen & Hands-On Kılavuzu") dokümanının bir **eki**dir —
oradaki "5. Yeni kaynak ekleme atölyesi" bölümünün **CLI'dan GUI'ye taşınmış
karşılığını** ve Console'a özgü rol/silme-güvenliği + deep-dive UI konularını
kapsar. Runbook detayları: `tools/README.md` ("Self-Service Console"
bölümü). Spec: `docs/superpowers/specs/2026-07-17-self-service-console-design.md`
(özellikle §4 "Yeni kaynak ekle akışı", §5 "CRUD & silme semantiği", §6 "İzleme").

Namespace tüm örneklerde `example`. Console URL'i `https://console.{{CORP_DOMAIN}}`,
Kafka UI `https://kafka-ui.{{CORP_DOMAIN}}` (her ikisi de Keycloak OIDC arkasında —
bkz. `chart/templates/10-keycloak.yaml` + `chart/templates/console/routes-oidc.yaml`;
Helm-öncesi hali arşivde `legacy-manifests/10-keycloak.yaml` +
`legacy-manifests/17-console-routes-oidc.yaml`).

---

## 1. Neden bu ek gerekli — CLI'dan GUI'ye

İngest omurgası eğitiminin "5. Yeni kaynak ekleme atölyesi" bölümü katılımcıya
`templates/scaffold-source.sh --dry-run` ile önizleme, sonra gerçek çalıştırma
öğretiyordu — terminal + `oc apply` + elle namespace DDL kopyalama gerektiren
bir akıştı. **Console bu akışın GUI karşılığıdır**: aynı girdiler (kaynak adı,
kind/type, tablo, hedef ns/tablo, incrementing/timestamp kolonları), aynı çıktı
(bucket + `KafkaTopic` + `KafkaConnector` + Iceberg namespace) — ama terminale
hiç dokunmadan, tarayıcıdan. Script hâlâ vardır ve geçerlidir (özellikle toplu/
otomasyon senaryolarında); Console, **aynı iş için ikinci, self-servis bir yol**
sunar; script'in yerini almaz.

**Önemli mimari fark:** `scaffold-source.sh` ürettiği YAML'ı `oc apply -f -`
ile uygular ama katılımcı isterse çıktıyı Git'e commit edip PR açabilir.
Console'un backend'i ise CR'ları **doğrudan Kubernetes API'sine** yazar
(`lakehouse-console` ServiceAccount ile) — Git'e hiçbir şey commit edilmez.
Yani Console üzerinden eklenen bir kaynak, bir sonraki `helm upgrade lakehouse
./chart -f chart/values-prod.example.yaml -n example` çalıştırmasında **görünmez**
(chart `values`'ında yok) ve üzerine yazılabilir/silinebilir bir drift oluşturur.
Katılımcıya açıkça söylenmesi gereken nokta: **Console hız için var, chart'ın
(`chart/templates/13-connectors.yaml` + `chart/values.yaml` `connectors.*`)
yerini almaz** — üretimde periyodik olarak `oc get kafkaconnector -n example`
çıktısı chart'ın `values`'ıyla karşılaştırılıp fark varsa manuel geri
yansıtılmalı (bkz. `tools/README.md` "Self-Service Console" bölümü).

---

## 2. Yeni kaynak ekleme atölyesi (Console sihirbazı)

Katılımcı, ingest atölyesindeki **aynı örneği** (`dbo.grades` — `mssql1`
kaynağının scheduled/Aiven JDBC yolu) bu kez CLI yerine Console'un sihirbazıyla
(`/sources/add`) ekleyecek. Sihirbaz 6 adımdır (bkz.
`console/frontend/src/pages/AddSourceWizard.tsx`):

**Adım 1 — Tip seç:** `Kind = scheduled`, `Type = mssql`. (CLI atölyesindeki
"0. Tip seç" adımıyla birebir aynı karar: delete önemsiz, periyodik senkron
yeterli → CDC feature gerekmez.)

**Adım 2 — Bağlantı & kimlik bilgisi:** `Source name = mssql1`,
`JDBC URL`, `Username`/`Password` (bu ikisi backend'e **yalnızca** bir
`Secret` oluşturmak için gider — `console/backend/app/services/k8s_service.py`
`create_secret`; ekranda geri gösterilmez, log'lanmaz).

**Adım 3 — Tablo & seçenekler:** `Database = OgrenciDB`, `Table = dbo.grades`,
`Incrementing column = grade_id`, `Timestamp column = updated_at` (delta
penceresi için — CLI atölyesindeki `--incrementing-col`/`--timestamp-col`
argümanlarının GUI karşılığı, `console/backend/app/models.py` `SourceSpec`
alanlarıyla bire bir eşlenir).

**Adım 4 — Hedef:** `Target namespace = mssql_ogrenci` (zaten `students`/
`courses` ile paylaşılan namespace — CLI atölyesindeki gibi), `Target table
= grades`.

**Adım 5 — Önizleme:** "Fetch preview" butonu `POST /api/sources/preview`
çağırır — **hiçbir K8s/S3/Trino client'ı oluşturulmaz** (bkz.
`console/backend/app/routers/sources.py` `preview_source`'un docstring'i);
yalnızca `render_service`'in üreteceği `KafkaConnector`/`KafkaTopic` CR'ları
ve namespace DDL'i gösterilir. Katılımcıya vurgulanacak nokta: bu, CLI'daki
`scaffold-source.sh --dry-run`'ın **tam eşdeğeridir** — "önce gör, sonra
uygula" prensibi her iki yolda da korunur.

**Adım 6 — Uygula:** "Create source" butonu `POST /api/sources` çağırır
(`AddSourceOrchestrator`) — bucket + namespace DDL + `KafkaTopic` +
`KafkaConnector` sırayla ve idempotent şekilde oluşturulur (spec §4).

**Doğrula** (CLI atölyesiyle aynı komutlar — Console bunları göstermez, hâlâ
`oc`/`trino` gerekir):
```bash
oc get kafkaconnector -n example                     # yeni connector RUNNING
trino --execute "SELECT count(*) FROM lakehouse.mssql_ogrenci.grades"
```
Ya da Console'un kendi **Durum panosu**ndan (`/status`) `mssql1`'in
`state`/`dlq`/`maintenance` sütunlarını izleyin — bkz. §4 aşağıda.

**Katılımcıya vurgulanacak nokta (ingest atölyesindeki ile aynı):** Iceberg
sink'e (`iceberg-sink-cdc`) hiç dokunulmadı; sink zaten `topics.regex` ile
yeni topic'i otomatik yakalar. Console de dahil hiçbir yol bu connector'ı
tekrar oluşturmaz/değiştirmez — "yeni kaynak eklemek connector tanımlamaktan
ibarettir" prensibi CLI'da olduğu kadar Console'da da geçerlidir.

---

## 3. Rol ve silme güvenliği

Console'un yetkilendirmesi **tamamen backend'de** zorlanır
(`console/backend/app/services/authz.py` + `app/deps.py`
`require_action`/`require_delete_mode`) — frontend yalnızca hangi butonların
görüneceğini belirler, güvenlik sınırı değildir. AD/OIDC `groups` claim'i
şu rollere eşlenir:

| AD grubu | Rol | Yapabildikleri |
|---|---|---|
| `lakehouse-admins` | **ADMIN** | Her şey: kaynak ekle/düzenle/pause/resume, **yalnızca-pipeline VE veriyle-birlikte silme**, tablo/bucket oluştur. |
| `lakehouse-analysts` | **ANALYST** | ADMIN ile aynı, **tek istisna: veriyle-birlikte silme YOK.** Kaynak ekleyebilir, düzenleyebilir, yalnızca-pipeline silebilir. |
| `lakehouse-students` | **STUDENT** | Yalnızca okuma (liste, detay, durum panosu, tablo/şema listesi). Hiçbir mutasyon yapamaz. |

**"Veriyle silme" neden çift-onaylıdır:**

1. **Rol seviyesinde:** `with_data` silme modu `Action.SOURCE_DELETE_WITH_DATA`
   gerektirir — yalnızca ADMIN rolünün `_MATRIX`'inde bu aksiyon var
   (ANALYST'te bilerek YOK). Bir ANALYST API'yi doğrudan çağırsa bile backend
   403 döner; frontend'in "veriyle birlikte" radio butonunu göstermemesi
   yalnızca **ikinci** bir savunma katmanıdır, tek savunma değildir.
2. **UI seviyesinde (yalnızca ADMIN'e görünür):** `with_data` seçildiğinde
   Console, operatörün silinecek kaynağın **tam adını** yeniden yazmasını
   ister (bkz. `console/frontend/src/components/DeleteModal.tsx`
   `nameConfirmed`); metin eşleşmeden "Delete" butonu devre dışı kalır.
   Bu, "yanlışlıkla tıklama" senaryosunu (ör. yanlış satırda Delete'e
   basmak) önler — kullanıcı bilinçli olarak hangi kaynağı sildiğini
   yazmak zorundadır.

**Neden bu iki katmanın ikisi de gerekli:** yalnızca rol kontrolü olsaydı bir
ADMIN yanlış kaynağı tek tıkla silebilirdi (veri geri gelmez — Iceberg tablo
`DROP`, bucket boşaltılır, `KafkaTopic` silinir). Yalnızca isim-yazma onayı
olsaydı bir ANALYST/STUDENT de (backend gate'i olmasaydı) veri kaybına yol
açabilirdi. İkisi birlikte: **"kim yapabilir" (rol) + "gerçekten bunu mu
istiyorsun" (isim onayı)**.

**Karşılaştırma — `pipeline_only` (varsayılan) vs `with_data`:**

| | `pipeline_only` (varsayılan) | `with_data` |
|---|---|---|
| Silinen | Yalnızca `KafkaConnector` CR | + `KafkaTopic`, Iceberg tablo (`DROP TABLE`), bucket içeriği |
| Kim yapabilir | ADMIN, ANALYST | **Yalnızca ADMIN** |
| Geri alınabilir mi | Evet (connector'ı tekrar apply edin, veri lakehouse'ta zaten duruyor) | **Hayır** — veri kalıcı olarak kaybolur |
| UI onayı | Tek tık | İsim yazarak onay |

---

## 4. Deep-dive UI'lar — ne zaman kullanılır

Console'un kendi Durum panosu (`/status`) **hızlı bir özet** verir: her
connector için `state` (RUNNING/PAUSED/FAILED), `maintenance` (bilerek
duraklatılmış mı), `dlq` (herhangi bir task DLQ'ya mesaj düşürmüş mü) —
ama **consumer-group lag'i göstermez** (Kafka Connect REST API'si bunu
sağlamaz; `lag` alanı bilerek `None` bırakılmış, "henüz yok" ile "sıfır
lag"ı karıştırmamak için — bkz. `console/backend/app/routers/status.py`
docstring'i). Bu yüzden aşağıdaki durumlarda **derinlemesine inceleme
UI'larına** geçin:

- **Kafka UI (`kafbat-ui`) — ne zaman:**
  - Consumer lag'i **sayısal olarak** görmek istediğinizde (Console bunu
    göstermez) — Kafka UI'nin consumer-group ekranı gerçek lag değerini verir.
  - Bir topic'teki **gerçek mesaj içeriğini** okumak istediğinizde (ör.
    "bu satır gerçekten Kafka'ya düştü mü, hangi payload ile" — DLQ debug'ı
    için birebir kullanışlı).
  - Kafka Connect worker'ın **kendi** durumunu (task trace, config) Console'un
    özetinden daha ayrıntılı görmek istediğinizde.
  - **Dikkat:** Kafka UI'de connector/topic **oluşturma/silme YOK** — RBAC
    bilerek yalnızca `view`/`messages_read` veriyor (bkz. `16-kafka-ui.yaml`
    `rbac.roles`); bu bir izleme aracıdır, veri düzlemini değiştirmek için
    Console'u veya Git'i (`13-connectors/`) kullanın.
  - Rol farkı: `lakehouse-admins` cluster/topic/connect/connector'ın tamamını
    görür; `lakehouse-students` yalnızca topic "view" alır (connect/connector
    sekmeleri öğrenciye görünmez).
- **Apicurio Registry UI — ne zaman:** Bir kaynağın (yalnızca ilişkisel/Avro
  yolu — Mongo bu registry'yi bypass eder, ham JSON kullanır) şemasının
  **sürüm geçmişini** veya **BACKWARD uyumluluk** durumunu incelemek
  istediğinizde; Console'un `/schemas` ekranı (`GET /api/schemas`) yalnızca
  salt-okunur bir liste sunar (kayıt/versiyon detayı için Apicurio UI'ye geçin).
- **Trino (CLI veya bir SQL istemcisi) — ne zaman:** Veriyi **sorgulamak**
  istediğinizde — Console veri düzlemini görüntülemez, yalnızca namespace/
  tablo/bucket **envanterini** listeler (`GET /api/tables`, `GET /api/buckets`).
  Kaynaklar-arası JOIN, satır sayısı doğrulama, "silme sonrası veri gerçekten
  gitti mi" kontrolü — hepsi Trino'da yapılır (bkz. ingest atölyesi §4.4
  federasyon örneği, bu ekin kapsamı dışındadır ama Console'un `/tables`
  ekranı aynı `lakehouse` katalogunu gösterir).

**Özet karar kuralı:** Console = "bu pipeline sağlıklı mı, ne var, kim
yönetebilir" sorusuna hızlı cevap. Kafka UI/Apicurio/Trino = "tam olarak
neden/ne kadar/hangi veri" sorusuna derinlemesine cevap.
