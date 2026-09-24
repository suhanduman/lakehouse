# Değişiklik nasıl uygulanır

**Bu bölümde:** üründe yapılan **her** yapılandırma değişikliğinin izlediği tek yol —
`platform/values/site/` altındaki dosyayı düzenleme, yerelde denetleme, commit + push,
ArgoCD'nin kümeye uygulamasını izleme, gerekirse eşitlemeyi zorlama ve `git revert` ile
geri alma. Kaynak ekleme, tablo ekleme, kullanıcı yetkisi, yedek ayarı: hepsi bu
döngüden geçer; diğer işletme bölümleri yalnız "hangi satır değişir" sorusunu yanıtlar.
**Süre:** düzenleme ve push 5–10 dakika; ArgoCD'nin uygulaması 1–5 dakika (`glue`
uygulamasında Kafka Connect yeniden başlarsa 10 dakikaya kadar).
**Gereken yetki:** kurumun Git deposunda `main` dalına yazma; `$ARGOCD_NS` ad alanında
okuma (`oc get application`). Küme üzerinde elle değişiklik yetkisi **gerekmez** ve
istenmez.
**Nerede çalıştırılır:** `[bastion]` — `git`, `oc` ve `helm` kurulu, `oc login` ile
kümeye girilmiş, deponun kopyası (`~/lakehouse`) duran yönetim makinesi.

Döngünün tamamı:

```text
site/*.yaml düzenle -> check-site.sh -> commit + push -> ArgoCD sync -> küme
                                                      -> git revert -> eski hâl
```

> **Kümeye elle dokunulmaz.** `glue` uygulamasının eşitleme kuralı `selfHeal: true`
> ve `prune: true` ile açıktır (`platform/apps/10-glue.yaml`): `oc edit` ile yapılan bir
> değişiklik birkaç dakika içinde Git'teki hâle geri alınır, Git'te karşılığı olmayan
> nesne ise silinir. Kalıcı olan tek şey Git'e yazılandır.

---

## 1. Değişkenleri yükleyin

Her yeni terminal oturumunda gereklidir; komutlardaki `$ARGOCD_NS` ve `$LAKEHOUSE_NS`
buradan gelir.

`[bastion]`

```bash
cd ~/lakehouse                      # depoyu `git clone` ile nereye indirdiyseniz orası
set -a; . install/lakehouse.env; set +a
echo "argocd=$ARGOCD_NS lakehouse=$LAKEHOUSE_NS"
```

**Beklenen çıktı** (örnek — kendi değerlerinizle):

```text
argocd=openshift-gitops lakehouse=lakehouse
```

**Ters giderse:** boş satır görüyorsanız dosya yüklenmemiştir; şablonu
`install/lakehouse.env.example` dosyasındadır ve değerler
[30-kurulum](../30-kurulum.md) §1'de doldurulmuştur.

---

## 2. Hangi dosya değişir

Müşteriye özel **her** değer yalnız `platform/values/site/` altındaki üç dosyadadır.
`platform/values/` kökündeki dosyalar ve `glue/values.yaml` ürün varsayılanlarıdır;
onlara dokunulmaz — bir sonraki ürün sürümünde üzerine yazılırlar.

| Dosya | Ne için | Sık değişen anahtarlar | `install/lakehouse.env` karşılığı |
|---|---|---|---|
| `platform/values/site/glue.yaml` | omurga: kaynaklar, Kafka, Polaris, Keycloak, yedek | `sources`, `pipelines`, `nginx.enabled`, `appsDomain`, `s3.endpoint`, `backup.s3.bucket` | `$APPS_DOMAIN`, `$S3_ENDPOINT`, `$S3_BUCKET_BACKUP`, `$LDAP_URL` |
| `platform/values/site/trino.yaml` | Trino erişim kuralları, grup sağlayıcı | erişim kuralları, `group-provider` bloğu | `$LDAP_URL`, `$LDAP_GROUPS_DN` |
| `platform/values/site/jupyterhub.yaml` | not defteri ortamı | disk boyutu, kaynak sınırları | `$STORAGE_CLASS` |

Anahtarların tamamı ve varsayılanları:
[90-referans/values-anahtarlari.md](../90-referans/values-anahtarlari.md).

**Git'e girmeyen tek şey Secret'lardır.** Parola, anahtar ve sertifika değerleri values
dosyalarına **yazılmaz**; `oc create secret` ile kümede yaratılır ve values yalnız
Secret'ın **adını** taşır. Liste:
[90-referans/secret-listesi.md](../90-referans/secret-listesi.md).

---

## 3. Değişikliği yapın ve yerelde denetleyin

Önce deponun güncel olduğundan emin olun, sonra dosyayı düzenleyin.

`[bastion]`

```bash
git pull --rebase origin main
${EDITOR:-vi} platform/values/site/glue.yaml
bash scripts/check-site.sh
```

`scripts/check-site.sh` üç site dosyasını birbiriyle çapraz doğrular (ör. AD kök CA
Secret'ı ile Trino'nun truststore yolu birlikte açılıp kapanmış mı) ve örnek değer kalıp
kalmadığına bakar.

**Beklenen çıktı** (kind kümesinde alınmış gerçek çıktı; `UYARI` satırı depodaki örnek
`setup.yaml` içindir, kendi kurulumunuzda çıkmayabilir):

```text
UYARI: platform/polaris/setup.yaml: endpoint 'https://s3.example.com' değil — Polaris sunucusu için ayrıca düzenlenir (docs/30-kurulum.md)
check-site: OK
```

Değişiklik `glue` chart'ının ürettiği nesneleri etkiliyorsa (kaynak ekleme, nginx akışı,
hostname) kümeye gitmeden **render edip** bakabilirsiniz:

`[bastion]`

```bash
helm template glue ./glue -n "$LAKEHOUSE_NS" \
  -f platform/values/glue.yaml -f platform/values/site/glue.yaml \
  | grep -E "^kind: |^  name: "
```

**Ters giderse:** `check-site: OK` yerine `HATA` satırı basılırsa değişiklik **itilmez**;
hata satırı hangi dosyadaki hangi anahtarın tutarsız olduğunu söyler. `helm template`
`Error: ... required` ile duruyorsa zorunlu bir alan boş bırakılmıştır (ör. bir kaynağın
`signalTable` alanı).

---

## 4. Commit edin ve itin

Değişiklik kümeye **yalnız** buradan geçer.

`[bastion]`

```bash
git add platform/values/site/glue.yaml
git commit -m "site: erp kaynagi eklendi"
git push origin main
```

**Beklenen çıktı** (örnek — kurumun Git sunucusunun adresi ve nesne sayıları farklıdır):

```text
To ssh://git.kurum.example.net/lakehouse.git
   4ff2cc4..9a13b7c  main -> main
```

**Ters giderse:** `rejected ... non-fast-forward` alırsanız depoda sizden sonra başka bir
commit vardır; `git pull --rebase origin main` çalıştırıp Adım 3'ü tekrarlayın. Çakışma
çıkarsa **elle çözmeden önce** değişikliği yapan kişiyle konuşun: aynı `sources`
listesine iki kişi aynı anda yazmış olabilir.

---

## 5. ArgoCD'nin uygulamasını izleyin

ArgoCD depoyu **en geç üç dakikada bir** yoklar; değişikliği gördüğünde uygulamayı
kendiliğinden eşitler (`platform/apps/10-glue.yaml` içinde `automated` kuralı `prune` ve
`selfHeal` ile açıktır).

`[bastion]`

```bash
oc -n "$ARGOCD_NS" get application glue \
  -o jsonpath='{.status.sync.status} {.status.health.status}{"\n"}'
```

**Beklenen çıktı** (örnek — OpenShift'e özgü; eşitleme bittiğinde iki sözcük de bu
hâlini alır):

```text
Synced Healthy
```

Eşitleme sırasında sırayla `OutOfSync Progressing` → `Synced Progressing` →
`Synced Healthy` görülür. Kafka Connect'in yeniden başlamasını gerektiren değişikliklerde
(kaynak ekleme ya da çıkarma) `Progressing` aşaması birkaç dakika sürer.

Hangi işlemin uygulandığını görmek için:

`[bastion]`

```bash
oc -n "$ARGOCD_NS" get application glue -o jsonpath='{.status.operationState.message}{"\n"}'
```

**Ters giderse:** beş dakikadan uzun süre `Synced` görünmüyorsa değişiklik ArgoCD'ye
ulaşmamıştır — Adım 4'teki push'un gerçekten kurumun deposuna gittiğini
(`git remote -v`) ve `platform/apps/10-glue.yaml` içindeki `targetRevision` dalının
ittiğiniz dal olduğunu doğrulayın.

### 5.1 Eşitlemeyi beklemeden zorlamak

Depoyu hemen okutmak için uygulamaya **hard refresh** açıklaması yazılır. Bu, önbelleği
atlatır; küme üzerinde bir şeyi silmez.

`[bastion]`

```bash
oc -n "$ARGOCD_NS" annotate application glue argocd.argoproj.io/refresh=hard --overwrite
```

**Beklenen çıktı** (örnek):

```text
application.argoproj.io/glue annotated
```

**Ters giderse:** açıklama yazıldığı hâlde eşitleme başlamıyorsa ArgoCD depoya
erişemiyordur; `lakehouse-repo` Secret'ındaki anahtar ya da adres bozulmuş olabilir
([30-kurulum](../30-kurulum.md) §3).

### 5.2 ArgoCD'siz (helm modu) kurulumlar

Geliştirme ve deneme kümeleri ArgoCD'siz "helm modunda" kurulmuş olabilir
([30-kurulum](../30-kurulum.md) "Lokal deneme (kind)" bölümü). Orada Adım 5 geçerli
değildir; commit'ten sonra değişiklik elle uygulanır:

`[bastion]`

```bash
helm upgrade glue ./glue -n "$LAKEHOUSE_NS" -f platform/values/glue-dev.yaml
```

**Beklenen çıktı** (kind kümesinde alınmış gerçek çıktı; `REVISION` her koşuda bir artar):

```text
Release "glue" has been upgraded. Happy Helming!
NAME: glue
LAST DEPLOYED: Thu Sep 24 12:16:48 2026
NAMESPACE: lakehouse
STATUS: deployed
REVISION: 11
DESCRIPTION: Upgrade complete
```

**Ters giderse:** `Error: UPGRADE FAILED: ... has no deployed releases` görürseniz release
adı yanlıştır; `helm -n "$LAKEHOUSE_NS" list` ile doğru adı alın.

---

## 6. Değişikliği geri alın

Kümeye uygulanmış bir değişiklik **kümede geri alınmaz**; Git'te geri alınır. `git revert`
ters içerikli yeni bir commit yazar, böylece tarihçe korunur ve ArgoCD eski hâli aynı
döngüyle uygular.

`[bastion]`

```bash
git pull --rebase origin main
git revert --no-edit HEAD
git push origin main
```

**Beklenen çıktı** (örnek):

```text
[main 3c9f0a2] Revert "site: erp kaynagi eklendi"
 1 file changed, 6 deletions(-)
```

Birkaç commit öncesine dönmek için `HEAD` yerine commit kimliği yazılır
(`git log --oneline -- platform/values/site/` ile bulunur). `git reset` ve zorlamalı push
**kullanılmaz**: ArgoCD'nin gördüğü dal geriye alınırsa eşitleme geçmişi ile küme hâli
arasındaki bağ kopar.

**Ters giderse:** `error: could not revert ... your local changes would be overwritten`
→ çalışma kopyanızda kaydedilmemiş düzenleme vardır; `git status` ile bakıp saklayın
(`git stash`) ya da geri alın.

> **Geri alınamayan iki şey.** (1) Kaynak veritabanından silinen bir tablo ya da katalogdan
> silinen bir Iceberg tablosu `git revert` ile geri gelmez; veri yedeklerden döner.
> (2) Bir kaynak `sources` listesinden çıkarılıp geri eklenirse Debezium sıfırdan snapshot
> alır. İkisi de işletme bölümündeki kaynak ve tablo silme sayfasında ayrıca anlatılır.

---

## 7. Eşitleme takılırsa

| Belirti | Olası neden | İlk bakılacak |
|---|---|---|
| `OutOfSync` kalıyor, işlem mesajı boş | ArgoCD depoyu okuyamıyor | `lakehouse-repo` Secret'ı, `git remote -v` |
| `OutOfSync Progressing` dakikalarca sürüyor | bir pod yeniden başlayamıyor | `oc -n "$LAKEHOUSE_NS" get pods` |
| `Synced Degraded` | nesne uygulandı ama sağlıksız (ör. `KafkaConnector` FAILED) | `oc -n "$LAKEHOUSE_NS" get kafkaconnector` |
| `ComparisonError` ya da `rpc error` | chart render hatası (eksik zorunlu alan) | Adım 3'teki `helm template` |
| Değişiklik uygulandı ama kümede geri alındı | aynı nesne `oc edit` ile elle değiştirilmiş; `selfHeal` geri aldı | Git'teki hâli otoriterdir |

Belirtilerin tam tablosu ve komut komut çözümü işletme bölümündeki **sorun giderme**
sayfasındadır.

---

## Kontrol listesi

- [ ] Değişiklik yalnız `platform/values/site/` altındaki bir dosyada yapıldı.
- [ ] `scripts/check-site.sh` `check-site: OK` bastı.
- [ ] Commit kurumun deposundaki `main` dalına itildi.
- [ ] `oc -n "$ARGOCD_NS" get application glue` çıktısı `Synced Healthy`.
- [ ] Değişikliğin kümedeki karşılığı (yeni nesne, yeni tablo, yeni ayar) gözle görüldü.
- [ ] Gerekirse geri almanın `git revert` olduğu, `oc edit` olmadığı ekipçe biliniyor.

## Sonraki bölüm

Bu döngüyü kullanan ilk iki işletme görevi:
[yeni-kaynak-ve-pipeline.md](yeni-kaynak-ve-pipeline.md) — yeni bir kaynak veritabanının
(PostgreSQL, SQL Server, MongoDB) ve nginx erişim günlüğü akışının eklenmesi;
[mevcut-kaynaga-tablo-ekleme.md](mevcut-kaynaga-tablo-ekleme.md) — var olan bir kaynağa
tablo ekleme ve artımlı snapshot.
