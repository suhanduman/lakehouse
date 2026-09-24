# Kullanıcı ve yetki

**Bu bölümde:** kimin hangi veriyi görebileceğinin nerede tanımlandığı — Active
Directory grubundan başlayıp Keycloak, Trino, Superset, JupyterHub ve Zeppelin'e kadar;
kullanıcı ekleme ve çıkarma, yeni bir grubun açılması, satır filtresi ve kolon maskesi
yazma, yazma alanı (`sandbox`) ve paylaşımlı servis hesaplarının sınırları.
**Süre:** var olan bir gruba kullanıcı eklemek dakikalar (AD tarafındaki iş); yeni bir
erişim kuralı yazmak 15–30 dakika (düzenleme + eşitleme + doğrulama).
**Gereken yetki:** AD tarafında grup üyeliği düzenleme (kimlik ekibi); Git deposunda
`main` dalına yazma; `$LAKEHOUSE_NS` ad alanında okuma.
**Nerede çalıştırılır:** `[bastion]` — `git` ve `oc` kurulu, `oc login` ile kümeye
girilmiş yönetim makinesi. Doğrulama sorguları `[pod]` etiketiyle işaretlidir.

> **Yetki kümede elle verilmez.** Bütün erişim kuralları Git'teki değer dosyalarındadır;
> `oc edit` ile yapılan bir değişikliği eşitleme birkaç dakika içinde geri alır
> ([değişiklik nasıl uygulanır](degisiklik-nasil-uygulanir.md)).

---

## 1. Değişkenleri yükleyin

`[bastion]`

```bash
cd ~/lakehouse
set -a; . install/lakehouse.env; set +a
echo "lakehouse=$LAKEHOUSE_NS grup agaci=$LDAP_GROUPS_DN"
```

**Beklenen çıktı** (örnek — kendi değerlerinizle):

```text
lakehouse=lakehouse grup agaci=OU=Groups,DC=example,DC=com
```

**Ters giderse:** ikinci alan boşsa `install/lakehouse.env` doldurulmamıştır
([30-kurulum](../30-kurulum.md) §1).

---

## 2. Tek kimlik kaynağı: Active Directory

Kullanıcı hesabı ve grup üyeliği **yalnız AD'de** tutulur. Ürün kendi kullanıcı listesini
taşımaz; her bileşen grubu farklı bir yoldan öğrenir ama **grup adı hepsinde aynıdır**:

- AD grubu → Keycloak grubu → Trino grubu, üçü de aynı adla.
- Üç grup vardır ve adları değişmez: `lakehouse-admins`, `lakehouse-analysts`,
  `lakehouse-users` ([20-on-kosullar](../20-on-kosullar.md) madde 6).

| Bileşen | Kimlik nereden | Grup bilgisi nereden gelir |
|---|---|---|
| Trino | Keycloak (tarayıcı/jeton) ya da servis hesabı (kullanıcı adı + parola) | **Grup sağlayıcısı**: üretimde doğrudan AD/LDAP (`platform/values/site/trino.yaml` → `group-provider.properties`), geliştirme kümesinde dosya (`platform/values/trino-dev.yaml`). Trino grubu jetondan **okuyamaz**, bu yüzden ayrı bir sağlayıcı gerekir |
| Superset | Keycloak | Keycloak'ın kullanıcı bilgisi yanıtındaki `groups` talebi → rol eşlemesi |
| JupyterHub | Keycloak | Aynı `groups` talebi → izinli gruplar ve yönetici grupları |
| Zeppelin | **Doğrudan AD (LDAPS)** — Keycloak kullanmaz | AD grup üyeliği → Zeppelin rolü (`examples/zeppelin/shiro-ad.ini`) |

**Zeppelin'in ayrı olması bilinçlidir:** kullanılan Zeppelin sürümünde OIDC desteği yoktur;
giriş doğrudan AD'ye LDAPS ile yapılır. Sonuç aynıdır (aynı üç grup), yol farklıdır.

---

## 3. Grup ↔ rol tablosu

| | `lakehouse-admins` | `lakehouse-analysts` | `lakehouse-users` |
|---|---|---|---|
| **Trino — katalog** | tam erişim (`lakehouse`, `system`) | tam erişim | yalnız okuma |
| **Trino — şema sahipliği** | her şemada sahip (tablo yaratma/silme) | yalnız `sandbox` şemasında sahip | sahiplik yok |
| **Trino — tablo** | `SELECT`, `INSERT`, `DELETE`, `UPDATE`, sahiplik | `sandbox`'ta tam; diğer şemalarda `SELECT` | yalnız `SELECT` + satır filtresi ve kolon maskesi |
| **Superset** | `Admin` | `Alpha` (kendi grafik ve panolarını yaratır) | `Gamma` (yalnız kendisine açılanı görür) |
| **JupyterHub** | giriş + hub yöneticisi | giriş | giriş |
| **Zeppelin** | `admin` rolü | `analyst` rolü | `user` rolü |

Eşlemelerin tanımlı olduğu yerler: Superset `glue/values.yaml` → `superset.roleMapping`;
JupyterHub `platform/values/jupyterhub.yaml` (izinli gruplar üçü de, yönetici grubu yalnız
`lakehouse-admins`); Zeppelin `examples/zeppelin/shiro-ad.ini`; Trino ise §5'teki erişim
kurallarıdır. Bu dört yer **ürün varsayılanıdır**; grup adları değişmediği sürece dokunulmaz.

**Rolsüz kullanıcı ne görür?** Zeppelin'de hiçbir şey: son kural üç rolden birini zorunlu
tutar, rolü olmayan kullanıcı giriş yapsa bile not defteri listesini alamaz (401). Trino'da
hiçbir katalog görünmez. Superset'te ise kullanıcı **hesap açabilir** ama `Public` rolünde
kalır: veri kaynağı, SQL Lab, pano ve grafik izinlerinin hiçbiri yoktur, boş bir arayüz
görür. Bu bilinçli bir ayardır — Superset'in kullandığı çerçevede ilk girişte kullanıcı
kaydı yaratılmazsa `lakehouse-admins` üyesi bile giremez. Grubu olmayanı Superset'e hiç
sokmamak isteniyorsa doğru yer **Keycloak**'tır (`superset` istemcisine grup zorunluluğu
eklenir); Superset tarafında kaydı kapatmak girişi **herkes için** kırar.

---

## 4. Kullanıcı ekleme ve çıkarma

**Kullanıcı eklemek = AD grubuna üye eklemek.** Üründe yapılacak hiçbir şey yoktur:
kullanıcı yaratılmaz, parola verilmez, izin satırı yazılmaz.

1. Kimlik ekibi kullanıcıyı ilgili gruba ekler (`lakehouse-admins`, `lakehouse-analysts`
   ya da `lakehouse-users`).
2. Kullanıcı arayüzlerden birine girer; hesabı ilk girişte kendiliğinden oluşur.
3. Rol her girişte grup üyeliğinden **yeniden hesaplanır** — gruptan çıkarılan kullanıcı bir
   sonraki girişinde yetkisini kaybeder.

Üyeliğin AD'de göründüğünü doğrulamak için:

`[bastion]`

```bash
ldapsearch -LLL -H "$LDAP_URL" -D "$LDAP_BIND_DN" -W \
  -b "$LDAP_GROUPS_DN" "(cn=lakehouse-analysts)" member
```

**Beklenen çıktı** (örnek — AD'ye özgü, **OpenShift'te doğrulanır**): grubun `member`
satırlarında kullanıcının tam adı (DN) görünür.

**Ters giderse:** `ldapsearch` bağlanamıyorsa bağlanma hesabı ya da LDAPS sertifikası
sorunludur ([20-on-kosullar](../20-on-kosullar.md) madde 6).

### 4.1 Değişiklik ne zaman etkili olur

| Bileşen | Etkili olma anı |
|---|---|
| Superset, JupyterHub, Zeppelin | Kullanıcının **bir sonraki girişinde** |
| Trino (üretim, AD/LDAP grup sağlayıcısı) | Kullanıcının bir sonraki oturumunda; ayrıca bir işlem gerekmez |
| Trino (geliştirme kümesi, dosya sağlayıcısı) | **Kendiliğinden olmaz** |

**Bu ayrım önemlidir.** Üretimde Trino grupları doğrudan AD'den okunur; AD'de yapılan üyelik
değişikliği başka hiçbir adım gerektirmez. Geliştirme ve kind kurulumlarında ise gruplar bir
**dosyadan** gelir (`platform/values/trino-dev.yaml` → `auth.groups`) ve bu dosyanın yenileme
periyodu **yoktur**: Trino onu yalnız açılışta okur. Orada bir grup adı değiştirildiğinde
`trino` sürümünün yeniden kurulması **ve** ardından koordinatörün yeniden başlatılması
gerekir:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" rollout restart deploy/trino-coordinator
```

**Beklenen çıktı** (örnek):

```text
deployment.apps/trino-coordinator restarted
```

Erişim **kuralları** (§5) bundan farklıdır: onlar bir ConfigMap'ten okunur ve 60 saniyede bir
tazelenir, yeniden başlatma gerektirmez.

---

## 5. Trino erişim kuralları

Yetkilendirmenin kalbi tek bir JSON belgesidir. Üründeki hâli `platform/values/trino.yaml`
→ `accessControl.rules` altındadır; müşteriye özel hâli `platform/values/site/trino.yaml`
dosyasına yazılır.

### 5.1 Üç bölüm ve tek kural

- **`catalogs`** — katalog görünürlüğü: `all`, `read-only` ya da `none`.
- **`schemas`** — şema **sahipliği**: `owner: true` = o şemada tablo yaratma ve silme hakkı.
- **`tables`** — tablo ayrıcalıkları + **satır filtresi** + **kolon maskesi**.

**İLK EŞLEŞEN KURAL KAZANIR.** Bu yüzden özel kurallar (bir gruba özgü filtre ya da maske)
genel `SELECT` kuralından **önce** yazılır. Sırayı bozmak filtreyi sessizce devre dışı
bırakır — hata mesajı alınmaz, yalnız kural işlemez.

Eşleştirme alanları düzenli ifadedir (`"group": "lakehouse-admins|lakehouse-analysts"`);
`user` alanı servis hesapları içindir (§7).

- **Satır filtresi:** `"filter": "status <> 'shipped'"` — kurala düşen kullanıcının o tabloya
  her erişimine `WHERE` koşulu olarak eklenir.
- **Kolon maskesi:** `"columns": [{"name": "remote", "mask": "'x.x.x.x'"}]` — maske bir SQL
  ifadesidir (sabit değer, `CASE`, karma fonksiyonu…) ve kolonun tipiyle uyumlu olmalıdır.

Kümedeki güncel hâli okumak için:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get configmap trino-access-control-volume-coordinator \
  -o jsonpath='{.data.rules\.json}' | head -20
```

**Beklenen çıktı** (kind kümesinde alınmış gerçek çıktı — ürün varsayılanının ilk 20
satırı):

```text
{
  "catalogs": [
    {"group": "lakehouse-admins|lakehouse-analysts", "catalog": "lakehouse|system", "allow": "all"},
    {"group": "lakehouse-users", "catalog": "lakehouse|system", "allow": "read-only"},
    {"user": "superset|zeppelin|e2e", "catalog": "lakehouse|system", "allow": "read-only"}
  ],
  "schemas": [
    {"group": "lakehouse-admins", "owner": true},
    {"group": "lakehouse-analysts", "schema": "sandbox", "owner": true},
    {"owner": false}
  ],
  "tables": [
    {"group": "lakehouse-admins", "privileges": ["SELECT", "INSERT", "DELETE", "UPDATE", "OWNERSHIP"]},
    {"group": "lakehouse-analysts", "schema": "sandbox", "privileges": ["SELECT", "INSERT", "DELETE", "UPDATE", "OWNERSHIP"]},
    {"group": "lakehouse-analysts", "privileges": ["SELECT"]},
    {"group": "lakehouse-users", "schema": "shop", "table": "orders", "privileges": ["SELECT"], "filter": "status <> 'shipped'"},
    {"group": "lakehouse-users", "schema": "nginx_raw", "table": "access_log", "privileges": ["SELECT"],
     "columns": [{"name": "remote", "mask": "'x.x.x.x'"}]},
    {"group": "lakehouse-users", "privileges": ["SELECT"]},
```

**Ters giderse:** ConfigMap bulunamıyorsa Trino uygulaması eşitlenmemiştir
([değişiklik nasıl uygulanır](degisiklik-nasil-uygulanir.md) Adım 5).

### 5.2 Kural eklerken: blok **tamamen** ezilir

`platform/values/site/trino.yaml` değer listesinin **en sonundadır**; Helm çok satırlı metin
bloklarını birleştirmez, son dosyanınki bütünüyle geçer. Bu yüzden site dosyasına bir kural
eklemek demek, **kuralların tamamını** oraya taşımak demektir: ürünün varsayılan satırları da
dâhil. Aynı kural site dosyasındaki diğer bloklar için de geçerlidir ve dosyanın başındaki
yorumda yazılıdır.

Yol:

1. Yukarıdaki `oc get configmap …` çıktısını ya da `platform/values/trino.yaml` içindeki
   bloğu **tam olarak** kopyalayın.
2. `platform/values/site/trino.yaml` dosyasına `accessControl.rules` altında `rules.json`
   anahtarıyla yapıştırın.
3. Kendi kuralınızı **doğru sıraya** ekleyin: özel kurallar genel `SELECT` kuralından önce.
4. Ürün satırlarının eksilmediğini denetleyip commit edin. `scripts/check-site.sh`,
   blok verilmişse ürünün **servis hesabı** satırlarını arar
   (`{"user": "superset|zeppelin|e2e", …}` — `catalogs` ve `tables` dizilerindeki iki
   satır); bunlar düşerse Superset panoları ve Zeppelin not defterleri Trino'dan sessizce
   `Access Denied` alır. `|e2e` eki isteğe bağlıdır: yalnız e2e koşusunun kullandığı
   hesaptır, üretimde çıkarabilirsiniz.

`[bastion]`

```bash
${EDITOR:-vi} platform/values/site/trino.yaml
bash scripts/check-site.sh
git add platform/values/site/trino.yaml
git commit -m "site: muhasebe grubuna satir filtresi"
git push origin main
```

**ArgoCD'siz (helm/kind) kurulumda:** bkz.
[30-kurulum.md kind kutusu](../30-kurulum.md#kind-gun2) (`helm upgrade`) — site dosyası bu
modda okunmaz.


**Beklenen çıktı** (kind kümesinde alınmış gerçek çıktı; `UYARI` satırı depodaki örnek dosya
içindir, kendi kurulumunuzda çıkmayabilir):

```text
UYARI: platform/polaris/setup.yaml: endpoint 'https://s3.example.com' değil — Polaris sunucusu için ayrıca düzenlenir (docs/30-kurulum.md)
check-site: OK
```

**Ters giderse:** `check-site.sh` `HATA` satırı basarsa değişiklik itilmez; satır hangi ürün
satırının eksildiğini söyler — örneğin
`HATA: site/trino.yaml: ürün satırı eksik -> {"user": "superset|zeppelin|e2e", "privileges":
["SELECT"]} (accessControl.rules."rules.json" içinde bulunmalı)`. Eksik satırı
`platform/values/trino.yaml` içindeki bloktan kopyalayıp doğru diziye geri koyun.

### 5.3 Örnek: bir gruba satır filtresi ve kolon maskesi

`lakehouse-users` üyeleri `erp.invoices` tablosunda yalnız kendi bölgelerinin kayıtlarını
görecek ve `vergi_no` kolonu maskelenecek olsun. `tables` dizisinde genel
`{"group": "lakehouse-users", "privileges": ["SELECT"]}` satırının **üstüne** şu satır
eklenir:

```json
{"group": "lakehouse-users", "schema": "erp", "table": "invoices",
 "privileges": ["SELECT"], "filter": "bolge = 'IC ANADOLU'",
 "columns": [{"name": "vergi_no", "mask": "'***'"}]}
```

Kuralın yayılması: eşitleme bittikten sonra ConfigMap pod'a yansır (~1 dakika) ve Trino
dosyayı 60 saniyede bir yeniden okur → **toplam 2 dakikadan az**. Koordinatörü yeniden
başlatmak gerekmez.

### 5.4 Doğrulama

Kuralın gerçekten uygulandığını, o gruptaki bir kullanıcının **kendi kimliğiyle** yaptığı
sorguyla doğrulayın. En hızlı yol Trino'nun web arayüzüdür (`/ui`); not defterinden de
yapılabilir ([40-kurulum-sonrasi](../40-kurulum-sonrasi.md) §5.4).

`[pod]` — Trino web arayüzünde ya da OIDC destekli bir istemcide:

```sql
SHOW SCHEMAS FROM lakehouse;
SELECT count(*) FROM lakehouse.erp.invoices;
SELECT vergi_no FROM lakehouse.erp.invoices LIMIT 1;
```

**Beklenen sonuç:** şema listesi kullanıcının görebildikleriyle sınırlıdır; satır sayısı
filtrenin bıraktığı kadardır; maskeli kolon `***` döner.

**Ters giderse:** kural işlemiyorsa iki şeye bakın — (1) kullanıcının grubu Trino'ya ulaşıyor
mu (§4.1), (2) sizin kuralınızdan **önce** eşleşen daha genel bir kural var mı (§5.1). Sorgu
`Access Denied` ile düşüyorsa kullanıcının o katalogda hiç görünürlüğü yoktur.

---

## 6. Yazma alanı: `sandbox`

Analistlerin kendi tablolarını yaratabileceği tek şema `sandbox`'tır. Yazma **iki katmandan**
birden geçer:

1. **Polaris (katalog).** `platform/polaris/setup.yaml` içindeki katalog rolü yalnız
   `sandbox` ad alanında tablo yaratma/silme/okuma/yazma verir ve bunu `trino` ile
   `notebooks` kimliklerine bağlar. Yani **motor** ancak oraya yazabilir.
2. **Trino erişim kuralları.** `schemas` bölümünde `lakehouse-analysts` için `sandbox`
   sahipliği, `tables` bölümünde aynı grup için `sandbox` şemasında tam ayrıcalık.
   `lakehouse-users` bu kurallara düşmez → yazma reddedilir.

Analist ile kullanıcı ayrımı Trino'da, **motor sınırı** Polaris'tedir. Bronze ve Silver ad
alanlarına Trino üzerinden **hiç kimse** yazamaz; oraya yalnız veri akışının kendi kimlikleri
yazar. Yeni bir şemayı yazılabilir yapmak isterseniz **iki yeri birden** değiştirmeniz
gerekir; yalnız Trino kuralını değiştirmek katalogdan yetki almadığı için işe yaramaz.

**Veriyle birlikte silme.** `DROP TABLE` sırasında dosyaların da silinmesi katalog düzeyinde
açılır ve yalnız `lakehouse` kataloğunu kapsar. Var olan bir kurulumda ayar sonradan
eklenecekse katalog özelliği güncellenir ([40-kurulum-sonrasi](../40-kurulum-sonrasi.md)
§2.4). Kapsam ve riskler: [kaynak-veya-tablo-silme.md](kaynak-veya-tablo-silme.md) Adım 6 ve 7.

---

## 7. Servis hesapları — ve kimliğin taşınmadığı yer

Superset ve Zeppelin, Trino'ya **paylaşımlı servis hesabıyla** bağlanır. Hesaplar
`trino-service-accounts` Secret'ındadır
([90-referans/secret-listesi.md](../90-referans/secret-listesi.md)).

| Hesap | Kullanan | Yetki |
|---|---|---|
| `superset` | Superset'in Trino bağlantısı | yalnız `SELECT`; `sandbox`'a yazamaz |
| `zeppelin` | Zeppelin'in JDBC yorumlayıcısı | aynı |
| `e2e` | yalnız geliştirme ve kabul koşusu (üretimde yaratılmaz) | aynı |

**Doğrudan sonucu:** Superset ve Zeppelin son kullanıcının kimliğini Trino'ya **taşımaz** →
o araçlarda **satır filtresi ve kolon maskesi uygulanmaz**; servis hesabı maskesiz veriyi
görür. Satır ve kolon yetkilendirmesi isteniyorsa kullanıcı Trino'ya **kendi kimliğiyle**
bağlanmalıdır: Trino web arayüzü, OIDC destekli bir JDBC/CLI istemcisi ya da not defterinden
OIDC ile bağlanma ([40-kurulum-sonrasi](../40-kurulum-sonrasi.md) §5.6, birinci madde).

Bu nedenle Superset ve Zeppelin'de kullanıcılara açılan veri **araç düzeyinde**
sınırlandırılır: Superset'te `Gamma` rolü + veri kaynağı/şema izinleri, Zeppelin'de not
defteri paylaşım izinleri.

**Parola değiştirme.** Servis hesabı parolaları değiştirilirse Secret yeniden yaratılır;
Trino parola dosyasını kendiliğinden yeniden okur, ama Superset parolayı ortam değişkeninden
aldığı için yeniden başlatılmalıdır:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" rollout restart deploy/superset-web-server
```

**Beklenen çıktı** (örnek):

```text
deployment.apps/superset-web-server restarted
```

**Ters giderse:** Zeppelin'de parola değişmiyorsa yorumlayıcı ayarı diskten geliyordur;
Secret yalnız bir tohumdur ([sorun-giderme.md](sorun-giderme.md) §7).

---

## 8. Yeni bir grup açmak

Üç grubun dışında bir grup gerekiyorsa (ör. `lakehouse-muhasebe`) sıra şudur:

1. **AD'de** grubu açın ve üyeleri ekleyin. Grup adının **CN** değeri neyse Trino'nun
   göreceği ad odur; adı baştan doğru seçin.
2. **Keycloak'ta** ayrıca bir şey yapmayın: AD federasyonu grubu kendiliğinden getirir.
   (Realm dosyasına elle grup eklemek gerekiyorsa
   [30-kurulum](../30-kurulum.md) "Realm içeriğini sonradan değiştirmek" bölümündeki yordam
   uygulanır — içe aktarma var olan realm'i **güncellemez**.)
3. **Trino** kurallarını §5.2'deki yolla güncelleyin (tam blok).
4. Grup adı **üç yerde daha** geçer; gerekiyorsa onları da güncelleyin: Superset rol
   eşlemesi, JupyterHub izinli gruplar listesi, Zeppelin rol eşlemesi (§3).
5. Commit + push → eşitleme → §5.4 ile doğrulayın.

**Ters giderse:** kullanıcı yeni gruba eklendiği hâlde Trino'da görünmüyorsa AD grubunun CN
değeri ile kuraldaki ad birebir aynı değildir.

---

## 9. Realm ve jeton bakımı

- **Realm içeriği değişecekse** (yeni istemci, eşleyici, hedef kitle ayarı):
  [30-kurulum](../30-kurulum.md) "Realm içeriğini sonradan değiştirmek". İçe aktarma var olan
  realm'i **güncellemez**; yordam realm'i silip yeniden kurar ve **kullanıcıların Keycloak
  arayüzünde elle yaptığı her şey gider**. AD federasyonu varsa kullanıcılar yeniden akar.

- **JupyterHub oturum anahtarları.** `jupyterhub-secrets` Secret'ı değiştirilirse hub yeniden
  başlar ve **açık bütün not defteri oturumları düşer**; kullanıcılar yeniden giriş yapar,
  veri kaybı olmaz.

- **JupyterHub ara sunucu jetonunu yenileme — tuzak.** Hub'ın ürettiği `hub` Secret'ı silinip
  yeniden yaratılacaksa **yalnız eşitleme yetmez**; uygulamanın depoyu baştan okuması gerekir.
  Doğru sıra:

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" delete secret hub
  oc -n "$ARGOCD_NS" annotate application jupyterhub argocd.argoproj.io/refresh=hard --overwrite
  oc -n "$ARGOCD_NS" patch application jupyterhub --type merge -p '{"operation":{"sync":{}}}'
  ```

  **Beklenen çıktı** (örnek):

  ```text
  secret "hub" deleted
  application.argoproj.io/jupyterhub annotated
  application.argoproj.io/jupyterhub patched
  ```

  **Ters giderse:** sert yenileme açıklaması yazılmadan yalnız eşitleme yapılırsa Secret
  yeniden üretilmez ve hub ara sunucuya bağlanamaz.

---

## Kontrol listesi

- [ ] Kullanıcı ekleme ve çıkarma işi **AD'de** yapıldı; üründe kullanıcı yaratılmadı.
- [ ] Grup adları üç yerde de (AD, Keycloak, Trino) birebir aynı.
- [ ] Yeni kural, genel `SELECT` kuralından **önce** yazıldı.
- [ ] `platform/values/site/trino.yaml` içindeki blok ürün satırlarını da taşıyor;
      `scripts/check-site.sh` `OK` bastı.
- [ ] Kural, ilgili gruptaki bir kullanıcının kendi kimliğiyle doğrulandı.
- [ ] Superset ve Zeppelin'de satır/kolon kısıtının **uygulanmadığı** ekipçe biliniyor; o
      araçlarda sınırlama araç düzeyinde yapıldı.
- [ ] Yazma yalnız `sandbox` şemasında; başka bir şema açıldıysa Polaris tarafı da
      güncellendi.

## Sonraki bölüm

Yetki hatalarının belirti tablosu: [sorun-giderme.md](sorun-giderme.md) §7. Yedek ve geri
dönüş: [yedek-ve-geri-donus.md](yedek-ve-geri-donus.md). Ortak GitOps döngüsü her zaman
aynıdır: [değişiklik nasıl uygulanır](degisiklik-nasil-uygulanir.md).
