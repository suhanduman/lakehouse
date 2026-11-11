# Kullanıcı ve yetki

Bu sayfa, kimin hangi veriyi görebileceğinin nerede belirlendiğini anlatır. Yol Active
Directory'deki gruptan başlar; Keycloak, Trino, Superset, JupyterHub ve Zeppelin bu
gruba göre davranır. Sayfada kullanıcı ekleme ve çıkarma, yeni grup açma, satır filtresi
ve kolon maskesi yazma, yazılabilir alan (`sandbox`) ve paylaşımlı servis hesapları
anlatılır.

**Ne kadar sürer:** var olan bir gruba kullanıcı eklemek birkaç dakika (AD'deki iş).
Yeni bir erişim kuralı yazmak 15–30 dakika (düzenleme, uygulama, doğrulama).

**Hangi yetkiler gerekir:** AD'de grup üyeliği düzenleme (kimlik ekibi), kurumun
lakehouse Git deposunda `main` dalına yazma, `$LAKEHOUSE_NS` ad alanında okuma.

**Komutlar nerede çalışır:**

- `[bastion]` — `git` ve `oc` kurulu, `oc login` ile kümeye bağlı yönetim makinesi
- `[pod]` — Trino'nun web arayüzü ya da kendi kimliğinizle bağlanan bir SQL istemcisi

> **Yetkiyi kümede elle vermeyin.** Bütün erişim kuralları Git'teki ayar
> dosyalarındadır. `oc edit` ile yapılan bir değişikliği ArgoCD birkaç dakika içinde
> geri alır ([değişiklik nasıl uygulanır](degisiklik-nasil-uygulanir.md)).

---

## 1. Değişkenleri yükleyin

Her yeni terminalde bir kez çalıştırın.

`[bastion]`

```bash
cd ~/lakehouse
set -a; . install/lakehouse.env; set +a
echo "lakehouse=$LAKEHOUSE_NS grup agaci=$LDAP_GROUPS_DN"
```

**Beklenen çıktı** (kendi değerlerinizle):

```text
lakehouse=lakehouse grup agaci=OU=Groups,DC=example,DC=com
```

**Ters giderse:** ikinci değer boşsa `install/lakehouse.env` doldurulmamıştır
([30-kurulum](../30-kurulum.md) §1).

---

## 2. Tek kimlik kaynağı: Active Directory

Kullanıcı hesapları ve grup üyelikleri **yalnız AD'de** tutulur. Lakehouse'un kendi
kullanıcı listesi yoktur. Her bileşen grubu farklı bir yoldan öğrenir ama grup adı
hepsinde aynıdır.

- Üç grup vardır ve adları değişmez: `lakehouse-admins`, `lakehouse-analysts`,
  `lakehouse-users` ([20-on-kosullar](../20-on-kosullar.md) madde 6).
- Aynı ad AD'de, Keycloak'ta ve Trino'da kullanılır.

| Bileşen | Kullanıcı nereden giriş yapar | Grubunu nereden öğrenir |
|---|---|---|
| Trino | Keycloak (tarayıcı) ya da servis hesabı (kullanıcı adı + parola) | doğrudan AD'den (LDAP grup sorgusu) |
| Superset | Keycloak | Keycloak'ın gönderdiği grup listesi |
| JupyterHub | Keycloak | Keycloak'ın gönderdiği grup listesi |
| Zeppelin | doğrudan AD (LDAPS) | AD grup üyeliği |

Zeppelin'in Keycloak kullanmaması bilinçlidir: kullanılan Zeppelin sürümü Keycloak'la
(OIDC) girişi desteklemez. Sonuç aynıdır; aynı üç grup aynı rolleri verir.

Trino, AD'ye bağlanırken AD'nin sertifikasını Java'nın güvendiği sertifikalar listesiyle
doğrular. AD sertifikası kurumun kendi CA'sından geliyorsa bu CA kurulumda listeye
eklenir ([30-kurulum](../30-kurulum.md) §5.13).

---

## 3. Grup ve rol tablosu

| | `lakehouse-admins` | `lakehouse-analysts` | `lakehouse-users` |
|---|---|---|---|
| **Trino — katalog** | tam erişim | tam erişim | yalnız okuma |
| **Trino — şema** | her şemada tablo yaratabilir ve silebilir | yalnız `sandbox` şemasında tablo yaratabilir ve silebilir | tablo yaratamaz |
| **Trino — tablo** | okuma, yazma, silme | `sandbox`'ta her şey; diğer şemalarda okuma | yalnız okuma; tanımlıysa satır filtresi ve kolon maskesiyle |
| **Superset** | `Admin` | `Alpha` (kendi grafik ve panolarını yaratır) | `Gamma` (yalnız kendisine açılanı görür) |
| **JupyterHub** | giriş ve hub yöneticiliği | giriş | giriş |
| **Zeppelin** | `admin` rolü | `analyst` rolü | `user` rolü |

Bu eşlemeler ürünün varsayılanıdır. Grup adları değişmedikçe dokunmanız gerekmez.

**Hiçbir gruptan olmayan biri ne görür?**

- **Zeppelin:** hiçbir şey. Giriş yapsa bile not defteri listesini alamaz.
- **Trino:** hiçbir katalog görünmez.
- **Superset:** hesap açılır ama hiçbir yetkisi olmayan boş bir arayüz görür. Bu
  bilinçlidir: Superset'te ilk girişte hesap açılması kapatılırsa yöneticiler de
  giremez. Grubu olmayan birinin Superset'e hiç girmemesi isteniyorsa bu, Keycloak'ta
  `superset` istemcisine grup zorunluluğu eklenerek yapılır.

---

## 4. Kullanıcı ekleme ve çıkarma

**Kullanıcı eklemek, AD'deki gruba üye eklemektir.** Lakehouse'ta yapılacak bir şey
yoktur: kullanıcı yaratılmaz, parola verilmez, kural yazılmaz.

1. Kimlik ekibi kullanıcıyı gruplardan birine ekler: `lakehouse-admins`,
   `lakehouse-analysts` ya da `lakehouse-users`.
2. Kullanıcı arayüzlerden birine girer; hesabı ilk girişte kendiliğinden açılır.
3. Rol her girişte grup üyeliğine göre yeniden belirlenir. Gruptan çıkarılan kullanıcı
   bir sonraki girişinde yetkisini kaybeder.

Üyeliği AD'de görmek için (komut bağlanma hesabının parolasını sorar):

`[bastion]`

```bash
ldapsearch -LLL -H "$LDAP_URL" -D "$LDAP_BIND_DN" -W \
  -b "$LDAP_GROUPS_DN" "(cn=lakehouse-analysts)" member
```

**Beklenen çıktı** (örnek; adlar kurumunuzun AD'sine göre değişir):

```text
dn: CN=lakehouse-analysts,OU=Groups,DC=example,DC=com
member: CN=Ali Analist,OU=Users,DC=example,DC=com
```

**Ters giderse:** `ldapsearch` bağlanamıyorsa bağlanma hesabını ya da AD'nin LDAPS
sertifikasını kontrol edin ([20-on-kosullar](../20-on-kosullar.md) madde 6).

### 4.1 Değişiklik ne zaman geçerli olur

| Bileşen | Geçerli olduğu an |
|---|---|
| Superset, JupyterHub, Zeppelin | kullanıcının bir sonraki girişinde |
| Trino | kullanıcının bir sonraki sorgusunda; ayrıca bir işlem gerekmez |

Trino grupları AD'den doğrudan okur; AD'deki üyelik değişikliği başka bir adım
gerektirmez. Erişim **kuralları** (§5) da yeniden başlatma gerektirmez: Trino onları
dakikada bir yeniden okur.

---

## 5. Trino erişim kuralları

Kimin hangi tabloyu nasıl görebileceği tek bir kural listesinde yazılıdır. Ürün bu
listenin bir varsayılanıyla gelir; kurumunuza özel kurallar
`platform/values/site/trino.yaml` dosyasına yazılır (§5.2).

### 5.1 Kural listesinin üç bölümü

- **`catalogs`** — hangi grup hangi kataloğu görür: `all` (tam), `read-only` (yalnız
  okuma) ya da `none` (görmez).
- **`schemas`** — hangi grup hangi şemada tablo yaratıp silebilir (`owner: true`).
- **`tables`** — hangi grup hangi tabloda neyi yapabilir; ayrıca **satır filtresi** ve
  **kolon maskesi**.

**Her bölümde ilk eşleşen kural geçerlidir.** Bu yüzden belirli bir gruba ya da tabloya
özel kurallar, genel `SELECT` kuralından **önce** yazılır. Sıra yanlışsa kural sessizce
işlemez; hata mesajı da çıkmaz.

- `group` ve `user` alanları `|` ile birden fazla ad alabilir:
  `"lakehouse-admins|lakehouse-analysts"`. `user` alanı servis hesapları içindir (§7).
- **Satır filtresi:** `"filter": "status <> 'shipped'"` — kurala uyan kullanıcının o
  tablodaki her sorgusuna bu koşul eklenir.
- **Kolon maskesi:** `"columns": [{"name": "remote", "mask": "'x.x.x.x'"}]` — kolon
  yerine bu ifadenin değeri döner. İfade kolonun tipine uygun olmalıdır.

Ürünün varsayılan kurallarında iki örnek vardır: `lakehouse-users` grubu `shop.orders`
tablosunda gönderilmiş siparişleri görmez (satır filtresi) ve nginx erişim günlüğünde
ziyaretçi IP adresini (`remote`) maskeli görür. Analistler IP adresini açık görür.

Kümede geçerli olan kuralları görmek için:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" get configmap trino-access-control-volume-coordinator \
  -o jsonpath='{.data.rules\.json}' | head -20
```

**Beklenen çıktı** (ürün varsayılanının ilk 20 satırı):

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

**Ters giderse:** `NotFound` görürseniz Trino uygulaması henüz kurulmamıştır
([değişiklik nasıl uygulanır](degisiklik-nasil-uygulanir.md) §5).

### 5.2 Kural eklemek: listenin tamamı site dosyasına taşınır

Kural listesi tek parça bir metindir. Site dosyasına yazılan liste, ürünün listesinin
**yerine geçer**; ikisi birleştirilmez. Bu yüzden kendi kuralınızı eklemek için ürünün
bütün kurallarını da site dosyasına taşırsınız.

**İlk kez kural ekliyorsanız** (site dosyasında henüz `accessControl:` yoksa), kümede
geçerli olan listenin tamamını doğru girintiyle site dosyasının sonuna ekleyin:

`[bastion]`

```bash
grep -c '^accessControl:' platform/values/site/trino.yaml
```

Bu komut `0` yazıyorsa şunları çalıştırın:

```bash
{
  echo "accessControl:"
  echo "  rules:"
  echo "    rules.json: |-"
  oc -n "$LAKEHOUSE_NS" get configmap trino-access-control-volume-coordinator \
    -o jsonpath='{.data.rules\.json}' | sed 's/^/      /'
  echo
} >> platform/values/site/trino.yaml
tail -5 platform/values/site/trino.yaml
```

**Beklenen çıktı** (`tail` komutu: eklenen listenin son beş satırı):

```text
          {"group": "lakehouse-users", "privileges": ["SELECT"]},
          {"user": "superset|zeppelin|e2e", "privileges": ["SELECT"]},
          {"catalog": "system", "privileges": ["SELECT"]}
        ]
      }
```

`1` yazıyorsa liste daha önce eklenmiştir; doğrudan bir sonraki adıma geçin.

**Kuralınızı ekleyin.** Dosyayı açın:

```bash
vi platform/values/site/trino.yaml
```

`accessControl:` altındaki listede kendi kuralınızı doğru yere ekleyin: özel kurallar
genel `SELECT` kuralından **önce** (örnek §5.3'te). Satırların başındaki boşluk,
listedeki diğer satırlarla aynı olmalıdır. Kaydedin ve denetleyin:

```bash
bash scripts/check-site.sh
```

**Beklenen çıktı:**

```text
check-site: OK
```

Betik, ürünün servis hesabı kurallarının listede kalıp kalmadığına bakar
(`{"user": "superset|zeppelin|e2e", …}` ile başlayan iki satır). Bu satırlar silinirse
Superset panoları ve Zeppelin not defterleri Trino'dan sessizce `Access Denied` alır.

**Gönderin:**

```bash
git diff
git add platform/values/site/trino.yaml
git commit -m "site: erp.invoices için bölge filtresi"
git push origin main
```

**Ters giderse:** `check-site.sh` `HATA` yazarsa göndermeyin. Satır hangi ürün kuralının
eksik olduğunu söyler, örneğin:

```text
HATA: site/trino.yaml: ürün satırı eksik -> {"user": "superset|zeppelin|e2e", "privileges": ["SELECT"]} (accessControl.rules."rules.json" içinde bulunmalı)
```

Eksik satırı §5.1'deki komutun çıktısından kopyalayıp listede doğru bölüme geri koyun.

### 5.3 Örnek: bir gruba satır filtresi ve kolon maskesi

`lakehouse-users` üyeleri `erp.invoices` tablosunda yalnız kendi bölgelerinin
kayıtlarını görsün ve `vergi_no` kolonu maskeli dönsün. `tables` bölümünde,
`{"group": "lakehouse-users", "privileges": ["SELECT"]},` satırının **hemen üstüne**
şu kural eklenir:

```json
{"group": "lakehouse-users", "schema": "erp", "table": "invoices",
 "privileges": ["SELECT"], "filter": "bolge = 'IC ANADOLU'",
 "columns": [{"name": "vergi_no", "mask": "'***'"}]},
```

Gönderdikten sonra kural 2 dakikadan kısa sürede geçerli olur: ArgoCD değişikliği
uygular, Trino listeyi dakikada bir yeniden okur. Trino'yu yeniden başlatmanız gerekmez.

### 5.4 Doğrulama

Kuralı, o gruptaki bir kullanıcının **kendi kimliğiyle** yaptığı sorgularla doğrulayın.
En kolay yol Trino'nun web arayüzüdür (Trino adresinin sonuna `/ui` eklenir); not
defterinden de yapılabilir ([40-kurulum-sonrasi](../40-kurulum-sonrasi.md) §5.4).

`[pod]`

```sql
SHOW SCHEMAS FROM lakehouse;
SELECT count(*) FROM lakehouse.erp.invoices;
SELECT vergi_no FROM lakehouse.erp.invoices LIMIT 1;
```

**Beklenen sonuç:**

- şema listesinde yalnız kullanıcının görebildiği şemalar vardır;
- satır sayısı yalnız filtreye uyan kayıtlardır;
- `vergi_no` kolonu `***` olarak döner.

**Ters giderse:**

- Kural işlemiyorsa sizin kuralınızdan **önce** eşleşen daha genel bir kural olup
  olmadığına bakın (§5.1).
- Kullanıcının AD grubunu §4'teki komutla doğrulayın.
- Sorgu `Access Denied` ile düşüyorsa kullanıcının o kataloğu görme yetkisi yoktur.

---

## 6. Yazılabilir alan: `sandbox`

Analistlerin kendi tablolarını yaratabildiği tek şema `sandbox`'tır. Yazma iki ayrı
kapıdan geçer:

1. **Katalog (Polaris):** Trino ve not defterleri yalnız `sandbox` ad alanına
   yazabilir. Bu, `platform/polaris/setup.yaml` dosyasındaki `lakehouse_sandbox`
   rolüyle belirlenir.
2. **Trino kuralları:** `sandbox` şemasında tablo yaratma ve yazma yalnız
   `lakehouse-analysts` grubuna (ve yöneticilere) verilmiştir. `lakehouse-users` grubu
   yazamaz.

Bronze ve Silver tablolarına Trino üzerinden **hiç kimse** yazamaz; onlara yalnız veri
akışının kendi bileşenleri yazar. Başka bir şemayı yazılabilir yapmak istiyorsanız
**iki yeri birden** değiştirmeniz gerekir: Polaris rolünü ve Trino kuralını. Yalnız
Trino kuralını değiştirmek yetmez; katalog yazmayı yine reddeder.

**Tablo silindiğinde dosyalar da silinir.** `sandbox`'ta `DROP TABLE`, tablonun
S3'teki dosyalarını da kaldırır. Ayrıntılar:
[kaynak-veya-tablo-silme.md](kaynak-veya-tablo-silme.md) Adım 6 ve 7.

---

## 7. Servis hesapları ve kimliğin taşınmadığı yer

Superset ve Zeppelin, Trino'ya **paylaşımlı bir servis hesabıyla** bağlanır. Hesapların
parolaları `trino-service-accounts` Secret'ındadır
([90-referans/secret-listesi.md](../90-referans/secret-listesi.md)).

| Hesap | Kullanan | Yetki |
|---|---|---|
| `superset` | Superset'in Trino bağlantısı | yalnız okuma; `sandbox`'a yazamaz |
| `zeppelin` | Zeppelin'in Trino bağlantısı | yalnız okuma |
| `e2e` | kabul testi | yalnız okuma |

**Bunun sonucu:** Superset ve Zeppelin, ekrandaki kullanıcının kimliğini Trino'ya
taşımaz. Bu yüzden bu iki araçta **satır filtresi ve kolon maskesi uygulanmaz**; servis
hesabı veriyi maskesiz görür. Satır ve kolon kuralları isteniyorsa kullanıcı Trino'ya
**kendi kimliğiyle** bağlanır: Trino web arayüzü, Keycloak destekli bir SQL istemcisi ya
da not defteri ([40-kurulum-sonrasi](../40-kurulum-sonrasi.md) §5.6).

Superset ve Zeppelin'de kullanıcının ne göreceği bu yüzden **araç içinde** sınırlanır:
Superset'te `Gamma` rolü ve veri seti izinleriyle, Zeppelin'de not defteri paylaşım
izinleriyle.

**Servis hesabı parolasını değiştirmek.** Secret yeni parolayla yeniden yaratılır
([30-kurulum](../30-kurulum.md) §5.7). Trino yeni parolayı kendiliğinden okur. Superset
parolayı açılışta okuduğu için yeniden başlatılır:

`[bastion]`

```bash
oc -n "$LAKEHOUSE_NS" rollout restart deploy/superset-web-server
```

**Beklenen çıktı:**

```text
deployment.apps/superset-web-server restarted
```

**Ters giderse:** Zeppelin yeni parolayı kullanmıyorsa yorumlayıcı ayarı Zeppelin'in
kendi diskinden okunuyordur; Secret yalnız ilk kurulumda kullanılır
([sorun-giderme.md](sorun-giderme.md) §7).

---

## 8. Yeni bir grup açmak

Üç grubun dışında bir grup gerekiyorsa (ör. `lakehouse-muhasebe`) sıra şudur:

1. **AD'de** grubu açın ve üyeleri ekleyin. Grubun **CN** değeri, Trino'nun göreceği
   addır; adı baştan doğru seçin.
2. **Keycloak'ta** bir şey yapmayın: AD bağlantısı grubu kendiliğinden getirir.
3. **Trino** kurallarına grubu §5.2'deki yolla ekleyin.
4. Grubun Superset, JupyterHub ya da Zeppelin'de de bir rolü olacaksa bu eşlemeler de
   güncellenir (§3). Bu eşlemeler ürün varsayılanıdır; değiştirmeden önce ürün ekibine
   danışın.
5. Gönderin, ArgoCD'nin uygulamasını bekleyin ve §5.4 ile doğrulayın.

**Ters giderse:** kullanıcı yeni gruba eklendiği hâlde Trino'da kural işlemiyorsa AD
grubunun CN değeri ile kuraldaki ad birebir aynı değildir (büyük-küçük harf dâhil).

---

## 9. Keycloak ve JupyterHub bakımı

- **Keycloak yapılandırması değişecekse** (yeni istemci, eşleyici, hedef kitle ayarı):
  [30-kurulum](../30-kurulum.md) §8. O yordam Keycloak'taki lakehouse yapılandırmasını
  silip yeniden kurar; **kullanıcıların Keycloak arayüzünde elle yaptığı her şey gider**.
  Kullanıcılar ve gruplar AD'den yeniden gelir.

- **JupyterHub oturum anahtarları.** `jupyterhub-secrets` Secret'ı değiştirilirse hub
  yeniden başlar ve **açık bütün not defteri oturumları kapanır**. Kullanıcılar yeniden
  giriş yapar; veri kaybolmaz.

- **JupyterHub'ın iç jetonunu yenilemek.** Hub'ın kendi ürettiği `hub` Secret'ı silinip
  yeniden üretilecekse ArgoCD'nin depoyu baştan okuması gerekir. Sıra:

  `[bastion]`

  ```bash
  oc -n "$LAKEHOUSE_NS" delete secret hub
  oc -n "$ARGOCD_NS" annotate application jupyterhub argocd.argoproj.io/refresh=hard --overwrite
  oc -n "$ARGOCD_NS" patch application jupyterhub --type merge -p '{"operation":{"sync":{}}}'
  ```

  **Beklenen çıktı** (örnek):

  ```text
  secret "hub" deleted from lakehouse namespace
  application.argoproj.io/jupyterhub annotated
  application.argoproj.io/jupyterhub patched
  ```

  **Ters giderse:** `refresh=hard` yazılmadan yalnız eşitleme yapılırsa Secret yeniden
  üretilmez ve hub açılamaz. Üç komutu sırayla tekrar çalıştırın.

---

## Kontrol listesi

- [ ] Kullanıcı ekleme ve çıkarma AD'de yapıldı; lakehouse'ta kullanıcı yaratılmadı.
- [ ] Grup adları AD'de, Keycloak'ta ve Trino kurallarında birebir aynı.
- [ ] Yeni kural, genel `SELECT` kuralından önce yazıldı.
- [ ] Site dosyasındaki kural listesi ürünün kurallarını da taşıyor;
      `scripts/check-site.sh` `check-site: OK` yazdı.
- [ ] Kural, ilgili gruptaki bir kullanıcının kendi kimliğiyle doğrulandı.
- [ ] Ekip, Superset ve Zeppelin'de satır/kolon kurallarının uygulanmadığını ve
      sınırlamanın araç içinde yapıldığını biliyor.
- [ ] Yazma yalnız `sandbox`'ta; başka bir şema açıldıysa Polaris rolü de güncellendi.

## Sonraki bölüm

Yetki hatalarının belirtileri ve çözümleri: [sorun-giderme.md](sorun-giderme.md) §7.
Yedek ve geri dönüş: [yedek-ve-geri-donus.md](yedek-ve-geri-donus.md). Her ayar
değişikliği aynı yoldan geçer: [değişiklik nasıl uygulanır](degisiklik-nasil-uygulanir.md).
