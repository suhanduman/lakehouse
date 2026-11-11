# lakehouse

Kurumun PostgreSQL, SQL Server ve MongoDB veritabanlarındaki değişiklikleri, web
sunucularının erişim günlüklerini ve uygulamaların JSON olaylarını sürekli olarak açık
formatlı bir veri gölüne (Apache Iceberg) taşıyan platform. Veri, Active Directory
kimlikleriyle SQL, pano ve not defterlerinden sorgulanır. Platform kod yazmadan, Git'teki
ayar dosyalarıyla yönetilir.

## Veri nasıl akar

```text
PostgreSQL / SQL Server   kaynak DB -> Debezium -> Kafka -> Iceberg yazıcısı -> Bronze
MongoDB                   kaynak DB -> Debezium -> Kafka -> Spark -> Bronze
nginx erişim günlüğü      web sunucusu -> Fluent Bit -> Kafka -> Iceberg yazıcısı -> Bronze
JSON olayları (IoT)       uygulama -> Kafka -> Iceberg yazıcısı -> Bronze
Silver ve bakım           Bronze -> silver-merge (15 dk) -> Silver -> sıkıştırma, eski sürümleri silme
Kimlik                    Active Directory -> Keycloak -> Trino / Superset / JupyterHub / Zeppelin
Yedek                     PostgreSQL -> Barman -> S3 yedek bucket'ı | ad alanı -> OADP
İzleme                    Kafka / Connect / Spark -> OpenShift izleme -> 6 alarm
```

Ayrıntılı anlatım ve kavramlar: [docs/00-genel-bakis.md](docs/00-genel-bakis.md).

## Nereden başlamalı

- **Kurulumu yapacaksanız:**
  [genel bakış](docs/00-genel-bakis.md) →
  [planlama](docs/10-planlama.md) →
  [ön koşullar](docs/20-on-kosullar.md) →
  [kurulum](docs/30-kurulum.md) →
  [kurulum sonrası](docs/40-kurulum-sonrasi.md)
- **Platformu işletecekseniz** (değişiklik yapmak, kaynak ve tablo eklemek):
  [değişiklik nasıl uygulanır](docs/50-isletme/degisiklik-nasil-uygulanir.md) →
  [yeni kaynak ve pipeline](docs/50-isletme/yeni-kaynak-ve-pipeline.md) →
  [mevcut kaynağa tablo ekleme](docs/50-isletme/mevcut-kaynaga-tablo-ekleme.md) →
  [JSON olay akışı ekleme](docs/50-isletme/json-akisi-ekleme.md)
- **Veriyi sorgulayacaksanız:** ilk giriş ve araçlar için
  [kurulum sonrası](docs/40-kurulum-sonrasi.md); tabloların geçmişi için
  [veri metrikleri](docs/50-isletme/veri-metrikleri.md) §5.
- **Kendi işinizi yazacaksanız** (Spark işi, Gold tabloları):
  [yeni Spark uygulaması](docs/50-isletme/yeni-spark-uygulamasi.md).

## Bileşenler ve sürümler

- **Veri alımı:** Kafka 4.3.1 (Strimzi 1.2.0), Debezium 3.6.2.Final, Iceberg 1.11.0 yazıcısı
- **Katalog ve depolama:** Polaris 1.7.0, kurumun S3'ü; CloudNativePG 1.30.0
- **İşleme:** Spark 4.1.0 (spark-operator 2.5.2) — Silver birleştirme ve bakım; Spark
  kütüphaneleri için küme içi Maven önbelleği (Reposilite 3.6.4)
- **Sorgu ve sunum:** Trino 483, Superset 6.1.0, JupyterHub 5.5.2, Zeppelin 0.12.1
- **Kimlik ve işletim:** Keycloak 26.7.3 (Active Directory), ArgoCD (OpenShift GitOps),
  OADP, OpenShift kullanıcı iş yükü izleme

## Lisans

Platform yalnız açık kaynak bileşenlerden oluşur ve özel konteyner imajı kullanmaz.
Bileşenlerin çoğu Apache-2.0 lisanslıdır; JupyterHub ve not defteri imajı BSD-3-Clause,
CloudNativePG'nin PostgreSQL imajı PostgreSQL License ile gelir. Bileşen bileşen liste:
[docs/90-referans/surumler-ve-lisanslar.md](docs/90-referans/surumler-ve-lisanslar.md).

## Belgeler

Belgeler baştan sona okunacak sırada numaralanmıştır.

- [docs/00-genel-bakis.md](docs/00-genel-bakis.md) — bileşenler, veri akışları, kavramlar
- [docs/10-planlama.md](docs/10-planlama.md) — kararlar, boyutlandırma, ağ ve portlar,
  toplanacak değerler, sorumluluklar
- [docs/20-on-kosullar.md](docs/20-on-kosullar.md) — OpenShift, GitOps, depolama, S3,
  registry, Active Directory, DNS ve sertifika
- [docs/30-kurulum.md](docs/30-kurulum.md) — depoyu kopyalama, site ayarları, Secret'lar,
  kurulumu başlatma ve doğrulama
- [docs/40-kurulum-sonrasi.md](docs/40-kurulum-sonrasi.md) — katalog kurulumu, panolar,
  ilk giriş, izleme, kabul testi
- İşletme (`docs/50-isletme/`):
  [değişiklik nasıl uygulanır](docs/50-isletme/degisiklik-nasil-uygulanir.md),
  [yeni kaynak ve pipeline](docs/50-isletme/yeni-kaynak-ve-pipeline.md),
  [mevcut kaynağa tablo ekleme](docs/50-isletme/mevcut-kaynaga-tablo-ekleme.md),
  [kaynak ya da tablo kaldırma](docs/50-isletme/kaynak-veya-tablo-silme.md),
  [JSON olay akışı ekleme](docs/50-isletme/json-akisi-ekleme.md),
  [yeni Spark uygulaması](docs/50-isletme/yeni-spark-uygulamasi.md),
  [kullanıcı ve yetki](docs/50-isletme/kullanici-ve-yetki.md),
  [veri metrikleri](docs/50-isletme/veri-metrikleri.md),
  [izleme ve alarmlar](docs/50-isletme/izleme-ve-alarmlar.md),
  [günlük ve haftalık kontroller](docs/50-isletme/gunluk-haftalik-kontroller.md),
  [sorun giderme](docs/50-isletme/sorun-giderme.md),
  [yedek ve geri dönüş](docs/50-isletme/yedek-ve-geri-donus.md),
  [yükseltme](docs/50-isletme/yukseltme.md)
- Başvuru (`docs/90-referans/`):
  [ayar anahtarları](docs/90-referans/values-anahtarlari.md),
  [Secret listesi](docs/90-referans/secret-listesi.md),
  [port ve servisler](docs/90-referans/port-ve-servisler.md),
  [sürümler ve lisanslar](docs/90-referans/surumler-ve-lisanslar.md),
  [kabul testleri](docs/90-referans/kabul-testleri.md),
  [oc hızlı başvuru](docs/90-referans/oc-hizli-basvuru.md),
  [teslim öncesi kontrol listesi](docs/90-referans/pre-ship-kontrol-listesi.md)

Kurulum değişkenlerinin şablonu: `install/lakehouse.env.example`.
