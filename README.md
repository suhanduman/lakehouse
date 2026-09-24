# lakehouse

Kurumun PostgreSQL, SQL Server, MongoDB ve nginx verisini sürekli olarak
açık formatlı bir veri gölüne (Apache Iceberg) taşıyan ve bu veriyi Active
Directory kimlikleriyle SQL, pano ve not defteri üzerinden sorgulatan,
kod yazmadan yalnız değer dosyalarıyla yönetilen deklaratif platform.

## Akışlar

```text
CDC (PostgreSQL/SQL Server)  kaynak DB -> Debezium -> Kafka -> Iceberg sink -> Bronze
MongoDB                      kaynak DB -> Debezium -> Kafka -> Spark -> Bronze
nginx erişim günlüğü         web sunucusu -> Fluent Bit -> Kafka -> Iceberg sink -> Bronze
Silver ve bakım              Bronze -> silver-merge (15 dk) -> Silver -> compaction/expire
Kimlik                       Active Directory -> Keycloak -> Trino/Superset/JupyterHub
Yedek                        PostgreSQL -> Barman -> S3 yedek bucket | ad alanı -> OADP
İzleme                       Kafka/Connect/Spark -> OpenShift UWM -> 5 alarm
```

Ayrıntılı diyagramlar ve sözlük: [docs/00-genel-bakis.md](docs/00-genel-bakis.md).

## Nereden başlayayım

- **Kurulumcu** — sıfırdan kurmak:
  [docs/00-genel-bakis.md](docs/00-genel-bakis.md) →
  [docs/10-planlama.md](docs/10-planlama.md) →
  [docs/20-on-kosullar.md](docs/20-on-kosullar.md) →
  [docs/30-kurulum.md](docs/30-kurulum.md)
- **İşletmen** — kaynak/tablo eklemek, yetki vermek, yedek almak:
  docs/50-isletme/ altındaki kılavuzlar
- **Analist** — ilk giriş, sorgu ve pano:
  [docs/40-kurulum-sonrasi.md](docs/40-kurulum-sonrasi.md)
- **Geliştirici** — kendi Spark uygulaması/CronJob'ı:
  docs/50-isletme/yeni-spark-uygulamasi.md

## Bileşenler ve sürümler

- **Alım:** Kafka 4.3.1 (Strimzi 1.2.0) + Debezium 3.6.2.Final + Iceberg 1.11.0 sink
- **Katalog/depolama:** Polaris 1.7.0 + müşterinin S3'ü; CloudNativePG 1.30.0
- **İşleme:** Spark 4.1.0 (spark-operator 2.5.2) — Silver birleştirme ve bakım
- **Sunum:** Trino 483, Superset 6.1.0, JupyterHub 5.5.2, Zeppelin 0.12.1
- **Kimlik/işletim:** Keycloak 26.7.3 (AD), ArgoCD (OpenShift GitOps), OADP, UWM

## Lisans

Ürün yalnız açık kaynak bileşenlerden oluşur; özel konteyner imajı yoktur.
Bileşenlerin çoğu Apache-2.0'dır; JupyterHub ve not defteri imajı
BSD-3-Clause, CNPG'nin PostgreSQL imajı PostgreSQL License, yalnız
geliştirme kurulumundaki Grafana ise AGPL-3.0'dır. Bileşen başına eşleme:
[docs/90-referans/surumler-ve-lisanslar.md](docs/90-referans/surumler-ve-lisanslar.md).

## Dokümantasyon

Kitap baştan sona okunacak sırada numaralanmıştır.

- [docs/00-genel-bakis.md](docs/00-genel-bakis.md) — bileşenler, yedi veri
  akışı, kavramlar sözlüğü
- [docs/10-planlama.md](docs/10-planlama.md) — mimari kararlar,
  boyutlandırma, ağ/portlar, değerler çalışma sayfası, sorumluluklar
- [docs/20-on-kosullar.md](docs/20-on-kosullar.md) — OpenShift, GitOps,
  StorageClass, S3, registry,
  Active Directory, DNS, sertifika
- [docs/30-kurulum.md](docs/30-kurulum.md) — depoyu kopyalama, site
  değerleri, Secret'lar, bootstrap, doğrulama
- [docs/40-kurulum-sonrasi.md](docs/40-kurulum-sonrasi.md) — Polaris kataloğu,
  Superset içe aktarma, ilk giriş, izleme hedefleri, kabul testi
- docs/50-isletme/ — gün-2: değişiklik uygulama, kaynak/tablo ekleme ve
  silme, kullanıcı ve yetki, yedek ve geri dönüş, yükseltme, izleme ve
  alarmlar, veri metrikleri, günlük/haftalık kontroller, sorun giderme
- docs/90-referans/ — [değer anahtarları](docs/90-referans/values-anahtarlari.md),
  [Secret listesi](docs/90-referans/secret-listesi.md),
  [port ve servisler](docs/90-referans/port-ve-servisler.md),
  [sürümler ve lisanslar](docs/90-referans/surumler-ve-lisanslar.md),
  [kabul testleri](docs/90-referans/kabul-testleri.md),
  [oc hızlı başvuru](docs/90-referans/oc-hizli-basvuru.md)

50 klasörü sonraki görevlerde eklenir.
Kurulum değişkenleri şablonu: `install/lakehouse.env.example`.
