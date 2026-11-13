"""nginx erişim günlüklerinden Gold özet tabloları.

Kaynak: nginx_raw.access_log (Fluent Bit -> Kafka -> Iceberg; her istek bir satır, yalnız eklenir).
Üretilen tablolar (üçü de `gun` kolonuna göre bölümlenir; gün ve saat Türkiye saatiyle):
  gold.nginx_saatlik       saat ve sunucu bazında istek, durum sınıfları (2xx-5xx), tekil IP, bayt, bot payı
  gold.nginx_ip_gunluk     gün ve IP bazında istek, 4xx/5xx, başarısız giriş (401), yasak (403), bulunamadı (404),
                           farklı sayfa sayısı, ilk ve son görülme; tarama ve kaba kuvvet denemelerini bulmak için
  gold.nginx_sayfa_gunluk  gün ve sayfa bazında istek ve hata; sayılar {id} ile gruplanır (/ogrenci/123 -> /ogrenci/{id})

Çalışma şekli: tablolar yoksa bütün geçmiş hesaplanır. Varsa son GERI_GUN günü (varsayılan 3) yeniden hesaplanır ve
yalnız o günlerin bölümleri değiştirilir (geç gelen satırlar da böylece sayılır). İş tekrar koşturulabilir.

Not: access.log'daki "hata" HTTP durum kodudur (4xx istemci, 5xx sunucu). nginx'in kendi uyarı ve hata kayıtları
([warn], [error]) ayrı bir dosyadadır (error.log) ve bu tabloya girmez.
"""
import os

from pyspark.sql import SparkSession

CATALOG = "lakehouse"
HAM = f"{CATALOG}.nginx_raw.access_log"
GOLD = f"{CATALOG}.gold"
GERI_GUN = int(os.environ.get("GERI_GUN", "3"))
BOT = "bot|crawler|spider|slurp|zgrab|masscan|nmap|curl|wget|python-requests|go-http-client"

builder = (SparkSession.builder.appName("gold-nginx")
           .config("spark.sql.session.timeZone", "Europe/Istanbul"))
cred = os.environ.get("POLARIS_CREDENTIAL")
if cred:
    builder = builder.config(f"spark.sql.catalog.{CATALOG}.credential", cred)
spark = builder.getOrCreate()

TABLOLAR = ["nginx_saatlik", "nginx_ip_gunluk", "nginx_sayfa_gunluk"]
mevcut = {r.tableName for r in spark.sql(f"SHOW TABLES IN {GOLD}").collect()}
hepsi_var = all(t in mevcut for t in TABLOLAR)
kosul = f"WHERE to_date(ts) >= date_sub(current_date(), {GERI_GUN - 1})" if hepsi_var else ""

spark.sql(f"""
SELECT ts,
       to_date(ts)                                   AS gun,
       date_trunc('HOUR', ts)                        AS saat,
       server                                        AS sunucu,
       remote                                        AS ip,
       code,
       coalesce(size, 0)                             AS size,
       regexp_replace(split(path, '[?]')[0], '/[0-9]+', '/{{id}}') AS yol,
       lower(coalesce(agent, '')) RLIKE '{BOT}'      AS bot
FROM {HAM} {kosul}
""").createOrReplaceTempView("istek")

SORGULAR = {
    "nginx_saatlik": """
SELECT gun, saat, sunucu,
       count(*)                                             AS istek,
       sum(CASE WHEN code BETWEEN 200 AND 299 THEN 1 ELSE 0 END) AS basarili_2xx,
       sum(CASE WHEN code BETWEEN 300 AND 399 THEN 1 ELSE 0 END) AS yonlendirme_3xx,
       sum(CASE WHEN code BETWEEN 400 AND 499 THEN 1 ELSE 0 END) AS istemci_hatasi_4xx,
       sum(CASE WHEN code >= 500 THEN 1 ELSE 0 END)              AS sunucu_hatasi_5xx,
       count(DISTINCT ip)                                   AS tekil_ip,
       sum(size)                                            AS toplam_bayt,
       sum(CASE WHEN bot THEN 1 ELSE 0 END)                 AS bot_istek,
       current_timestamp()                                  AS hesaplanma_zamani
FROM istek GROUP BY gun, saat, sunucu""",
    "nginx_ip_gunluk": """
SELECT gun, ip,
       count(*)                                             AS istek,
       sum(CASE WHEN code BETWEEN 400 AND 499 THEN 1 ELSE 0 END) AS istemci_hatasi_4xx,
       sum(CASE WHEN code >= 500 THEN 1 ELSE 0 END)              AS sunucu_hatasi_5xx,
       sum(CASE WHEN code = 401 THEN 1 ELSE 0 END)          AS basarisiz_giris_401,
       sum(CASE WHEN code = 403 THEN 1 ELSE 0 END)          AS yasak_403,
       sum(CASE WHEN code = 404 THEN 1 ELSE 0 END)          AS bulunamadi_404,
       count(DISTINCT yol)                                  AS farkli_sayfa,
       count(DISTINCT sunucu)                               AS sunucu_sayisi,
       max(bot)                                             AS bot,
       min(ts)                                              AS ilk_gorulme,
       max(ts)                                              AS son_gorulme,
       current_timestamp()                                  AS hesaplanma_zamani
FROM istek GROUP BY gun, ip""",
    "nginx_sayfa_gunluk": """
SELECT gun, yol,
       count(*)                                             AS istek,
       sum(CASE WHEN code BETWEEN 400 AND 499 THEN 1 ELSE 0 END) AS istemci_hatasi_4xx,
       sum(CASE WHEN code >= 500 THEN 1 ELSE 0 END)              AS sunucu_hatasi_5xx,
       count(DISTINCT ip)                                   AS tekil_ip,
       sum(size)                                            AS toplam_bayt,
       current_timestamp()                                  AS hesaplanma_zamani
FROM istek GROUP BY gun, yol""",
}

for ad, sql in SORGULAR.items():
    df = spark.sql(sql)
    hedef = f"{GOLD}.{ad}"
    if hepsi_var:
        df.writeTo(hedef).overwritePartitions()
    else:
        df.writeTo(hedef).using("iceberg").partitionedBy(df["gun"]).createOrReplace()
    print(f"GOLD_OK {ad} {df.count()} satır ({'son ' + str(GERI_GUN) + ' gün' if hepsi_var else 'tüm geçmiş'})")
