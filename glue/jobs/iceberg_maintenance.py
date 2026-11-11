"""iceberg_maintenance — spec §6 bakım (D(f) deklaratif): 3 ScheduledSparkApplication aynı dosyayı farklı --mode ile koşturur.
  --mode position-deletes   Silver: rewrite_position_delete_files (MoR delete dosyalarını katla)         saatlik
  --mode compact            Silver+Bronze: rewrite_data_files(delete-file-threshold=5, remove-dangling-deletes, partial-progress)  6 saat
  --mode expire-orphan-ttl  Silver+Bronze: expire_snapshots(--snapshot-days) + remove_orphan_files(--orphan-days); Bronze: DELETE _cdc.ts (düz tablolarda ts) < now-ttl  günlük; maintain_namespaces: bakım var, TTL yok — saklama kararı F5
Silver tabloları pipelines.json'dan (bronze'dan türetilir). Bronze tabloları KATALOGDAN: pipelines.json'daki
bronze_namespaces için SHOW TABLES (+ pipelines'ın bronze adları) — Silver pipeline'ı olmayan (append-only) Bronze
tablolar da bakım görsün. Var olmayan namespace/tablo atlanır (henüz veri gelmemiş olabilir).

Saatlik position-deletes koşusu sonunda katalogdaki HER tablonun durumunu ops.table_health'e ekler (health_lib):
Superset "Tablo sağlığı" panosunun tek veri kaynağı; pano bu sayede her kurulumda tablo listesi yazmadan çalışır."""
import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import health_lib as hl  # noqa: E402
import merge_lib as ml  # noqa: E402
from merge_cdc import CATALOG, PIPELINES, session, table_exists  # noqa: E402
from pyspark.sql.utils import AnalysisException  # noqa: E402


def ts_days_ago(days: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S+00:00")


def bronze_tables(spark, namespaces: list[str], from_pipelines: list[str]) -> list[str]:
    """bronze_namespaces içindeki tüm tablolar (ns.tablo) + pipelines'ın bronze adları. Olmayan namespace atlanır."""
    out = set(from_pipelines)
    for ns in namespaces:
        try:
            rows = spark.sql(f"SHOW TABLES IN {CATALOG}.{ns}").collect()
        except AnalysisException as e:
            print(f"[{ns}] namespace yok/okunamadı — atlandı ({type(e).__name__}: {str(e)[:120]})")
            continue
        out |= {f"{ns}.{r['tableName']}" for r in rows}
    return sorted(out)


def call(spark, proc: str, table: str, extra: str = "") -> None:
    # katalogla nitelenmiş ad: aksi hâlde Spark, prosedür argümanının ilk parçasını katalog sanıp CATALOG_NOT_FOUND yoklar
    sql = f"CALL {CATALOG}.system.{proc}(table => '{CATALOG}.{table}'{extra})"
    print(sql)
    for r in spark.sql(sql).collect():
        print("  ", r.asDict())


def collect_health(spark, silver_namespaces: set[str]) -> None:
    """Katalogdaki bütün tabloların son snapshot özetini ops.table_health'e ekler; 90 günden eski ölçümleri siler."""
    measured = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)
    target = f"{CATALOG}.{hl.HEALTH_NAMESPACE}.{hl.HEALTH_TABLE}"
    rows = []
    for ns_row in spark.sql(f"SHOW NAMESPACES IN {CATALOG}").collect():
        ns = ns_row[0]
        try:
            tables = [r["tableName"] for r in spark.sql(f"SHOW TABLES IN {CATALOG}.{ns}").collect()]
        except AnalysisException as e:
            print(f"[sağlık] {ns} okunamadı — atlandı ({type(e).__name__})")
            continue
        for t in tables:
            if ns == hl.HEALTH_NAMESPACE and t == hl.HEALTH_TABLE:
                continue
            try:
                snaps = [(r["committed_at"], dict(r["summary"] or {})) for r in
                         spark.sql(f"SELECT committed_at, summary FROM {CATALOG}.{ns}.{t}.snapshots").collect()]
            except AnalysisException as e:
                print(f"[sağlık] {ns}.{t} okunamadı — atlandı ({type(e).__name__})")
                continue
            row = hl.summarize(f"{ns}.{t}", hl.layer_of(ns, silver_namespaces), snaps, measured)
            if row:
                rows.append(row)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {target} ({hl.HEALTH_DDL_COLUMNS}) USING iceberg "
              f"PARTITIONED BY (days(olcum_zamani))")
    if rows:
        cols = [c.split()[0] for c in hl.HEALTH_DDL_COLUMNS.split(", ")]
        spark.createDataFrame([tuple(r[c] for c in cols) for r in rows],
                              spark.table(target).schema).writeTo(target).append()
    # saklama: ölçüm satırları 90 günden eskiyse düşer (tablo yalnız bu işin yazdığı ölçümleri tutar)
    spark.sql(f"DELETE FROM {target} WHERE olcum_zamani < "
              f"current_timestamp() - INTERVAL {hl.HEALTH_RETENTION_DAYS} DAYS")
    print(f"HEALTH_OK {len(rows)} tablo")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", required=True, choices=["position-deletes", "compact", "expire-orphan-ttl"])
    ap.add_argument("--snapshot-days", type=int, default=7)
    ap.add_argument("--orphan-days", type=int, default=3)
    ap.add_argument("--bronze-ttl-days", type=int, default=30)
    a = ap.parse_args()
    with open(PIPELINES, encoding="utf-8") as f:
        doc = json.load(f)
    pipes = ml.parse_pipelines(doc)
    silver = [p.silver or ml.silver_name(p.bronze) for p in pipes]
    spark = session(f"maint-{a.mode}")
    bronze = bronze_tables(spark, list(doc.get("bronze_namespaces") or []), [p.bronze for p in pipes])
    # pipeline/mongo dışı bakım kapsamı (nginx_raw, JSON akışları): bakım var; saklama yalnız retentionDays ile
    extra = bronze_tables(spark, list(doc.get("maintain_namespaces") or []), [])
    retention = doc.get("retention") or {}
    failed = []
    for t in (silver if a.mode == "position-deletes" else silver + bronze + extra):
        try:
            # table_exists try İÇİNDE: tek bir bozuk/erişilemeyen tablo döngünün kalanını kesmesin (sonda fail-loud)
            if not table_exists(spark, f"{CATALOG}.{t}"):
                print(f"[{t}] yok — atlandı")
                continue
            if a.mode == "position-deletes":
                call(spark, "rewrite_position_delete_files", t)
            elif a.mode == "compact":
                call(spark, "rewrite_data_files", t,
                     ", options => map('delete-file-threshold','5','remove-dangling-deletes','true','partial-progress.enabled','true')")
                # veri dosyaları katlandıktan sonra manifest'ler de katlanır (F5 backlog): çok sayıda küçük manifest
                # metadata okuma/planlama maliyetini artırır — rewrite_data_files bunu otomatik yapmaz
                call(spark, "rewrite_manifests", t)
            else:
                call(spark, "expire_snapshots", t, f", older_than => TIMESTAMP '{ts_days_ago(a.snapshot_days)}', retain_last => 1")
                # prefix_listing: Hadoop FS yerine FileIO (S3FileIO) ile listeler -> resmi Spark imajında s3a yok (F2 e2e: "No FileSystem for scheme s3")
                call(spark, "remove_orphan_files", t, f", older_than => TIMESTAMP '{ts_days_ago(a.orphan_days)}', prefix_listing => true")
                if t in bronze:
                    # gün başına hizalı sınır: day(ts) partition'ları tam düşer (kısmi gün = gereksiz delete dosyası).
                    # Zaman kolonu: CDC Bronze _cdc.ts; düz tablolar (mongo __quarantine: ts) üst düzey ts (F3 e2e)
                    ts_col = "_cdc.ts" if "_cdc" in spark.table(f"{CATALOG}.{t}").columns else "ts"
                    sql = (f"DELETE FROM {CATALOG}.{t} WHERE {ts_col} < "
                           f"date_trunc('DAY', current_timestamp() - INTERVAL {a.bronze_ttl_days} DAYS)")
                    print(sql)
                    spark.sql(sql)
                elif t in retention:
                    # pipeline dışı tablolar (nginx, JSON akışları): yalnız retentionDays verildiyse saklama uygulanır
                    r = retention[t]
                    sql = (f"DELETE FROM {CATALOG}.{t} WHERE {r['col']} < "
                           f"date_trunc('DAY', current_timestamp() - INTERVAL {int(r['days'])} DAYS)")
                    print(sql)
                    spark.sql(sql)
        except Exception as e:  # noqa: BLE001
            failed.append(t)
            print(f"[{t}] HATA {type(e).__name__}: {e}", file=sys.stderr)
    if a.mode == "position-deletes":
        try:
            collect_health(spark, {t.split(".")[0] for t in silver})
        except Exception as e:  # noqa: BLE001
            failed.append(f"{hl.HEALTH_NAMESPACE}.{hl.HEALTH_TABLE}")
            print(f"[sağlık] HATA {type(e).__name__}: {e}", file=sys.stderr)
    spark.stop()
    if failed:
        print(f"BAŞARISIZ: {failed}", file=sys.stderr)
        sys.exit(1)
    print("MAINT_OK")


if __name__ == "__main__":
    main()
