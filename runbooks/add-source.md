# Kaynak DB ekleme (pg / mssql)

1. **Kaynakta hazırlık** (DBA):
   - pg: `wal_level=logical`; rol: `CREATE ROLE <u> LOGIN REPLICATION PASSWORD '…'`; yakalanacak tabloların sahibi bu rol olmalı (filtered publication) ya da `ALTER TABLE … OWNER TO <u>`; `GRANT CREATE ON DATABASE <db> TO <u>`;
     sinyal tablosu: `CREATE TABLE <schema>.debezium_signal (id varchar(42) PRIMARY KEY, type varchar(32) NOT NULL, data varchar(2048)); ALTER TABLE <schema>.debezium_signal OWNER TO <u>;`
   - mssql: `EXEC sys.sp_cdc_enable_db; EXEC sys.sp_cdc_enable_table @source_schema='dbo', @source_name='<t>', @role_name=NULL;` her tablo için; sinyal tablosu aynı şemayla; kullanıcıya `db_owner` ya da CDC şemasına SELECT.
     TLS varsayılan AÇIK (`database.encrypt=true`); `trustServerCertificate: true`'yu YALNIZ özel CA'lı laboratuvarda kullan (sertifika doğrulamasını kapatır) — üretimde bunun yerine truststore'u `extraConfig` ile ver (`database.trustStore`/`database.trustStorePassword`).
2. **Secret** (Git'e girmez): `kubectl -n lakehouse create secret generic <name>-db --from-literal=username=<u> --from-literal=password=<p>`
3. **Polaris namespace'leri**: `platform/polaris/setup.yaml` → `namespaces:` listesine `<name>_raw` ve `<name>` ekle → `runbooks/scripts/polaris-setup.sh` (idempotent).
4. **values**: `platform/values/glue.yaml`
   ```yaml
   sources:
   - {name: <name>, type: postgres|sqlserver, host: <host>, port: <port>, database: <db>, tables: [<schema.t1>, <schema.t2>], signalTable: <schema>.debezium_signal}
   pipelines:
   - {bronze: <name>_raw.<t1>, keys: [<pk>], casts: {<timestamptz_kolon>: timestamp}}
   ```
   Silver bölümleme `bucket(bucket_count, keys[0])` — **yalnız ilk anahtar** kullanılır; bileşik anahtarda kolonları kardinaliteye göre sırala (en ayırt edici olan başa).
   Commit + push → ArgoCD `glue` sync: `KafkaUser/connect` ACL'i, `Role/connect-secrets-reader`, `KafkaConnector/dbz-<name>` + `sink-<name>`, `ConfigMap/lakehouse-jobs` güncellenir.
5. **Doğrulama**: `kubectl -n lakehouse get kafkaconnector` (Ready), `kubectl -n lakehouse get kafkaconnector dbz-<name> -o jsonpath='{.status.connectorStatus.tasks[0]}'`; Bronze tablo `<name>_raw.<t>` ilk commit'ten (≤ 5 dk) sonra görünür; sonraki `silver-merge` çalışmasında Silver `<name>.<t>` yaratılır.

DLQ gerçeği: sink `ErrantRecordReporter` uygulamaz → `<prefix>.dlq` yalnız converter/SMT hatalarını alır; yazma hatası task'ı durdurur (izleme F5). Sorun giderme: task FAILED → `…tasks[0].trace`; yeniden başlatma `kubectl -n lakehouse annotate kafkaconnector <ad> strimzi.io/restart=true`; consumer konumu ileri kaldıysa `runbooks/install.md` sorun giderme (offset reset).
