# Var olan kaynağa tablo ekleme

1. Kaynakta tablonun sahibi/izinleri kaynak rolünde (pg: `ALTER TABLE <schema>.<t> OWNER TO <u>`; mssql: `sp_cdc_enable_table`).
2. `platform/values/glue.yaml` → ilgili `sources[].tables` listesine `<schema>.<t>` ekle; entity tablosuysa `pipelines` listesine `{bronze: <name>_raw.<t>, keys: [...]}`. Commit + push → ArgoCD sync (Debezium connector yeniden başlar, `table.include.list` güncellenir; pg'de filtered publication'a tablo otomatik eklenir).
3. Mevcut satırlar için **incremental snapshot** (kaynakta, sinyal tablosuna):
   ```sql
   INSERT INTO <schema>.debezium_signal (id, type, data)
   VALUES (gen_random_uuid()::text, 'execute-snapshot', '{"data-collections": ["<schema>.<t>"], "type": "incremental"}');
   ```
   (mssql: `NEWID()`.) İlerleme: `kubectl -n lakehouse logs deploy/connect-connect | grep -i "incremental snapshot"`. Debezium 3.x'te iki-sinyal tekrarı gerekmez (DBZ-8780 düzeltildi).
4. Bronze `<name>_raw.<t>` otomatik yaratılır; Silver bir sonraki `silver-merge` çalışmasında.
