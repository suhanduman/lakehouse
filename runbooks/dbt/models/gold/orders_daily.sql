-- Gold örneği: Silver `shop.orders` tablosundan günlük sipariş özeti.
-- Kaynak tam nitelikli yazılır (bu referans projede `sources.yml` yoktur); gerçek projede
-- `source()`/`ref()` kullanmak önerilir.
{{ config(materialized='table') }}

select
    date(updated_at)            as gun,
    status                      as durum,
    count(*)                    as siparis_adedi,
    sum(amount)                 as toplam_tutar
from lakehouse.shop.orders
group by 1, 2
