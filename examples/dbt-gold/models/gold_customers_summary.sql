-- One example Gold model: an aggregate over Silver customers.
select
    count(*)                as customer_count,
    count(distinct id)      as distinct_ids
from {{ source('silver', 'customers') }}
