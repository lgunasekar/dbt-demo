
{{ config(materialized='table',
          alias='CUSTOMER_ORDER',
          tags='a') }}

with source_data as (
select 
C_CUSTKEY as customer_id,
C_NAME as customer_name,
O_ORDERKEY as order_id,
O_ORDERSTATUS as order_status,
O_TOTALPRICE as order_price,
O_ORDERDATE as order_date
from {{ source('jaffle_shop', 'orders') }} o
join {{ source('jaffle_shop', 'customer') }} c on o.O_CUSTKEY = c.C_CUSTKEY
)

select *
from source_data

