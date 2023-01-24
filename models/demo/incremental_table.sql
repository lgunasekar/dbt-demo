
{{ config(materialized='incremental',
          unique_key='CUST_ORDER_KEY',
          alias='INCR_CUSTOMER_ORDER') }}

with source_data as (
select 
concat_ws('-',C_CUSTKEY,O_ORDERKEY) as cust_order_key,
C_CUSTKEY as customer_id,
C_NAME as customer_name,
O_ORDERKEY as order_id,
O_ORDERSTATUS as order_status,
O_TOTALPRICE as order_price,
O_ORDERDATE as order_date
from {{ source('jaffle_shop', 'orders') }} o
join {{ source('jaffle_shop', 'customer') }} c on o.O_CUSTKEY = c.C_CUSTKEY

{% if is_incremental() %}

where O_ORDERDATE >= (select max(O_ORDERDATE) from {{ source("jaffle_shop","orders") }})

{% endif %}
)

select *
from source_data

