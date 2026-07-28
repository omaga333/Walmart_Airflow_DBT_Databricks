{{ config(materialized='ephemeral') }}

select distinct
    order_id,
    order_item_id,
    payment_method,
    order_status,
    order_created_timestamp,
    order_updated_timestamp,
    order_is_active,
    order_processed_at,
    obt_b_processed_at ,
    current_timestamp() AS order_gold_processed_at
from {{ ref('obt_b') }}