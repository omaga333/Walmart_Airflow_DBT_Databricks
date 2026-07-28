{{ config(severity="warn") }}
Select 1

From {{ ref('obt_b') }} 

Where order_id is null
or product_id is null
or order_item_id is null
or customer_id is null
or store_id is null
or employee_id is null
