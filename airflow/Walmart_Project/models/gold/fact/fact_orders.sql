select

    order_id,
    order_item_id,
    customer_id,
    product_id,
    store_id,
    employee_id,

    quantity,
    unit_price,
    line_amount,
    total_amount

from {{ ref('obt_b') }}