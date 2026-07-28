{{

    config(
        materialized='incremental',
        unique_key='employee_id'
    )
}}

Select*

, current_timestamp() as processed_at 

from {{ source('walmart_databricks', 'employees') }}




{% if is_incremental() %}

where updated_timestamp
 >
(
select coalesce(max(updated_timestamp),'1900-01-01')

from {{ this }}
)

{% endif %}