{{
    config(
        materialized='table'
    )
}}

SELECT
    status_id,
    status_name
FROM
    {{ ref('stg_bling_sales_status') }}