{{
    config(
        materialized='table'
    )
}}

SELECT
    channel_id,
    channel_name
FROM
    {{ ref('stg_bling_sales_channels') }}