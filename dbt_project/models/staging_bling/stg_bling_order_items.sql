{{
  config(
    materialized='view',
    tags=['semanal']
  )
}}

WITH source AS (
    SELECT
        *
    FROM
        {{ source('bronze_bling', 'raw_sales') }}
)

SELECT
    source.data.id AS order_id,
    item.produto.id AS product_id,
    SAFE_CAST(item.quantidade AS INT64) AS quantity,
    SAFE_CAST(item.valor AS NUMERIC) AS order_unit_price, 

FROM
    source,
    UNNEST(source.data.itens) AS item