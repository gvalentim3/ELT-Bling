{{
  config(
    materialized='view'
  )
}}

SELECT
    id AS channel_id,
    UPPER(descricao) AS channel_name

FROM 
    {{source('bronze_bling', 'raw_sales_channels')}}