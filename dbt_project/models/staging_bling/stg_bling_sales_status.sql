{{
  config(
    materialized='view'
  )
}}


SELECT
    id AS status_id,
    UPPER(nome) AS status_name

FROM 
    {{source('bronze_bling', 'raw_status')}}