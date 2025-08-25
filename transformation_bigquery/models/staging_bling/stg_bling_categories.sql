{{
  config(
    materialized='view'
  )
}}

SELECT
    id AS category_id,
    UPPER(descricao) AS category_name,
    categoriaPai.id AS parent_category_id

FROM 
    {{source('bronze_bling', 'raw_categories')}}