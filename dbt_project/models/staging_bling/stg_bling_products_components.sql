{{
  config(
    materialized='view'
  )
}}

SELECT
    data.id AS composite_product_id,
    componente.produto.id AS component_id,
    SAFE_CAST(componente.quantidade AS INT64) AS component_quantity

FROM
    {{ source('bronze_bling', 'raw_products') }},
    UNNEST(data.estrutura.componentes) AS componente

WHERE
    data.formato = 'E'