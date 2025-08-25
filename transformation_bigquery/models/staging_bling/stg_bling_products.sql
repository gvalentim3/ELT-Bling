{{
  config(
    materialized='view'
  )
}}

SELECT
    data.id AS product_id,
    UPPER(data.nome) AS product_name,
    data.codigo AS product_internal_code,
    UPPER(data.marca) AS brand,

    SAFE_CAST(data.preco AS NUMERIC) AS price,

    data.situacao = 'A' AS is_active,
    data.formato = 'E' AS is_kit,

    data.categoria.id AS category_id,

    UPPER(data.fornecedor.contato.nome) AS supplier_name,
    data.fornecedor.codigo AS product_supplier_code,
    SAFE_CAST(data.fornecedor.precoCusto AS NUMERIC) AS cost_price,
    SAFE_CAST(data.fornecedor.precoCompra AS NUMERIC) AS buy_price

FROM 
    {{source('bronze_bling', 'raw_products')}}