{{
  config(
    materialized='view',
    tags=['semanal']
  )
}}

SELECT
    data.id AS order_id,
    data.numero AS order_number,

    SAFE_CAST(data.data AS DATE) AS order_date,
    SAFE_CAST(data.dataSaida AS DATE) AS dispatch_date,
    SAFE_CAST(data.dataPrevista AS DATE) AS estimated_delivery_date,
    SAFE_CAST(data.totalProdutos AS NUMERIC) AS total_products_value,
    SAFE_CAST(data.total AS NUMERIC) AS total_order_value,

    data.contato.id AS client_id,
    data.situacao.id AS status_id,
    data.loja.id AS sale_channel_id,

    SAFE_CAST(data.desconto.valor AS NUMERIC) AS order_discount_value,

    SAFE_CAST(data.taxas.taxaComissao AS NUMERIC) AS order_commission_fee,
    SAFE_CAST(data.taxas.custoFrete AS NUMERIC) AS order_shipping_cost,
    SAFE_CAST(data.taxas.valorBase AS NUMERIC) AS base_value

FROM 
    {{source('bronze_bling', 'raw_sales')}}