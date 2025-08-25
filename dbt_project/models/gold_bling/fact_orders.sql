{{ config(
    materialized='incremental',
    unique_key=['order_id'],
    tags=['semanal']
) }}

WITH orders_details AS (
    SELECT *
    FROM {{ ref('silver_orders_details') }}
),
order_items_details AS (
    SELECT *
    FROM {{ ref('fact_order_items_details') }}
),
sales_channels AS (
    SELECT *
    FROM {{ ref('dim_sales_channels') }}
)

SELECT
    o.order_id,
    ANY_VALUE(o.order_date) AS order_date,
    ANY_VALUE(o.client_id) AS client_id,
    ANY_VALUE(sc.channel_name) AS sale_channel_name,
    ANY_VALUE(o.order_shipping_cost) AS order_shipping_cost,
    ANY_VALUE(o.order_discount_value) AS order_discount_value,
    ANY_VALUE(o.order_commission_fee) AS order_commission_fee,
    ANY_VALUE(o.total_order_value) AS total_order_value,

    SUM(oi.order_product_cost) AS total_order_product_cost,

    (ANY_VALUE(o.order_shipping_cost)
     + ANY_VALUE(o.order_discount_value)
     + ANY_VALUE(o.order_commission_fee)
     + SUM(oi.order_product_cost)
    ) AS total_order_cost,

    (ANY_VALUE(o.total_order_value)
     - (ANY_VALUE(o.order_shipping_cost)
        + ANY_VALUE(o.order_discount_value)
        + ANY_VALUE(o.order_commission_fee)
        + SUM(oi.order_product_cost))
    ) AS total_order_profit,

    (
        (ANY_VALUE(o.total_order_value)
         - (SUM(oi.order_product_cost)
            + ANY_VALUE(o.order_shipping_cost)
            + ANY_VALUE(o.order_commission_fee)
            + ANY_VALUE(o.order_discount_value))
        ) / NULLIF(ANY_VALUE(o.total_order_value), 0)
    ) AS total_order_margin

FROM orders_details o
LEFT JOIN order_items_details oi
    ON o.order_id = oi.order_id
LEFT JOIN sales_channels sc
    ON o.sale_channel_id = sc.channel_id


GROUP BY o.order_id
