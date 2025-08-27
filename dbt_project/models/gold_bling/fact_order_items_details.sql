{{  config(
        materialized='incremental',
        unique_key=['order_id', 'product_id_for_merge'],
        tags=['semanal']
)   }}

WITH order_items_details AS (
    SELECT 
        *
    FROM 
        {{ ref('silver_order_items_details') }}
),
orders_details AS (
    SELECT
    order_id, order_date, sale_channel_id
    FROM
        {{ ref('silver_orders_details') }}
),
products AS (
    SELECT 
        *
    FROM
        {{ ref('dim_products') }}
)

SELECT 
    oi.order_id,
    od.order_date,

    oi.product_id,
    p.product_name,
    oi.product_id_for_merge,
    oi.quantity,
    oi.order_unit_price,

    (p.cost_price * oi.quantity) AS order_product_cost,
    (oi.order_unit_price * oi.quantity) AS gross_revenue,
    (oi.order_unit_price - p.cost_price) * oi.quantity AS gross_profit

FROM order_items_details oi
LEFT JOIN products p ON oi.product_id = p.product_id
LEFT JOIN orders_details od ON oi.order_id = od.order_id