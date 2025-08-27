{{ config(
    materialized='incremental',
    unique_key=['product_id_for_merge','order_date'], 
    tags=['semanal']
) }}

WITH orders_details AS (
    SELECT *
    FROM {{ ref('fact_orders') }}
),
order_items_details AS (
    SELECT *
    FROM {{ ref('fact_order_items_details') }}
),
products AS (
    SELECT *
    FROM {{ ref('dim_products') }}
),
categories AS (
    SELECT *
    FROM {{ ref('dim_categories') }}
)

SELECT
    oi.product_id,
    p.product_name,
    oi.product_id_for_merge,
    od.order_date,
    SUM(oi.order_unit_price * oi.quantity) AS gross_revenue, 
    SUM(p.cost_price * oi.quantity) AS total_cost,
    SUM((oi.order_unit_price - p.cost_price) * oi.quantity) AS gross_profit, 
    SUM(oi.quantity) AS total_quantity,
    c.category_name
    
FROM order_items_details oi
LEFT JOIN orders_details od 
    ON oi.order_id = od.order_id
LEFT JOIN products p 
    ON oi.product_id_for_merge = CAST(p.product_id AS STRING)
LEFT JOIN categories c
    ON p.category_id = c.category_id

GROUP BY 
    oi.product_id,
    p.product_name,
    oi.product_id_for_merge,
    od.order_date,
    c.category_name

