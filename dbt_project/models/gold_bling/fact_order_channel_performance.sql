{{
    config(
        materialized='incremental',
        unique_key=['sale_channel_name', 'category_name', 'order_date'],
        tags=['semanal']
    )
}}

WITH order_items_agg AS (
    SELECT
        oi.order_id,
        c.category_name,
        
        SUM(oi.quantity) AS total_quantity_sold,
        SUM(CASE WHEN p.is_kit THEN oi.quantity ELSE 0 END) AS kit_quantity_sold,
        SUM(CASE WHEN NOT p.is_kit THEN oi.quantity ELSE 0 END) AS individual_product_quantity_sold
        
    FROM {{ ref('fact_order_items_details') }} AS oi
    LEFT JOIN {{ ref('dim_products') }} AS p
        ON oi.product_id_for_merge = CAST(p.product_id AS STRING)
    LEFT JOIN {{ ref('dim_categories') }} AS c
        ON p.category_id = c.category_id
    GROUP BY 
        oi.order_id, c.category_name
)

SELECT
    o.sale_channel_name,
    o.order_date,
    oi.category_name,

    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(o.total_order_value) AS total_revenue,
    SUM(o.order_shipping_cost) AS total_shipping_cost,
    SUM(o.order_discount_value) AS total_discount_value,
    SUM(o.total_order_profit) AS total_profit,

    SUM(oi.total_quantity_sold) AS total_quantity_sold,
    SUM(oi.kit_quantity_sold) AS kit_quantity_sold,
    SUM(oi.individual_product_quantity_sold) AS individual_product_quantity_sold

FROM {{ ref('fact_orders') }} AS o
LEFT JOIN order_items_agg AS oi
    ON o.order_id = oi.order_id
GROUP BY
    o.sale_channel_name, o.order_date, oi.category_name