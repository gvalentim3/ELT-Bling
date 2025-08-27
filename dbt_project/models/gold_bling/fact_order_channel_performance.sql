{{
    config(
        materialized='incremental',
        unique_key=['sale_channel_name', 'category_name', 'order_date'],
        tags=['semanal']
    )
}}

WITH item_metrics AS (
    SELECT
        o.sale_channel_name,
        o.order_date,
        c.category_name,

        SUM(oi.quantity) AS total_quantity_sold,
        SUM(CASE WHEN p.is_kit THEN oi.quantity ELSE 0 END) AS kit_quantity_sold,
        SUM(CASE WHEN NOT p.is_kit THEN oi.quantity ELSE 0 END) AS individual_product_quantity_sold
    
    FROM {{ ref('fact_order_items_details') }} AS oi
    LEFT JOIN {{ ref('fact_orders') }} AS o
        ON oi.order_id = o.order_id
    LEFT JOIN {{ ref('dim_products') }} AS p
        ON oi.product_id_for_merge = CAST(p.product_id AS STRING)
    LEFT JOIN {{ ref('dim_categories') }} AS c
        ON p.category_id = c.category_id
    GROUP BY 
        o.sale_channel_name, o.order_date, c.category_name
),

order_metrics AS (
    SELECT
        o.sale_channel_name,
        o.order_date,

        COUNT(DISTINCT o.order_id) AS total_orders,
        SUM(o.total_order_value) AS total_revenue,
        SUM(o.order_shipping_cost) AS total_shipping_cost,
        SUM(o.order_discount_value) AS total_discount_value,
        SUM(o.total_order_profit) AS total_profit

    FROM {{ ref('fact_orders') }} AS o
    GROUP BY
        o.sale_channel_name, o.order_date
)

SELECT
    COALESCE(im.sale_channel_name, om.sale_channel_name) AS sale_channel_name,
    COALESCE(im.order_date, om.order_date) AS order_date,
    im.category_name,

    om.total_orders,
    om.total_revenue,
    om.total_shipping_cost,
    om.total_discount_value,
    om.total_profit,

    im.total_quantity_sold,
    im.kit_quantity_sold,
    im.individual_product_quantity_sold

FROM item_metrics AS im
FULL OUTER JOIN order_metrics AS om
    ON im.sale_channel_name = om.sale_channel_name
    AND im.order_date = om.order_date