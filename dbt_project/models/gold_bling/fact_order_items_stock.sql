{{  config(
        materialized='incremental',
        unique_key=['component_product_id', 'order_date'],
        tags=['semanal']
) }}

WITH base_exploded_items AS (
    SELECT
        COALESCE(comp.component_id, oi.product_id) AS component_product_id,

        od.order_date,

        oi.quantity * COALESCE(comp.component_quantity, 1) AS final_quantity

    FROM {{ ref('fact_order_items_details') }} AS oi

    LEFT JOIN {{ ref('fact_orders') }} AS od
        ON oi.order_id = od.order_id

    LEFT JOIN {{ ref('silver_composites_products') }} AS comp
        ON oi.product_id = comp.composite_product_id
)

SELECT
    b.component_product_id,
    b.order_date,
    SUM(b.final_quantity) AS total_quantity_sold

FROM base_exploded_items b
GROUP BY
    component_product_id, order_date
