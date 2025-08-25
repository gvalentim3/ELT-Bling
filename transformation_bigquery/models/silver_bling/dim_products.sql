{{
    config(
        materialized='table'
    )
}}

WITH products AS (
    SELECT *
    FROM {{ ref('stg_bling_products') }}
),

products_components AS (
    SELECT *
    FROM {{ ref('stg_bling_products_components') }}
),


composite_products_costs AS (
    SELECT
        pc.composite_product_id,
        SUM(p.cost_price * pc.component_quantity) AS calculated_composite_product_cost
    FROM
        products_components AS pc
    LEFT JOIN
        products AS p ON pc.component_id = p.product_id
    GROUP BY
        pc.composite_product_id
)

SELECT
    p.product_id,
    p.product_name,
    p.product_internal_code,
    p.brand,
    p.price,
    p.is_active,
    p.is_kit,
    p.category_id,
    p.supplier_name,
    p.product_supplier_code,
    
    COALESCE(cp.calculated_composite_product_cost, p.cost_price) AS cost_price,
    
    p.buy_price
FROM
    products AS p
LEFT JOIN
    composite_products_costs AS cp ON p.product_id = cp.composite_product_id