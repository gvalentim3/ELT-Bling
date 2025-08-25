{{ config(
    materialized='table'
) }}

WITH products_components AS (
    SELECT *
    FROM {{ ref('silver_composites_products') }}
)

SELECT
    pc.composite_product_id AS kit_id,
    p.product_name AS kit_name,
    pc.component_id,
    comp_p.product_name AS component_name,
    pc.component_quantity
FROM products_components as pc
LEFT JOIN {{ ref('dim_products') }} p
    ON pc.composite_product_id = p.product_id
LEFT JOIN {{ ref('dim_products') }} AS comp_p
    ON pc.component_id = comp_p.product_id
