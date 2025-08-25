{{ config(
    materialized='table'
) }}

WITH components AS (
    SELECT
        composite_product_id,
        component_id,
        component_quantity
    FROM {{ ref('stg_bling_products_components') }}
)

SELECT
    composite_product_id,
    component_id,
    component_quantity
FROM components
WHERE component_id IS NOT NULL
