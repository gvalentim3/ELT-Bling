{{
    config(
        materialized='table'
    )
}}

SELECT
    category_id,
    category_name,
    parent_category_id
FROM
    {{ ref('stg_bling_categories') }}