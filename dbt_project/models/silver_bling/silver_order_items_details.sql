{{ config( 
    materialized='incremental', 
    unique_key = ['order_id', 'product_id_for_merge'], 
    on_schema_change='sync_all_columns', 
    tags=['semanal'] ) 
}}

WITH base AS (
    SELECT 
        oi.order_id,
        oi.product_id,
        CURRENT_TIMESTAMP() AS _ingested_at,
        oi.quantity,
        oi.order_unit_price,
        CASE 
            WHEN oi.product_id = 0 THEN CONCAT('missing_', CAST(oi.order_id AS STRING))
            ELSE CAST(oi.product_id AS STRING)
        END AS product_id_for_merge,
        o.order_date
    FROM {{ ref('stg_bling_order_items') }} AS oi
    LEFT JOIN {{ ref('stg_bling_sales_orders')}} AS o
        ON oi.order_id = o.order_id
    LEFT JOIN {{ ref('stg_bling_sales_status') }} AS s
        ON o.status_id = s.status_id
    WHERE s.status_name != "CANCELADO"
    {% if is_incremental() %}
        AND (
            o.order_date > (SELECT MAX(order_date) FROM {{ this }})
            OR o.order_date >= CURRENT_DATE() - 7
        )
    {% endif %}
),
deduped AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER(PARTITION BY order_id, product_id_for_merge ORDER BY order_date DESC) AS rn
        FROM base
    )
    WHERE rn = 1
)

SELECT *
FROM deduped

