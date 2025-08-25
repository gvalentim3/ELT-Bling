{{
    config(
        materialized='incremental',
        unique_key = 'order_id',
        partition_by={
            "field": "order_date",
            "data_type": "date"
        },
        on_schema_change='sync_all_columns',
        tags=['semanal']
    )
}}

SELECT
    o.order_id,

    o.client_id,
    o.sale_channel_id,
    o.status_id,
    
    o.order_date,
    o.dispatch_date,
    o.estimated_delivery_date,

    o.order_shipping_cost,
    o.order_discount_value,
    o.order_commission_fee,

    o.total_order_value
    
FROM
    {{ ref('stg_bling_sales_orders') }} AS o
    LEFT JOIN
        {{ ref('stg_bling_sales_status') }} AS s
    ON o.status_id = s.status_id

WHERE s.status_name != "CANCELADO"
{% if is_incremental() %}
    AND (o.order_date > (SELECT MAX(order_date) FROM {{ this }})
         OR o.order_date >= CURRENT_DATE() - 7)
{% endif %}