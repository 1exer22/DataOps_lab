SELECT
    o.order_id,
    o.order_date,
    o.status,
    o.customer_country,
    o.customer_segment,
    COUNT(oi.product_id)              AS nb_products,
    COALESCE(SUM(oi.total_price), 0)  AS total_amount
FROM {{ ref('int_orders_enriched') }} o
LEFT JOIN {{ ref('int_order_items_enriched') }} oi
    ON o.order_id = oi.order_id
GROUP BY ALL