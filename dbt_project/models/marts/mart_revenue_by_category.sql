SELECT
    product_category,
    COUNT(DISTINCT order_id)    AS nb_orders,
    SUM(total_price)            AS total_revenue
FROM {{ ref('int_order_items_enriched') }}
GROUP BY product_category
ORDER BY total_revenue DESC