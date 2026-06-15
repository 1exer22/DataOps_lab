SELECT
    o.order_id,
    o.customer_id,
    o.store_id,
    o.order_date,
    o.status,
    c.country AS customer_country,
    c.segment AS customer_segment,
    c.signup_date AS customer_signup_date
FROM {{ ref('stg_orders') }} o
LEFT JOIN {{ ref('stg_customers') }} c
    ON o.customer_id = c.customer_id