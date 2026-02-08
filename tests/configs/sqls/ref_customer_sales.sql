SELECT 
    c.customer_id ,
    c.customer_name,
    c.customer_address,
    c.customer_phone_mapics,
    c.customer_email_mapics,
    c.customer_status_mapics,
    c.customer_since_mapics,
    c.customer_type_mapics,
    c.customer_region,
    c.customer_loyalty_points,
    c.customer_preferred_store,
    s.transaction_id,
    s.product_id_mapics,
    s.store_id_mapics,
    s.quantity,
    s.price,
    s.discount,
    s.total_amount,
    s.transaction_date_mapics,
    s.payment_method_mapics
FROM sandbox.integration_framework.customer_mapics c
JOIN sandbox.integration_framework.sales_mapics s
ON c.customer_id = s.customer_id_mapics
WHERE s.total_amount > 30