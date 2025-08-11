{{ config(
    materialized='view',
    alias='mart_finance_fraud_transaction'
) }}

WITH stg1 AS (
    SELECT
        transaction_id,
        customer_name,
        city,
        province,
        merchant_guid,
        transaction_usd_amount,
        transaction_date,

        CASE
            WHEN transaction_usd_amount > 1000 THEN TRUE
            WHEN transaction_date >= CURRENT_DATE - INTERVAL '1 day' AND transaction_usd_amount > 500 THEN TRUE
            ELSE FALSE
        END AS is_fraud,

        CASE
            WHEN transaction_usd_amount > 1000 THEN 'Large transaction amount.'
            WHEN transaction_date >= CURRENT_DATE - INTERVAL '1 day' AND transaction_usd_amount > 500 THEN 'Recent large transaction.'
            ELSE NULL
        END AS fraud_reason

   FROM {{ source('public_int', 'int_geo_transaction') }}
), 

stg AS (
    SELECT
        customer_name,
        city,
        province,

        COUNT(DISTINCT CASE WHEN is_fraud THEN transaction_id END) AS fraud_transaction_count,
        SUM(CASE WHEN is_fraud THEN transaction_usd_amount ELSE 0 END) AS fraud_transaction_usd_total,
        COUNT(DISTINCT CASE WHEN is_fraud THEN merchant_guid END) AS fraud_distinct_merchants,

        MIN(transaction_date) AS first_transaction_date,
        MAX(transaction_date) AS last_transaction_date,

        MD5(
          COALESCE(customer_name, '') || COALESCE(city, '') || COALESCE(province, '')
        ) AS fraud_agg_id

    FROM stg1
    WHERE is_fraud = TRUE
    GROUP BY customer_name, city, province
)
SELECT * FROM stg