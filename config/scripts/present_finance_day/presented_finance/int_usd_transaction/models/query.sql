{{ config(
    materialized='table',
    unique_key='transaction_id',
    schema='int',
    alias='int_usd_transaction'
) }}

WITH stg1 AS(
    SELECT DISTINCT transaction_id, account_id,
        CASE 
            WHEN currency IS NULL THEN 'USD' 
            ELSE currency 
        END AS currency_type,
        COALESCE(transaction_amount, 0) transaction_amount,
        COALESCE(
            CASE 
                WHEN currency = 'EUR' THEN 1.17 * transaction_amount
                WHEN currency = 'AUD' THEN 0.65 * transaction_amount
                WHEN currency = 'IDR' THEN 0.000062 * (transaction_amount * 10000)
                WHEN currency = 'JPY' THEN 0.0068 * (transaction_amount * 100)
                ELSE transaction_amount
            END, 0) AS transaction_usd_amount,
        merchant_guid,
        transaction_date

    FROM {{ source('staging_std', 'sheet_finance_transactions') }}
)
SELECT * FROM stg1
WHERE transaction_amount > 0