{{ 
    config(
        materialized='table',
        alias='sheet_finance_transactions',
        schema='std'
    ) 
}}

WITH stg AS (
    SELECT DISTINCT
        CASE
            WHEN transaction_id::TEXT ~ '^\d+(\.0+)?$' THEN CAST(transaction_id AS BIGINT)
            ELSE NULL
        END AS transaction_id,

        CASE
            WHEN account_id::TEXT ~ '^\d+(\.0+)?$' THEN CAST(account_id AS BIGINT)
            ELSE NULL
        END AS account_id,

        transaction_type,
        CASE
            WHEN amount ~ '^\d+(\.\d+)?$' THEN CAST(amount AS NUMERIC(18,2))
            ELSE NULL
        END as transaction_amount,

        currency,
        merchant_id AS merchant_guid,
        
        CASE
            WHEN transaction_date ~ '/' THEN NULL
            WHEN transaction_date = 'NaN' THEN NULL
            WHEN transaction_date ~ '^\d{4}-\d{2}-\d{2}$' THEN CAST(transaction_date AS DATE)
            ELSE NULL
        END AS transaction_date
    FROM {{ source('staging', 'raw_sheet_finance_transactions') }}

)
SELECT * 
FROM stg
WHERE NOT (
    transaction_id IS NULL 
    OR account_id IS NULL
    OR transaction_date IS NULL
)