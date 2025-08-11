{{ 
    config(
        materialized='table',
        alias='sheet_finance_accounts',
        schema='std'
    ) 
}}

WITH stg AS (
    SELECT 
        CASE
            WHEN account_id::TEXT ~ '^\d+(\.0+)?$' THEN CAST(account_id AS BIGINT)
            ELSE NULL
        END AS account_id,

        CASE
            WHEN customer_id::TEXT ~ '^\d+(\.0+)?$' THEN CAST(customer_id AS BIGINT)
            ELSE NULL
        END AS customer_id,

        UPPER(account_type) account_type,

        CASE
            WHEN balance ~ '^\d+(\.\d+)?$' THEN CAST(balance AS NUMERIC(18,2))
            ELSE NULL
        END AS account_balance,

        CASE
            WHEN opening_date ~ '/' THEN NULL
            WHEN opening_date = 'NaN' THEN NULL
            WHEN opening_date ~ '^\d{4}-\d{2}-\d{2}$' THEN CAST(opening_date AS DATE)
            ELSE NULL
        END AS opening_date
        
    FROM {{ source('staging', 'raw_sheet_finance_accounts') }}
)   

SELECT * 
FROM stg
WHERE NOT (
    account_id IS NULL
    OR opening_date IS NULL
)