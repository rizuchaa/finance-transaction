{{ 
    config(
        materialized='table',
        alias='sheet_finance_customers',
        schema='std'
    ) 
}}

WITH stg AS (
    SELECT

        CASE
            WHEN customer_id::TEXT ~ '^\d+(\.0+)?$' THEN CAST(customer_id AS BIGINT)
            ELSE NULL
        END AS customer_id,

        CASE
            WHEN first_name = 'NaN' THEN NULL
            ELSE INITCAP(first_name)
        END AS first_name,

        CASE
            WHEN last_name = 'NaN' THEN NULL
            ELSE INITCAP(last_name)
        END AS last_name,

        CASE
            WHEN date_of_birth ~ '/' THEN NULL
            WHEN date_of_birth = 'NaN' THEN NULL
            WHEN date_of_birth ~ '^\d{4}-\d{2}-\d{2}$' THEN CAST(date_of_birth AS DATE)
            ELSE NULL
        END AS date_of_birth,

        CASE
            WHEN split_part(address, ' ', 1) = 'NaN' THEN NULL
            WHEN split_part(address, ' ', 1) !~ '^\d+$' THEN NULL
            ELSE INITCAP(address)
        END AS address,

        CASE
            WHEN city = 'NaN' THEN NULL
            ELSE INITCAP(city)
        END AS city,
        
        CASE
            WHEN province = 'NaN' THEN NULL
            ELSE INITCAP(province)
        END AS province

    FROM {{ source('staging', 'raw_sheet_finance_customers') }}
)   
SELECT * 
FROM stg
WHERE NOT (
    customer_id IS NULL
)
