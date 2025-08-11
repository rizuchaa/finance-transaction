{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    schema='int',
    alias='int_geo_transaction'
) }}

WITH stg AS (
    SELECT
        first_name || ' ' || last_name AS customer_name,
        usd.transaction_id,
        accounts.account_type,
        currency_type,
        transaction_amount,
        transaction_usd_amount,
        city,
        province,
        merchant_guid,
        transaction_date
    
    FROM {{ source('public_int', 'int_usd_transaction') }} usd

    LEFT JOIN {{ source('staging_std', 'sheet_finance_accounts') }}  accounts
        ON usd.account_id = accounts.account_id
    
    LEFT JOIN {{ source('staging_std', 'sheet_finance_customers') }} customers
        ON accounts.customer_id = customers.customer_id
    
    WHERE accounts.account_id IS NOT NULL

)
SELECT * FROM stg

{% if is_incremental() %}
  WHERE transaction_date > (SELECT max(transaction_date) FROM {{ this }})
{% endif %}