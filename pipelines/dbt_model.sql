-- dbt model for Amex Credit Cards
SELECT *, CURRENT_TIMESTAMP AS processed_at
FROM { ref('stg_amex_credit_cards') };
