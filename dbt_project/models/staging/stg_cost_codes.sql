select cost_code, description, category from {{ source('bronze', 'cost_codes') }}
