select project_id, project_name, market, region,
       cast(start_date as date) as start_date,
       cast(contract_value as decimal(16, 2)) as contract_value
from {{ source('bronze', 'projects') }}
