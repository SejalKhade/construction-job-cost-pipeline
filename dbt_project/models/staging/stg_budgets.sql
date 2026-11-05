select project_id, cost_code, cast(budget_amount as decimal(16, 2)) as budget_amount
from {{ source('bronze', 'budgets') }}
