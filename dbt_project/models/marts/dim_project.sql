select p.*, coalesce(sum(b.budget_amount), 0) as budget_at_completion
from {{ ref('stg_projects') }} p
left join {{ ref('stg_budgets') }} b using (project_id)
group by all
