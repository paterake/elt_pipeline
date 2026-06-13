select
  order_date,
  total_amount
from order_summary
where order_date between '2026-01-01' and '2026-01-31'
order by order_date
