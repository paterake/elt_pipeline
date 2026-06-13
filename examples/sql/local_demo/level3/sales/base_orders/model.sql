select order_id, amount, order_date
from raw_orders
where order_date >= '{{ window.start_date }}'
  and order_date <= '{{ window.end_date }}'
