select order_date, sum(amount) as total_amount
from base_orders
group by order_date
