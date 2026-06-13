drop table if exists raw_orders;
create table raw_orders (
  order_id integer,
  updated_at text,
  amount integer,
  status text
);

insert into raw_orders (order_id, updated_at, amount, status) values
  (100, '2026-01-01T12:00:00+00:00', 10, 'created'),
  (101, '2026-01-02T08:30:00+00:00', 25, 'paid'),
  (102, '2026-01-03T15:45:00+00:00', 15, 'shipped');
