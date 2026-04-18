-- Task4 补充建表：订单相关表
-- 在 01_create_schema.sql 执行完毕后运行此文件
-- 依赖：flightdb.passenger, flightdb.flight_instance

set search_path to flightdb, public;

-- 订单主表：一次购票行为
create table if not exists booking_order (
    order_id    int generated always as identity primary key,
    passenger_id int not null references passenger(passenger_id) on update cascade,
    instance_id  int not null references flight_instance(instance_id) on update cascade,
    cabin_class  varchar(10) not null,
    order_time   timestamptz not null default now(),
    constraint ck_order_cabin check (cabin_class in ('economy', 'business'))
);

create index if not exists idx_order_passenger on booking_order(passenger_id);
create index if not exists idx_order_instance  on booking_order(instance_id);
