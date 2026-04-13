create database project1 encoding = 'utf8';
create schema if not exists flightDb;
set search_path to flightDb;
create table if not exists region(
    region_id serial primary key,
    name text not null unique,
    code char(2),
    constraint up_region_code unique(code)
);
create table if not exists airline(
    airline_id serial primary key,
    code varchar(3) not null unique,
    name text not null unique,
    region_id int,foreign key (region_id) references region(region_id)
    on update cascade
);
create table if not exists airport(
    airport_id serial primary key,
    name text not null,
    city text not null,
    region_id int not null references region(region_id)on update cascade,
    iata_code char(3) not null unique
);
create table if not exists  airport_info(
    airport_id int primary key references airport(airport_id) on delete cascade,
    latitude numeric(9,6),
    longitude numeric(9,6),
    altitude integer,
    timezone_offset int,
    timezone_dst varchar(4),
    timezone_region text --要不要拆开呢？
);
create table if not exists passenger(
    passenger_id int primary key,
    first_name text not null,
    last_name text not null,
    age int not null,
    gender varchar(10),
    mobile_number varchar(20)not null unique
);
create table if not exists flight_route(
    route_id serial primary key,
    flight_no varchar(10) not null,
    airline_id int not null references airline(airline_id) on update cascade,
    source_airport_id int not null references airport(airport_id) on update cascade,
    destination_airport_id int not null references airport(airport_id)on update cascade,
    departure_time time not null,
    arrival_time time not null,
    arrival_day_offset int not null DEFAULT 0,
    constraint uq_route_natural unique (
        flight_no,
        airline_id,
        source_airport_id,
        destination_airport_id,
        departure_time,
        arrival_time,
        arrival_day_offset
    )
);
create table if not exists flight_instance(
    instance_id serial primary key,
    route_id int not null references flight_route(route_id) on delete cascade,
    flight_data date not null,
    business_price numeric(10,2)not null,
    business_remain int not null,
    economy_price numeric(10,2) not null,
    economy_remain int not null
    constraint ck_instance_business_price check (business_price >= 0),
    constraint ck_instance_economy_price check (economy_price >= 0),
    constraint ck_instance_business_remain check (business_remain >= 0),
    constraint ck_instance_economy_remain check (economy_remain >= 0),
    constraint uq_instance_route_date unique (route_id, flight_data)
);
alter table region add constraint ck_region_code_format check(code is null or ~ '^[A-Z]{2}$');
alter table region add constraint un_region_code unique(code);
alter table airport add constraint ck_airport_iata_format check(iata_code ~'^[A-Z]{3}');

