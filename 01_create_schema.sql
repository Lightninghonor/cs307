create database project1 encoding 'utf8';

create schema if not exists flightdb;
set search_path to flightdb, public;

create table if not exists region (
    region_id int generated always as identity primary key,
    name text not null unique,
    code char(2),
    constraint uq_region_code unique (code),
    constraint ck_region_code_format check (code is null or lower(code) ~ '^[a-z]{2}$')
);

create table if not exists airline (
    airline_id int generated always as identity primary key,
    code varchar(3) not null unique,
    name text not null unique,
    region_id int references region(region_id) on update cascade,
    constraint ck_airline_code_format check (lower(code) ~ '^[a-z0-9]{2,3}$')
);

create table if not exists airport (
    airport_id int generated always as identity primary key,
    name text not null,
    city text not null,
    region_id int not null references region(region_id) on update cascade,
    iata_code char(3) not null unique,
    constraint ck_airport_iata_format check (lower(iata_code) ~ '^[a-z]{3}$')
);

create table if not exists airport_info (
    airport_id int primary key references airport(airport_id) on delete cascade,
    latitude numeric(9,6),
    longitude numeric(9,6),
    altitude int,
    timezone_offset int,
    timezone_dst varchar(4),
    timezone_region text,
    constraint ck_latitude_range check (latitude is null or latitude between -90 and 90),
    constraint ck_longitude_range check (longitude is null or longitude between -180 and 180),
    constraint ck_timezone_offset_range check (timezone_offset is null or timezone_offset between -12 and 14)
);

create table if not exists passenger (
    passenger_id int primary key,
    name text not null,
    age int not null,
    gender varchar(10),
    mobile_number varchar(20) not null unique,
    constraint ck_passenger_age check (age between 0 and 120),
    constraint ck_passenger_gender check (gender is null or lower(gender) in ('male', 'female', 'unknown')),
    constraint ck_passenger_mobile check (mobile_number ~ '^[0-9]{7,20}$')
);

create table if not exists flight_route (
    route_id int generated always as identity primary key,
    flight_no varchar(10) not null,
    airline_id int not null references airline(airline_id) on update cascade,
    source_airport_id int not null references airport(airport_id) on update cascade,
    destination_airport_id int not null references airport(airport_id) on update cascade,
    departure_time time not null,
    arrival_time time not null,
    arrival_day_offset int not null default 0,
    constraint ck_route_no_same_airport check (source_airport_id <> destination_airport_id),
    constraint ck_route_day_offset check (arrival_day_offset in (0, 1)),
    constraint ck_route_flight_no_format check (lower(flight_no) ~ '^[a-z0-9]{4,10}$'),
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

create table if not exists flight_instance (
    instance_id int generated always as identity primary key,
    route_id int not null references flight_route(route_id) on delete cascade,
    flight_date date not null,
    business_price numeric(10,2) not null,
    business_remain int not null,
    economy_price numeric(10,2) not null,
    economy_remain int not null,
    constraint ck_instance_business_price check (business_price >= 0),
    constraint ck_instance_economy_price check (economy_price >= 0),
    constraint ck_instance_business_remain check (business_remain >= 0),
    constraint ck_instance_economy_remain check (economy_remain >= 0),
    constraint uq_instance_route_date unique (route_id, flight_date)
);

create index if not exists idx_airline_region_id on airline(region_id);
create index if not exists idx_airport_region_id on airport(region_id);
create index if not exists idx_airport_city on airport(city);
create index if not exists idx_route_search on flight_route(source_airport_id, destination_airport_id);
create index if not exists idx_instance_query on flight_instance(flight_date, route_id);

create or replace function fn_normalize_region()
returns trigger
language plpgsql
as $$
begin
    new.name := btrim(new.name);
    new.code := nullif(upper(btrim(new.code)), '');
    return new;
end;
$$;

create or replace function fn_normalize_airline()
returns trigger
language plpgsql
as $$
begin
    new.code := upper(regexp_replace(btrim(new.code), '\s+', '', 'g'));
    new.name := btrim(new.name);
    return new;
end;
$$;

create or replace function fn_normalize_airport()
returns trigger
language plpgsql
as $$
begin
    new.name := btrim(new.name);
    new.city := btrim(new.city);
    new.iata_code := upper(regexp_replace(btrim(new.iata_code), '\s+', '', 'g'));
    return new;
end;
$$;

create or replace function fn_normalize_passenger()
returns trigger
language plpgsql
as $$
begin
    new.name := btrim(new.name);
    new.mobile_number := regexp_replace(btrim(new.mobile_number), '\s+', '', 'g');
    new.gender := nullif(lower(btrim(new.gender)), '');
    return new;
end;
$$;

create or replace function fn_normalize_route()
returns trigger
language plpgsql
as $$
begin
    new.flight_no := upper(regexp_replace(btrim(new.flight_no), '\s+', '', 'g'));

    if new.arrival_day_offset is null then
        if new.arrival_time < new.departure_time then
            new.arrival_day_offset := 1;
        else
            new.arrival_day_offset := 0;
        end if;
    end if;

    if new.source_airport_id = new.destination_airport_id then
        raise exception 'source_airport_id and destination_airport_id cannot be the same';
    end if;

    return new;
end;
$$;

drop trigger if exists tg_region_normalize on region;
create trigger tg_region_normalize
before insert or update on region
for each row execute function fn_normalize_region();

drop trigger if exists tg_airline_normalize on airline;
create trigger tg_airline_normalize
before insert or update on airline
for each row execute function fn_normalize_airline();

drop trigger if exists tg_airport_normalize on airport;
create trigger tg_airport_normalize
before insert or update on airport
for each row execute function fn_normalize_airport();

drop trigger if exists tg_passenger_normalize on passenger;
create trigger tg_passenger_normalize
before insert or update on passenger
for each row execute function fn_normalize_passenger();

drop trigger if exists tg_route_normalize on flight_route;
create trigger tg_route_normalize
before insert or update on flight_route
for each row execute function fn_normalize_route();
