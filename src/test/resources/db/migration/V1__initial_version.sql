create extension if not exists "uuid-ossp";

drop table taxi_trips_info;

create table if not exists taxi_trips_info
(
    Borough text not null primary key,
    total_trips bigint,
    min_distance numeric,
    mean_distance numeric,
    max_distance numeric,
    created_at timestamp with time zone not null default current_timestamp
);