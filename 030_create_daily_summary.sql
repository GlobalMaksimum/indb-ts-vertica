drop table nyc.bike_trip_daily;

CREATE TABLE nyc.bike_trip_daily as
select
    starttime::date starttime_dt,
    count(1) n
from
    nyc.bike_trip
group by
    1
order by
    1;