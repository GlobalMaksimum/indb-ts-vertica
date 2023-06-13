create schema if not exists nyc;

drop table nyc.bike_trip ;
create TABLE  nyc.bike_trip(
  trip_duration int,
  starttime varchar,
  stoptime varchar,
  start_station_id int,
  start_station_name varchar,
  start_station_latitude float,
  start_station_longitude float,
  end_station_id int,
  end_station_name varchar,
  end_station_latitude float,
  end_station_longitude float,
  bikeid int,
  usertype varchar,
  birth_year int,
  gender int
) order by start_station_id, starttime
segmented by HASH(start_station_id) all nodes;


--starttime_f filler varchar,
--starttime as to_timestamp(starttime_f, 'MM/DD/YYYY HH24:MI:SS'),
--stoptime_f filler varchar,
--stoptime as to_timestamp(stoptime_f, 'MM/DD/YYYY HH24:MI:SS'),

truncate table  nyc.bike_trip;
COPY nyc.bike_trip(
trip_duration,
starttime_f filler varchar,
starttime as case when starttime_f like '%-%' then to_timestamp(starttime_f,'YYYY-MM-DD HH24:MI:SS')
else to_timestamp(starttime_f,'MM/DD/YYYY HH24:MI:SS') end,
stoptime_f filler varchar,
stoptime as case when stoptime_f like '%-%' then to_timestamp(stoptime_f,'YYYY-MM-DD HH24:MI:SS')
else to_timestamp(stoptime_f,'MM/DD/YYYY HH24:MI:SS') end,
start_station_id,
start_station_name,
start_station_latitude,
start_station_longitude,
end_station_id,
end_station_name,
end_station_latitude,
end_station_longitude,
bikeid,
usertype,
birth_year_f filler varchar,
birth_year as birth_year_f::!int,
gender
) FROM 's3://share/nyc-trip/2013*.parquet',
's3://share/nyc-trip/201401.parquet',
's3://share/nyc-trip/201402.parquet',
's3://share/nyc-trip/201403.parquet',
's3://share/nyc-trip/201404.parquet',
's3://share/nyc-trip/201405.parquet',
's3://share/nyc-trip/201406.parquet',
's3://share/nyc-trip/201407.parquet',
's3://share/nyc-trip/201408.parquet'
 PARQUET
ENFORCELENGTH REJECTMAX 1;
-- Time: First fetch (1 row): 15882.080 ms.All rows formatted: 15882.148 ms

COPY nyc.bike_trip(
trip_duration,
starttime_f filler varchar,
starttime as case when starttime_f like '%-%' then to_timestamp(starttime_f,'YYYY-MM-DD HH24:MI:SS')
else to_timestamp(starttime_f,'MM/DD/YYYY HH24:MI:SS') end,
stoptime_f filler varchar,
stoptime as case when stoptime_f like '%-%' then to_timestamp(stoptime_f,'YYYY-MM-DD HH24:MI:SS')
else to_timestamp(stoptime_f,'MM/DD/YYYY HH24:MI:SS') end,
start_station_id,
start_station_name,
start_station_latitude,
start_station_longitude,
end_station_id,
end_station_name,
end_station_latitude,
end_station_longitude,
bikeid,
usertype,
birth_year_f filler float,
birth_year as birth_year_f::!int,
gender
) FROM 's3://share/nyc-trip/201409.parquet',
's3://share/nyc-trip/201410.parquet',
's3://share/nyc-trip/201411.parquet',
's3://share/nyc-trip/201412.parquet',
's3://share/nyc-trip/2015*.parquet',
's3://share/nyc-trip/2016*.parquet'
PARQUET
ENFORCELENGTH REJECTMAX 1;


-- select year( case when starttime like '%-%' then to_timestamp(starttime,'YYYY-MM-DD HH24:MI:SS')
-- else to_timestamp(starttime,'MM/DD/YYYY HH24:MI:SS') end ),count(1)
-- from nyc.bike_trip group by 1;

