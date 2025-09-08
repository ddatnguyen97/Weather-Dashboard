CREATE TABLE "hourly_weather_data" (
  "id" varchar(20) PRIMARY KEY,
  "date_id" varchar(10),
  "time_id" varchar(10),
  "relative_humidity_2m" numeric,
  "dew_point_2m" numeric,
  "apparent_temperature" numeric,
  "precipitation" numeric,
  "weather_code" varchar(10),
  "cloud_cover" numeric,
  "wind_speed_10m" numeric,
  "wind_direction_10m" numeric,
  "wind_gusts_10m" numeric,
  "is_day" varchar(10),
  "sunshine_duration" numeric
);

CREATE TABLE "weather_code" (
  "id" varchar(10) PRIMARY KEY,
  "name" varchar(50)
);

CREATE TABLE "times_of_day" (
  "id" varchar(10) PRIMARY KEY,
  "name" varchar(20)
);

CREATE TABLE "dim_date" (
  "id" varchar(10) PRIMARY KEY,
  "date" date,
  "year" int,
  "quarter" int,
  "month" int,
  "day" int
);

CREATE TABLE "dim_time" (
  "id" varchar(10) PRIMARY KEY,
  "time" time,
  "hour" int
);

ALTER TABLE "hourly_weather_data" ADD FOREIGN KEY ("weather_code") REFERENCES "weather_code" ("id");

ALTER TABLE "hourly_weather_data" ADD FOREIGN KEY ("is_day") REFERENCES "times_of_day" ("id");

ALTER TABLE "hourly_weather_data" ADD FOREIGN KEY ("date_id") REFERENCES "dim_date" ("id");

ALTER TABLE "hourly_weather_data" ADD FOREIGN KEY ("time_id") REFERENCES "dim_time" ("id");
