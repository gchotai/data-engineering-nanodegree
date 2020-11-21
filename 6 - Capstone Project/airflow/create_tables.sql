CREATE TABLE public.city_temp_from_bucket (
   dt varchar(256) NOT NULL,
   averagetemperature varchar(256),
   city varchar(256),
   country varchar(256),
   latitude varchar(256),
   longitude varchar(256)
);

CREATE TABLE public.city_temp (
   averagetemperature varchar(256),
   city varchar(256),
   country varchar(256)
);

CREATE TABLE public.immigration_from_bucket (
   i94yr varchar(256),
   i94mon varchar(256),
   i94cit varchar(256),
   i94port varchar(256),
   arrdate varchar(256),
   i94mode varchar(256),
   depdate varchar(256),
   i94visa varchar(256)
);

CREATE TABLE public.immigration (
   i94yr varchar(256),
   i94mon varchar(256),
   i94cit varchar(256),
   i94port varchar(256),
   i94mode varchar(256),
   i94visa varchar(256)
);


CREATE TABLE public.cities_demographics_from_bucket (
   city varchar(256),
   state varchar(256),
   male_population varchar(256),
   female_population varchar(256),
   total_population varchar(256),
   state_code varchar(256)
);

CREATE TABLE public.cities_demographics (
   city varchar(256),
   male_population varchar(256),
   female_population varchar(256),
   total_population varchar(256),
   state_code varchar(256)
);

CREATE TABLE public.airport_code_from_bucket (
   code varchar(256),
   name varchar(256),
   continent varchar(256),
   country varchar(256),
   state varchar(256),
   municipality varchar(256)
);

CREATE TABLE public.airport_code (
   code varchar(256),
   name varchar(256),
   country varchar(256),
   state varchar(256)
);

CREATE TABLE public.immigration_city_temp (
   i94yr varchar(256),
   i94mon varchar(256),
   i94cit varchar(256),
   i94mode varchar(256),
   i94visa varchar(256),
   averagetemperature varchar(256),
   City varchar(256),
   Country varchar(256)
);

