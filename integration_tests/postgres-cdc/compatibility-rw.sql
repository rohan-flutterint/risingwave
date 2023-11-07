DROP TABLE IF EXISTS postgres_all_types;
CREATE TABLE IF NOT EXISTS postgres_all_types(
c_boolean boolean,
c_smallint smallint,
c_integer integer,
c_bigint bigint,
c_decimal decimal,
c_real real,
c_double_precision double precision,
c_varchar varchar,
c_bytea bytea,
c_date date,
c_time time,
c_timestamp timestamp,
c_timestamptz timestamptz,
c_interval interval,
c_jsonb jsonb,
c_boolean_array boolean[],
c_smallint_array smallint[],
c_integer_array integer[],
c_bigint_array bigint[],
c_decimal_array decimal[],
c_real_array real[],
c_double_precision_array double precision[],
c_varchar_array varchar[],
c_bytea_array bytea[],
c_date_array date[],
c_time_array time[],
c_timestamp_array timestamp[],
c_timestamptz_array timestamptz[],
c_interval_array interval[],
c_jsonb_array jsonb[],
PRIMARY KEY (c_boolean,c_bigint,c_date)
) WITH (
connector = 'postgres-cdc',
hostname = 'postgres',
port = '5432',
username = 'myuser',
password = '123456',
database.name = 'mydb',
schema.name = 'public',
table.name = 'postgres_all_types',
slot.name = 'postgres_all_types'
);