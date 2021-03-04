CREATE TYPE color AS ENUM ('red', 'green', 'blue');

CREATE TABLE many_types (
    test_null text,
    test_not_null text NOT NULL,
    test_bool boolean,
    test_bool_array boolean[],
    test_date date,
    test_date_array date[],
    -- (PostgreSQL can't import these yet because they require a very precise
    -- parsing algorithm we'll need to port from C.)
    --
    -- test_decimal numeric,
    -- test_decimal_array numeric[],
    test_float32 real,
    test_float32_array real[],
    test_float64 double precision,
    test_float64_array double precision[],
    test_geojson public.geometry (Geometry, 4326),
    -- PostgreSQL can't import this yet because it requires an OID that changes
    -- between servers.
    --
    -- test_geojson_array public.geometry(Geometry, 4326)[],
    test_geojson_3857 public.geometry (Geometry, 3857),
    test_int16 smallint,
    test_int16_array smallint[],
    test_int32 int,
    test_int32_array int[],
    test_int64 bigint,
    test_int64_array bigint[],
    test_json jsonb,
    -- Our BigQuery import code needs some work.
    --
    -- test_json_array jsonb[],
    test_text text,
    test_text_array text[],
    test_timestamp_without_time_zone timestamp,
    test_timestamp_without_time_zone_array timestamp[],
    test_timestamp_with_time_zone timestamp WITH time zone,
    test_timestamp_with_time_zone_array timestamp WITH time zone[],
    test_uuid uuid,
    test_uuid_array uuid[],
    test_enum color
    -- This would require the ability to fetch PostgreSQL OIDs from the
    -- database, which we don't have.
    --
    -- test_enum_array color[]
)
