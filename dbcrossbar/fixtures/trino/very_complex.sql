CREATE TABLE "very_complex" (
    "nested" ROW(
        "test_null" VARCHAR,
        "test_bool" BOOLEAN,
        "test_bool_array" ARRAY(BOOLEAN),
        "test_date" DATE,
        "test_date_array" ARRAY(DATE),
        "test_float32" REAL,
        "test_float64" DOUBLE,
        "test_float64_array" ARRAY(DOUBLE),
        "test_geojson" SphericalGeography,
        "test_geojson_array" ARRAY(SphericalGeography),
        -- Does not exist in portable schema, so it "upgrades" to SMALLINT.
        --"test_int8" TINYINT,
        "test_int16" SMALLINT,
        "test_int32" INTEGER,
        "test_int64" BIGINT,
        "test_int64_array" ARRAY(BIGINT),
        "test_json" JSON,
        "test_text" VARCHAR,
        "test_text_array" ARRAY(VARCHAR),
        "test_timestamp_without_time_zone" TIMESTAMP,
        "test_timestamp_without_time_zone_array" ARRAY(TIMESTAMP),
        "test_timestamp_with_time_zone" TIMESTAMP WITH TIME ZONE,
        "test_timestamp_with_time_zone_array" ARRAY(TIMESTAMP WITH TIME ZONE),
        "test_uuid" UUID,
        "test_uuid_array" ARRAY(UUID),
        "record" ROW("nested" ROW("i1" BIGINT)),
        "records" ARRAY(ROW("i2" BIGINT))
    ),
    -- These aren't in many_types.sql.
    "test_geojson_array" ARRAY(SphericalGeography),
    "record" ROW("nested" ROW("i1" BIGINT)),
    "records" ARRAY(ROW("i2" BIGINT))
)
