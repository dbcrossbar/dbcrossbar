CREATE EXTENSION IF NOT EXISTS postgis;

DROP TABLE IF EXISTS copy_binary_test;

CREATE TABLE copy_binary_test (
    test_null smallint,
    test_array_int32 integer[],
    test_bool boolean,
    test_date date,
    test_float32 real,
    test_float64 double precision,
    test_geo geometry(point, 4326),
    test_i16 smallint,
    test_i32 integer,
    test_i64 bigint
);

\copy copy_binary_test FROM 'pg_copy_binary.bin' WITH BINARY

SELECT * FROM copy_binary_test;

