CREATE TABLE many_types (
    test_null text,
    test_not_null text NOT NULL,
    test_bool boolean,
    test_date date,
    test_float32 real,
    test_float64 double precision,
    test_int16 smallint,
    test_int32 int,
    test_int64 bigint,
    test_text text,
    test_timestamp_without_time_zone timestamp,
    test_timestamp_with_time_zone timestamp WITH time zone
);

