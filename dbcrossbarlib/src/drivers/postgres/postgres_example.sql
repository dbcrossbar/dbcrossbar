-- This is an example table.
CREATE TABLE example (
    a text,
    b integer,
    c uuid NOT NULL,
    d date,
    e double precision,
    f text[],
    g integer[],
    h geometry(Geometry,4326),
    -- Just to be annoying:
    i public.geometry(Geometry,3857),
    j smallint,
    k timestamp without time zone
)
