CREATE TYPE color AS ENUM ('red', 'green', 'blue');

CREATE TYPE mood AS ENUM ('happy', 'sad', 'amused');

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
    k timestamp without time zone,
    l color,
    m mood,
    n time without time zone
)
