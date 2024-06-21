CREATE TABLE "images" (
    "id" UUID NOT NULL,
    "url" VARCHAR NOT NULL,
    "format" VARCHAR,
    "metadata" JSON,
    "thumbnails" ARRAY(ROW("url" VARCHAR, "width" DOUBLE, "height" DOUBLE))
)
