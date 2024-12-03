-- Maybe https://github.com/trinodb/trino/pull/23649 ?
CREATE TABLE "very_complex" (
    "record" ROW("nested" ROW("i" BIGINT))
)
