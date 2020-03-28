dbcrossbar cp \
    --if-exists=overwrite \
    --schema=postgres-sql:my_table.sql \
    csv:my_table.csv \
    'postgres://postgres@127.0.0.1:5432/postgres#my_table'
