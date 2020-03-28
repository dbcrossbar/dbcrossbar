dbcrossbar cp \
    --if-exists=upsert-on:id \
    --temporary=gs://$GS_TEMP_BUCKET \
    --temporary=bigquery:$GCOUD_PROJECT:temp_dataset \
    'postgres://postgres@127.0.0.1:5432/postgres#my_table' \
    bigquery:$GCOUD_PROJECT:my_dataset.my_table
