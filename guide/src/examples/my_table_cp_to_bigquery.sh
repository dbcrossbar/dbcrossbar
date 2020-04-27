dbcrossbar config add temporary gs://$GS_TEMP_BUCKET
dbcrossbar config add temporary bigquery:$GCLOUD_PROJECT:temp_dataset
dbcrossbar cp \
    --if-exists=upsert-on:id \
    'postgres://postgres@127.0.0.1:5432/postgres#my_table' \
    bigquery:$GCLOUD_PROJECT:my_dataset.my_table
