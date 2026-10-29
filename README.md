# Redpanda Iceberg topics with SQL
## enable Iceberg

 * reference: https://docs.redpanda.com/cloud-data-platform/manage/iceberg/iceberg-topics-aws-glue/

```
# Glue requires Redpanda Iceberg tables to be manually deleted
# so iceberg_delete is set to false.
rpk cloud login

rpk profile create --from-cloud <cluster-id>

rpk cluster config set \
  iceberg_enabled=true \
  iceberg_delete=false \
  iceberg_default_catalog_namespace='["<custom-namespace>"]' \
  iceberg_catalog_type=rest \
  iceberg_rest_catalog_endpoint=https://glue.<glue-region>.amazonaws.com/iceberg \
  iceberg_rest_catalog_authentication_mode=aws_sigv4 \
  iceberg_rest_catalog_credentials_source=sts \
  iceberg_rest_catalog_aws_region=<glue-region> \
  iceberg_rest_catalog_base_location=s3://<cluster-storage-bucket-name>/<warehouse-path>
```

## env setup

```shell
curl -LsSf https://astral.sh/uv/install.sh | sh
uv python install
uv venv --python 3.13 .venv
uv pip install "confluent-kafka[avro]" duckdb pyarrow
```


