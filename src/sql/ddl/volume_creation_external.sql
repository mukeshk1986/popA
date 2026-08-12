-- Volume creation for rest all plans(non anthem & planwise)------------------------
CREATE external VOLUME IF NOT EXISTS ${catalog}.${schema_ingestion}.ingestion
LOCATION 's3://bhi-${env_bucket}-datalake-${schema_plan_name}bronze-us-east-1/${schema_ingestion}/ingestion';
