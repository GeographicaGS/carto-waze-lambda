# Carto Waze Lambda Connector

Developed according to "Waze Traffic-data Specification Document (Version 2.7.2)".

## Usage

### 1. Prepare config

Prepare config file with Carto and Waze credentials:

```
$ cp config.example.env config.env
```

### 2. Carto Data Model creation

Load to Carto this function: ```db/plpgsql/traffico_create_tables.sql```.

Execute the function in Carto:

```sql
SELECT traffico_create_tables(
     'mycity', clean_tables:=FALSE
    );
```

This function creates 3 tables and 3 materialized views:
- {mycity}_waze_data_alerts
- {mycity}_waze_data_jams
- {mycity}_waze_data_irrgs
- {mycity}_waze_data_alerts_mv
- {mycity}_waze_data_jams_mv
- {mycity}_waze_data_irrgs_mv

### 3. AWS Lambda deploy

Create serverless YAML config file:

```
$ cp serverless.example.yml serverless.yml
```

Change service name with your city prefix in new YAML file:

```yml
service: carto-waze-lambda-mycity
```

AWS Lambda deploy function:

```
$ serverless deploy -v --stage prod
```

AWS Lambda  invoke function:

```
$ serverless invoke -f georss -l --stage prod
```

AWS Lambda update function (without AWS CloudFormation because is slow):

```
$ serverless deploy function -f georss --stage prod -v
```

## Development

Run function without AWS Lambda (only for development purpose):

```
$ python3 run_handler_dev.py
```
