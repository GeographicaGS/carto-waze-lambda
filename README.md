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
    'mycity',
    clean_tables:=FALSE,
    create_mviews:=TRUE,
    create_historic_agg_tables:=FALSE
);
```

In some cases you will need to specify the CARTO username, if the query above
fails then execute this:

```sql
SELECT traffico_create_tables(
    'mycity',
    'mycartousername',
    clean_tables:=FALSE,
    create_mviews:=TRUE,
    create_historic_agg_tables:=FALSE
);
```

This function creates 3 tables and 3 materialized views:
- {mycity}_waze_data_alerts
- {mycity}_waze_data_jams
- {mycity}_waze_data_irrgs
- {mycity}_waze_data_alerts_mv
- {mycity}_waze_data_jams_mv
- {mycity}_waze_data_irrgs_mv

If you wish to create historic tables, set `create_historic_agg_tables` to `TRUE`.
The historic tables are:

- `{mycity}_waze_data_jams_agg_hour`: contains aggregated information of jams and irregularities
  grouped by road segment.
- `{mycity}_waze_data_jams_agg_times`: contains information about start and end times of jams
  and irregularities.

**Note: The tables that compose the historic model are adapted to the use cases of the AMB
project. For other cities this model will require additional changes.**

### 2.b Google Big Query model (for historic purposes)

Data processing of historic models is done with Big Query. The `db/big_query` directory
contains the necessary DDL scripts for replicating the Postgres model in Big Query and
store historic data.

The `db/big_query/jobs` directory contains the necessary scheduled queries/jobs
used to populate the aggregated historic data.

**Note: tables related to historic data are adapted to the uses cases of the AMB project.
For other cities aggregated models and jobs related to historic data will require additional changes.**

### 3. AWS Lambda deploy

Create serverless YAML config file:

```
$ cp serverless.example.yml serverless.yml
```

Change service name with your city prefix in new YAML file:

```yml
service: carto-waze-lambda-mycity
```

In order to enable the storage of historic data in Big Query you will need to set
the appropiate environment variables in the `serverless.yml` file and provide a
Google Cloud Application Credentials (`gcloud-credentials.json`) in the root directory.
During deployment, the credentials file will be included in the lambda package with the code.

#### AWS Lambda deploy function:

```
$ serverless deploy -v --stage prod
```

#### AWS Lambda invoke function:

```
$ serverless invoke -f {georss, daily-aggs} -l --stage prod
```

#### AWS Lambda update function (without AWS CloudFormation because is slow):

```
$ serverless deploy function -f {georss, daily-aggs} --stage prod -v
```

## Development

Run function without AWS Lambda (only for development purpose):

```
$ python3 run_handler_dev.py {georss, daily-aggs}
```
