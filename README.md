# Carto Waze Lambda Connector

Developed according to "Waze Traffic-data Specification Document (Version 2.7.2)".

## Usage

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

Run function without AWS Lambda to test:

```
$ python3 run_handler_dev.py
```