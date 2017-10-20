# Carto Waze Lambda Connector

## Waze
Developed according to "Waze Traffic-data Specification Document (Version 2.7.2)".

## Usage

Update function

```bash
$ serverless deploy -v --stage prod
```

Run from command line

```bash
$ serverless invoke -f carto-waze-lambda-srv -l --stage prod
```

Run function without AWS Lambda to test:
```bash
$ python3 run_handler_dev.py
```