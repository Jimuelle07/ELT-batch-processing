
# START

FROM python:3.12-slim AS base

WORKDIR /ETL-BATCH-PROCESSING

COPY . /ETL-BATCH-PROCESSING

CMD etl-batch-data-pipelines

