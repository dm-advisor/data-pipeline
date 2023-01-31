# data-pipeline
A repository of sample code to conceptualize a cloud-based data pipeline.
The primary puprose of this repository is to provide sample code for implementating a data pipeline to ingest, tranform and curate data into a data lake for BI reporting and analytics consumption using the Amazon Web Services (AWS) native services.

A depicted in the diagram below, the sample use case involves the following processes:
1. Ingesting files into AWS Simple Storage Service (S3) object store in raw form
2. Transforming the ingested data 
3. Curating and shaping data for BI reporting and analysis consumption.

![data-pipeline](https://user-images.githubusercontent.com/123999086/215606241-d0526b38-c795-4d93-9ef5-a65aea1807ac.jpg)

## Scope of POC
This proof-of-concept involves creating an ETL pipeline for ingesting the de-identified student enrollment campus files and making the data available for access through SQL queries.
Following is the list of steps included in the pipeline:
1. An AWS Lambda function that detects when a file is placed in the landing zone and moves the file under the corresponding folder in the incoming zone (sdap-poc-incoming s3 bucket).
2. Five Athena tables that represent the incoming data in full fidelity as provided by campuses. Each of the tables represents the structure of each campus file type: 3rd week, EOT, summer 3rd week, summer EOT and residents.
3. A Python AWS Glue job that converts the data in the incoming zone to Parquet format and writes the output to the corresponding folder in the processing zone (sdap-poc-processing s3 bucket).
4. Five Athena tables that represent the incoming data that has been transformed to the correct data types and in columnar format.
5. Allowing end-users to connect to AWS and query the data in Athena tables using the tool of their choice (DbVisualzer).

## Technical Details
- The git repository for this project is comprised of the following directories under sdap-poc root: 
    - CLI: contains the AWS command line interface scripts
    - DDL: contains the table create statements of all Athena tables.
    - ETL: contains the Python source code used for the various components of the pipeline.
    - SQL: contains the sample SQL for querying the Athena tables.
