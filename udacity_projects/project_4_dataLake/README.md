# Project Overview 

Sparkify, a music streaming startup, has grown their user base and this has increased the song database to grow massively and the companywant to move their data warehouse to a data lake. 

This project is about designing an ETL pipeline that will extract the data which is currently in JSON files stored in S3 Bucket. This project will involve the data extraction from S3, processing the data using Spark in ASW EMR an load the processed/transformed data back into the S3 Bucket.

# ETL Pipeline

The ETL pipleline involves loading a subset of the data from S3 Bucket 's3a://udacity-dend/'. I used a subset of the data because the entire datasets takes a long time to load.

The data is later processed using spark by loading them into dimensions tables and fact table.

The processed data were load back into partitioned parquet files.

# Running the ETL Pipeline

I ran the ETL complete ETL pipeline by using the execute.ipynb