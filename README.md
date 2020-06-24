# Sparkify Data Warehouse

## Introduction 
Sparkify is music streaming startup,they have grown their user base and song database and want to move their processes and data onto the cloud. The data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. 

This project is made to builde an ETL pipeline that extractsdata from S3, stages them in Redshift, and transforms data into a set of dimensional tables for the analytics team to continue finding insights in what songs their users are listening to. 

## Files discription:

* `create_table.py` create fact and dimension tables for the star schema in Redshift.
* `etl.py`  load data from S3 into staging tables on Redshift and then process that data into your analytics tables on Redshift. 
* `sql_queries.py` define SQL statements, which will be imported into the two other files above.
* `dwh.cfg` configuration and settings file 
