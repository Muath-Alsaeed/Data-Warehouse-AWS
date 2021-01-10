Project: Data Warehouse in Amazon Redshift.
-------------------------------------------------------

Introduction:
---------------------------------

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The goal of the project:
-----------------------------------------
In this project we building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

Project Datasets:
-----------------------------------------------------


Song data: s3://udacity-dend/song_data
Log data: s3://udacity-dend/log_data

Schema for project:
-------------------------

![image](https://user-images.githubusercontent.com/52973147/100525293-948a9200-31d0-11eb-9f14-57b78d5aa3ae.png)



To run the Python scripts:
----------------------------------------
1- run lack.ipynp to create the infrastructure of aws.
2- run create_table.py to create your fact and dimension tables for the star schema in Redshift.
3-run etl.py to load data from S3 into staging tables on Redshift and then process that data into your analytics tables on Redshift.
