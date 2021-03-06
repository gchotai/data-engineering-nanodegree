# Building a Data Warehouse ETL pipeline for Sparkify

## Introduction
Spariky provides music streaming data to its users. The songs details and the user activity details are collected and stored in
the form of json files in S3. The goal of the current project is to build an ETL pipeline that extracts their data from S3, 
stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to 
continue finding insights in what songs their users are listening to.

## Datasets

### Log Dataset

{"artist":"Pavement", "auth":"Logged In", "firstName":"Sylvie", "gender", "F", "itemInSession":0, "lastName":"Cruz", "length":99.16036, "level":"free", "location":"Klamath Falls, OR", "method":"PUT", "page":"NextSong", "registration":"1.541078e+12", "sessionId":345, "song":"Mercy:The Laundromat", "status":200, "ts":1541990258796, "userAgent":"Mozilla/5.0(Macintosh; Intel Mac OS X 10_9_4...)", "userId":10}

### Song Dataset

{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}


## Database schema design
Prepared database schema design and ETL pipeline.

####  Fact Table
- songplays - records in event data associated with song plays i.e. records with page NextSong - 
*songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent*

#### Dimension Tables
- users - users in the app - 
*user_id, first_name, last_name, gender, level*
- songs - songs in music database - 
*song_id, title, artist_id, year, duration*
- artists - artists in music database - 
*artist_id, name, location, lattitude, longitude*
- time - timestamps of records in songplays broken down into specific units - 
*start_time, hour, day, week, month, year, weekday*

##  Infrastructure provisioning

There are `create_redshift_cluster.ipynb` that will ease our job to create our data warehouse infrastructure:
##### 1. Creating a IAM ROLE
##### 2. Creating a Redshift Cluster
##### 2.1 Checking the cluster availability 
##### 2.2 Take note of the cluster
##### 3. Open an incoming TCP port to access the cluster ednpoint
##### 4. Make sure you can connect to the clusterConnect to the cluster
##### 5.  Clean up your resources

_After the ETL process done, don't forgot to run last command_

## The ETL Process

It consists of these two simple python scripts:

 - `python create_tables.py` - It will drop the tables if exists, and then create it (again);
 - `python etl.py` - This script does two principal tasks:
     - Copy (load) the logs from the dataset's S3 bucket to the staging tables;
     - Translate all data from the staging tables to the analytical tables with `INSERT ... SELECT` statements.

## Analyzing the results

After the ETL process completion we can check if we did it right by running the `python analytics.py`.

It is a simple script to return the counting of each analytical table.

## Required Steps to run the project 

1) Configuration setup - Fill the dwh.cfg with the necessary information to start a redshift cluster
2) create_redshift_cluster.ipynb - Run this jupyter notebook and create the cluster
3) Run create_tables.py - Use this python file to drop and create tables
4) Run etl.py - Run this python file to create the etl pipeline to insert data into the created tables.
5) Run analytics.py - This python file to run some basic analytical queries.

#### Don't forget to run the last steps in the file 'create_redshift_cluster.ipynb' jupyter notebook to delete the cluster.