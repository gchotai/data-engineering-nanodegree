# Sparkify song play logs ETL process


This project consists following concepts:
-   Data modeling with Postgres
-   Database star schema 
-   ETL pipeline using Python

## Context

This project extract, transform and load data into below tables from Sparkify app logs, we would use the STAR schema like this:

 - ```users``` - DIMENSION Table
 - ```songs``` - DIMENSION Table
 - ```artists``` - DIMENSION Table
 - ```time``` - DIMENSION Table
 - ```songplays``` - FACT Table
 
The analytic team is particularly interested in understanding what songs users are listening to, but would like a data engineer to create a Postgres database with tables designed to optimize queries on song play analysis.

Here we got data in JSON files, which can take time to analysis. Our focus is to import JSON data into a Postgres database and use modeling techniques to allow fast retrieval of data. In this case, we will use the STAR schema.

The STAR schema consists of one or more FACT tables referencing any number of DIMENSION tables. These tables can help Sparkify solve simplified common business logic. This logic includes:
- What song should play next for the Sparkify user, based on past behavior.
- Which song an user would be interested in listening to at that particular point of time.

The above databases will help the analytics team at Sparkify to run different kinds of analysis to recommend a Sparkify user.
- Favorite songs of user based on the week day: By joining songplays and songs and user table based on level.
- Recent listened to songs: By joining songplays and user table can show recommendation on the app based on subscription level.
- Help in recommending most popular songs of the day/week.

## ETL Pipeline

- Create FACT table from the dimension tables and log_data called songplays.
- Create DIMENSION songs and artist table from extracting songs_data by selected columns.
- Create DIMENSION users and time tables from extracting log_data by selected columns.


## Usage
- `test.ipynb` used for testing purpose that table contains data or not.
- `create_tables.py` used for drop and create tables. You run this file to reset your tables before each time you run your ETL scripts.
- `etl.ipynb` read and process a single file from song_data and log_data and loads the data into your tables. This notebook contains detailed instructions on the ETL process for each of the tables.
- `etl.py` read and process files from song_data and log_data and loads them into your tables. You can fill this out based on your work in the ETL notebook.
- `sql_queries.py` contains all your sql queries.

## Execute the below files in order each time before pipeline.

1. create_tables.py $ `python create_tables.py`
2. etl.py $ `python etl.py`
3. test.ipynb