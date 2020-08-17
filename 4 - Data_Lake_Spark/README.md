# Data Lake Using Spark

## Introduction

Spariky provides music streaming data to its users. Due to increasing demand of Sparkify, they has grown their user and song database even more so they want to move their data warehouse to a data lake.
Their data is available on S3, in a form of nested JSON file.

In this project we will build an ETL pipeline for extracts their data from the data lake which is hosted on S3, processes them using Spark which will be deployed on an EMR cluster using AWS, and load the data back into S3 as a set of dimensional tables in parquet format. 

### Source Data
- **Song datasets**: all json files are nested in subdirectories under *s3a://udacity-dend/song_data*.

```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

- **Log datasets**: all json files are nested in subdirectories under *s3a://udacity-dend/log_data*.

```
{"artist":"Slipknot","auth":"Logged In","firstName":"Aiden","gender":"M","itemInSession":0,"lastName":"Ramirez","length":192.57424,"level":"paid","location":"New York-Newark-Jersey City, NY-NJ-PA","method":"PUT","page":"NextSong","registration":1540283578796.0,"sessionId":19,"song":"Opium Of The People (Album Version)","status":200,"ts":1541639510796,"userAgent":"\"Mozilla\/5.0 (Windows NT 6.1) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"","userId":"20"}
```

## ETL pipeline

1. First Load credentials of user from AWS

2. Read data from S3

    - Song data: `s3://udacity-dend/song_data`
    - Log data: `s3://udacity-dend/log_data`

3. Process data using spark

    Using data to create four dimension tables like `songs, time, users, artists` and one fact table     `songplays`.
    
4. Load it back to S3

   Writes that to partitioned parquet files in table on S3.
   
5. Create S3 Bucket

   Create bucket `project-datalake-udacity` on S3, where output results will be stored.
   
## How to run

Add Credential of AWS user in `dl.cfg` file :

```
KEY=YOUR_AWS_ACCESS_KEY
SECRET=YOUR_AWS_SECRET_KEY
```

Finally, run the command `python etl.py` on terminal.
