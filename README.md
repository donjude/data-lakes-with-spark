# data-lakes-with-spark

## Project Summary

Sparkify is a music straming startup that has grown their user base and song database and would want to move their data warehouse to a data lake in the cloud. Their data consist of user activities on using the music streaming up as well as metadata on the songs in their app. These data are currently stored on Amazon s3 storage bucket on Amazon Web Service.

Sparkify requires an ETL pipeline to be built that extracts their data from s3, process them using Spark and loads the data back into s3 as a set of dimensional tables. This would help their analytics team to continue finding insights in what songs their users are listening to. The database and ETL pipeline would be tested by running queries given by Sparkify analytics team and compare results with their expected results.

## Project Description

In this Project an ETL pipeline would be built using Apache Spark for a data lake data warehouse hosted on s3. Amazon Elastic MapReduce (EMR) Spark Cluster on Amazon Webservice(AWS) cloud computing platform would be used for the processing of the data. The ETL pipleine would load data from s3, process the data into analytics tables using Elastic MapReduce (EMR) Spark cluster, and load them back into s3.

## Datasets

There are two datasets:

- **Song datasets**: Each file is in JSON format and contains metadata about a song and the artist of that song.

    **Location:** `s3://udacity-dend/song_data`
    
    **Sample:**
```JSON
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

- **Log datasets**: The second dataset consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate activity logs from a music streaming app are based on specified configurations.

    **Location:** `s3://udacity-dend/log_data` <br/>
    **log data JSON path:** `s3://udacity-dend/log_json_path.json`
    
    
    **Sample:**
    
```JSON
{"artist":null, "auth":"Logged In", "firstName":"Walter", "gender":"M", "itemInSession":0, "lastName":"Frye", "length":null, "level":"free", "location":"San Francisco-Oakland-Hayward, CA", "method":"GET", "page":"Home", "registration":1540919166796.0, "sessionId":38,"song":null, "status":200, "ts":1541105830796, "userAgent":"\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"", "userId":"39"}
```


## Database Schema
The database schema consist of a star schema optimized for queries on song play analysis. One fact table and four dimensional tables were created as seen below:

### Fact table: songplay

```
root
 |-- start_time: timestamp (nullable = true)
 |-- user_id: string (nullable = true)
 |-- level: string (nullable = true)
 |-- song_id: string (nullable = true)
 |-- artist_id: string (nullable = true)
 |-- session_id: long (nullable = true)
 |-- location: string (nullable = true)
 |-- user_agent: string (nullable = true)
 |-- year: string (nullable = true)
 |-- songplay_id: long (nullable = false)
 ```

 ### Dimension table: songs

 ```
 root
 |-- song_id: string (nullable = true)
 |-- title: string (nullable = true)
 |-- artist_id: string (nullable = true)
 |-- year: long (nullable = true)
 |-- duration: double (nullable = true)
 ```

 ### Dimension table: artists

 ```
 root
 |-- artist_id: string (nullable = true)
 |-- artist_name: string (nullable = true)
 |-- artist_location: string (nullable = true)
 |-- artist_latitude: double (nullable = true)
 |-- artist_longitude: double (nullable = true)
 ```

 ### Dimension table: users

 ```
root
 |-- userId: string (nullable = true)
 |-- firstName: string (nullable = true)
 |-- lastName: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- level: string (nullable = true)
 ```


 ### Dimension table: time

```
 root
 |-- start_time: timestamp (nullable = true)
 |-- hour: integer (nullable = true)
 |-- day: integer (nullable = true)
 |-- week: integer (nullable = true)
 |-- month: integer (nullable = true)
 |-- year: long (nullable = true)
 |-- weekday: integer (nullable = true)
```

## Project Setup

The project consist of the script below:

- dl.cfg - configuration script which should contain your amazon IAM ID credentials.
```
[AWS]
AWS_ACCESS_KEY_ID = replace with amazon IAM ID
AWS_SECRET_ACCESS_KEY = replace with amazon IAM secret key
```

- etl.py - Pyspark script to run the etl process.

## Run Scripts

To run the script:

1. Make sure you have or have created an Amazon webservice account.
1. Provision an Elastic Map Reduce (EMR) compute cluster
1. Log in to the EMR cluster using your local compute bash shell.
1. Copy the etl.py and dl.cfg files onto the /home/hadoop directory of the cluster.
1. execute the below code in the bash shell to run the etl process.

    ```python
    $ spark-submit etl.py
    ```