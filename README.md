# Apache Spark - migration from data warehouse to data lake

## Table of contents
* [Problem to Solve](#Problem-to-Solve)
* [Description](#Description)
* [Technological Stack](#Technological-Stack)
* [Database Schema](#Database-Schema)
* [Raw JSON files structure](#Raw-JSON-files-structure)
* [Prerequisites](#Prerequisites)
* [How to run?](#How-to-run)
--------------------------------------------

#### Problem to Solve
A music streaming startup, has grown their user base and song database and want to move their data warehouse to a data lake. 

#### Description
Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. The task is to build an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

#### Technological stack
- AWS S3 where JSON files are stored
- Apache Spark to process the data and insert it back to S3
- Python 3 as a bridge betweeen Apache Spark and AWS services

#### Database Schema
The database has "star schema". It has 1 facts table and 4 dimensional tables. Diagram below represents the structure:
![schema](https://github.com/JBielan/apache_spark_warehouse_to_datalake/blob/master/database_schema.png?raw=true)

#### Raw JSON files structure
- **log_data**: log_data contains data about what users have done (columns: event_id, artist, auth, firstName, gender, itemInSession, lastName, length, level, location, method, page, registration, sessionId, song, status, ts, userAgent, userId)
- **song_data**: song_data contains data about songs and artists (columns: num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, song_id, title, duration, year)

#### Prerequisites
- `Python 3` - https://www.anaconda.com/distribution/
- `pyspark` with dependencies - https://spark.apache.org/docs/latest/api/python/pyspark.sql.html

#### How to run?
As easy as 1 line in command line set to project directory:
```python
python etl.py
```

#### Example Queries
In the end of ETL process some examples of queries run to show possible usecases. 

- Get users and songs they listened at particular time. Limit query to 1000 hits:
```sql
SELECT  sp.songplay_id,
        u.user_id,
        s.song_id,
        u.last_name,
        sp.start_time,
        a.name,
        s.title
FROM songplays AS sp
        JOIN users   AS u ON (u.user_id = sp.user_id)
        JOIN songs   AS s ON (s.song_id = sp.song_id)
        JOIN artists AS a ON (a.artist_id = sp.artist_id)
        JOIN time    AS t ON (t.start_time = sp.start_time)
ORDER BY (sp.start_time)
LIMIT 1000;
```

- Get count of rows in each Dimension table:
```sql
SELECT COUNT(*)
FROM songs_table;

SELECT COUNT(*)
FROM artists_table;

SELECT COUNT(*)
FROM users_table;

SELECT COUNT(*)
FROM time_table;
```

- Get count of rows in Fact table:
```sql
SELECT COUNT(*)
FROM songplays_table;
```
    
  
