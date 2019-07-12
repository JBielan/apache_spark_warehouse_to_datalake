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
****DB graph

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
    
  
