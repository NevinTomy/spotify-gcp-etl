# spotify-gcp-etl

## Description
A pipeline is designed and developed using GCP services to extract Top 50 Global songs playlist data from Spotify using the Spotify API, transform the extracted data and load the transformed into BigQuery table. The pipeline is scheduled to run once a week to extract the updated playlist data from spotify.

## Architecture Diagram
![spotify GCP etl architecture diagram](https://github.com/NevinTomy/spotify-gcp-etl/blob/main/spotify-gcp-etl.jpeg)

## Python Packages 
* spotipy
* google.cloud.storage
* pandas
* google.cloud.bigquery
* datetime
* json
* io
* os

## GCP Services
* Cloud Scheduler
* Pub/Sub
* Cloud Functions
* Cloud Storage
* Bigquery

## Working
1. A cron job is set using cloud scheduler which triggers the Pub/Sub topic every week on Firday at 7PM IST.
2. The Pub/Sub topic initiates the first cloud function job(cloud_func_spotify_raw_data_to_gcs.py) which extracts the raw data from Spotify API using the spotipy python package and the extracted data is stored in GCS bucket as a csv file.
3. Once the raw csv data is availbale in GCS the second cloud fucntion job(cloud_func_spotify_data_transform.py) is triggered which extracts the songs, albums and artists data from the raw data. Datatype transformations are performed on the extracted data and 3 new csv files are uploaded onto the GCS bucket(tracks/albums/artists csv files).
4. The data is then loaded into bigquery tables(track/album/artist) from the final csv files(track/album/artist).
5. This data can then be used later for analysis. 
