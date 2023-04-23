import pandas as pd
import json
from io import StringIO
from google.cloud import storage
from datetime import datetime
from google.cloud import bigquery

def track(data):
  track_list = []
  for track in data['items']:
    track_id = track['track']['id']
    track_name = track['track']['name']
    track_duration = track['track']['duration_ms']
    track_url = track['track']['external_urls']['spotify']
    track_popularity = track['track']['popularity']
    track_added = track['added_at']
    album_id = track['track']['album']['id']
    artist_id = track['track']['album']['artists'][0]['id']
    track_element = {'track_id': track_id, 'track_name': track_name, 'track_duration': track_duration, 'track_url':track_url, 'track_popularity': track_popularity, 'track_added': track_added, 'album_id': album_id, 'artist_id': artist_id}
    track_list.append(track_element)
  return track_list

def album(data):
  album_list = []
  for album in data['items']:
    album_id = album['track']['album']['id']
    album_name = album['track']['album']['name']
    album_total_tracks = album['track']['album']['total_tracks']
    album_release_date = album['track']['album']['release_date']
    album_url = album['track']['album']['external_urls']['spotify']
    album_element = {'album_id': album_id, 'album_name': album_name, 'album_total_tracks': album_total_tracks, 'album_release_date': album_release_date, 'album_url': album_url}
    album_list.append(album_element)
  return album_list
    
def artist(data):
  artist_list = []
  for row in data['items']:
    for key,value in row.items():
      if key == 'track':
        for artist in value['artists']:
          artist_id = artist['id']
          artist_name = artist['name']
          artist_url = artist['external_urls']['spotify']
          artist_element = {'artist_id': artist_id, 'artist_name': artist_name, 'artist_url': artist_url}
          artist_list.append(artist_element)
  return artist_list

def upload_blob_from_memory(bucket_name, contents, destination_blob_name):
    """Uploads a file to the bucket."""

    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"

    # The contents to upload to the file
    # contents = "these are my contents"

    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string(contents, 'txt/csv')

def mv_blob(bucket_name, blob_name, new_bucket_name, new_blob_name):
    """
    Function for moving files between directories or buckets. it will use GCP's copy 
    function then delete the blob from the old location.
    
    inputs
    -----
    bucket_name: name of bucket
    blob_name: str, name of file 
        ex. 'data/some_location/file_name'
    new_bucket_name: name of bucket (can be same as original if we're just moving around directories)
    new_blob_name: str, name of file in new directory in target bucket 
        ex. 'data/destination/file_name'
    """
    storage_client = storage.Client()
    source_bucket = storage_client.get_bucket(bucket_name)
    source_blob = source_bucket.blob(blob_name)
    destination_bucket = storage_client.get_bucket(new_bucket_name)

    # copy to new destination
    new_blob = source_bucket.copy_blob(
        source_blob, destination_bucket, new_blob_name)
    # delete in old destination
    source_blob.delete()

def spotify_data_transform(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    storage_client = storage.Client()
    bucket_name = 'spotify-api-project-nevin'
    path = 'raw_data/processing/'

    spotify_data = []
    spotify_file = []
    for file in storage_client.list_blobs(bucket_name, prefix=path):
      if file.name.split('.')[-1] == '.json':
        blob = storage_client.get_blob(bucket_name, prefix=path)
        blob_data = blob.download_as_text()
        jsonObject = json.loads(blob_data)
        spotify_data.append(jsonObject)
        spotify_file.append(file.name)

    for data in spotify_data:
      track_list = track(data)
      album_list = album(data)
      artist_list = artist(data)

      track_df = pd.DataFrame.from_dict(track_list)
      album_df = pd.DataFrame.from_dict(album_list)
      artist_df = pd.DataFrame.from_dict(artist_list)

      track_df = track_df.drop_duplicates(subset=['track_id'])
      album_df = album_df.drop_duplicates(subset=['album_id'])
      artist_df = artist_df.drop_duplicates(subset=['artist_id'])

      track_df['track_added'] = pd.to_datetime(track_df['track_added'])
      album_df['album_release_date'] = pd.to_datetime(album_df['album_release_date'])

      track_blob = 'final_data/track_data/track_data_final_' + str(datetime.now()) + '.csv'
      track_buffer = StringIO()
      track_df.to_csv(track_buffer, index=False)
      track_content = track_buffer.get_value()
      upload_blob_from_memory(bucket_name, track_content, track_blob)

      album_blob = 'final_data/album_data/album_data_final_' + str(datetime.now()) + '.csv'
      album_buffer = StringIO()
      album_df.to_csv(album_buffer, index=False)
      album_content = album_buffer.get_value()
      upload_blob_from_memory(bucket_name, album_content, album_blob)

      artist_blob = 'final_data/artist_data/artist_data_final_' + str(datetime.now()) + '.csv'
      artist_buffer = StringIO()
      artist_df.to_csv(artist_buffer, index=False)
      artist_content = artist_buffer.get_value()
      upload_blob_from_memory(bucket_name, artist_content, artist_blob)

      # BigQuery load
      bq_client = bigquery.Client()
      job_config = bigquery.LoadJobConfig(
                      source_format=bigquery.SourceFormat.CSV, 
                      skip_leading_rows=1, 
                      autodetect=True,
                      write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
      
      track_uri = 'gs://' + bucket_name + '/' + track_blob
      album_uri = 'gs://' + bucket_name + '/' + album_blob
      artist_uri = 'gs://' + bucket_name + '/' + artist_blob

      track_tbl = 'spotify-api-project-383911.spotify_db.track'
      album_tbl = 'spotify-api-project-383911.spotify_db.album'
      artist_tbl = 'spotify-api-project-383911.spotify_db.artist'

      track_load = bq_client.load_table_from_uri(track_uri, track_tbl, job_config=job_config)  # Make an API request.
      track_load.result()  # Waits for the job to complete.
      album_load = bq_client.load_table_from_uri(album_uri, album_tbl, job_config=job_config)  # Make an API request.
      album_load.result()  # Waits for the job to complete.
      artist_load = bq_client.load_table_from_uri(artist_uri, artist_tbl, job_config=job_config)  # Make an API request.
      artist_load.result()  # Waits for the job to complete.

    # File archive
    for file in spotify_file:
      new_bucket_name = 'spotify-api-project-nevin'
      new_blob_name = 'raw_data/archive/' + file.split('/')[-1]
      mv_blob(bucket_name, file, new_bucket_name, new_blob_name)







    


    
