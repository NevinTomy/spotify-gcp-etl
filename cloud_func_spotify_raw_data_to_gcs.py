import functions_framework

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from google.cloud import storage
import json
import os
from datetime import datetime

# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def spotify_raw_data_to_gcs(cloud_event):
    client_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')

    client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)

    playlist_link = "https://open.spotify.com/playlist/37i9dQZEVXbNG2KDcFcKOF?si=735310521cd048dd"
    playlist_URI = playlist_link.split('/')[-1].split('?')[0]

    raw_data = sp.playlist_tracks(playlist_URI)

    filename = 'spotify_raw_' + str(datetime.now()) + '.json'

    bucket_name = 'spotify-api-project-nevin'
    blob_name = 'raw_data/processing/' + filename

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(data=json.dumps(raw_data),content_type='application/json')