
import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd
import boto3 
from datetime import datetime

def lambda_handler(event, context):
    client_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')
    
    client_credentials_manager=SpotifyClientCredentials(client_id =client_id ,client_secret =client_secret )
    sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)
    
    playListLink = "https://open.spotify.com/playlist/4hOKQuZbraPDIfaGbM3lKI"
    playListUri = playListLink.split('/')[-1]
    
    SpotifyData = sp.playlist_tracks(playListUri)
    
    client = boto3.client('s3')
    fileName = "spotify_row_" + str(datetime.now()) + ".json"
    
    client.put_object(
        Bucket = "spotify-project-kunal",
        Key = "row_data/to_processed/"+ fileName,
        Body = json.dumps(SpotifyData) )
    # print(Data)
    

