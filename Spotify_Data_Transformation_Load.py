
def lambda_handler(event, context):
import json
import boto3
import pandas as pd
from datetime import datetime
from io import StringIO

def album(Data):
    album_list = []
    for i in Data['items']:
        album_id = i['track']['album']['id'] 
        album_name = i['track']['album']['name'] 
        album_release_date = i['track']['album']['release_date']
        album_total_tracks = i['track']['album']['total_tracks']
        album_external_urls = i['track']['album']['external_urls']['spotify']
        album_element = {'album_id': album_id,'album_name':album_name,'album_release_date':album_release_date,
                         'album_total_tracks':album_total_tracks,'album_external_urls':album_external_urls }
        
        album_list.append(album_element)
    return album_list
    
def artist(Data):
    artist_list =[]
    for i in Data['items']:
        for key,value in i.items():
            if key == 'track':
                for j in value['artists']:
                    artist_dict = {'artist_id':j['id'], 'artist_name':j['name'], 'external_url': j['href']}
                    artist_list.append(artist_dict)
    return artist_list
    
def songes(Data):
    song_list = []
    for row in Data['items']:
        song_id = row['track']['id']
        song_name = row['track']['name']
        song_duration = row['track']['duration_ms']
        song_url = row['track']['external_urls']['spotify']
        song_popularity = row['track']['popularity']
        song_added = row['added_at']
        album_id = row['track']['album']['id']
        artist_id = row['track']['album']['artists'][0]['id']
        song_element = {'song_id':song_id,'song_name':song_name,'duration_ms':song_duration,'url':song_url,
                        'popularity':song_popularity,'song_added':song_added,'album_id':album_id,
                        'artist_id':artist_id
                       }
        song_list.append(song_element)
    return song_list
    

def lambda_handler(event, context):
    client = boto3.client('s3')
    bucket = "spotify-project-kunal"
    key = "row_data/to_processed/"
    
    fileName = []
    spotifyData = []
    
    # fisrt we take how much files are avalible in dir for prosesing
    for i in client.list_objects(Bucket  = bucket,Prefix = key)['Contents']:
        # we check avalible file are json form and take inside contents list we take Key value the name of file 
        
        if i['Key'].split(".")[-1] == "json":
    
            # by using s3 get_objects getting inside data avalible in file 
            responce = client.get_object(Bucket  = bucket,Key = i['Key'])
            #  taking body of the file
            content = responce["Body"]
            
            #  read data in json form becouse we save data in json from s3 (using json library for load data)
            jesonform = json.loads(content.read())
            
            #  append data in list (complete data of that file)
            spotifyData.append(jesonform)
            
            fileName.append(i['Key'])
    
    for Data in spotifyData:
        albumList = album(Data)
        # print(tt)
        artistList  = artist(Data)
        songesList = songes(Data)
        
        albumDf = pd.DataFrame(albumList).drop_duplicates(subset=['album_id'])
        artistDf = pd.DataFrame(artistList).drop_duplicates(subset=['artist_id'])
        songsDf = pd.DataFrame(songesList)
        
        
        # albumDf['album_release_date'] = pd.to_datetime(albumDf['album_release_date'])
        
        songsDf['song_added'] = pd.to_datetime(songsDf['song_added'])
        
        albumKey = "transformed_data/album_data/album_transformed_" + str(datetime.now()) + ".csv"
        albumBuffer=StringIO()
        albumDf.to_csv(albumBuffer, index=False)
        albumContent = albumBuffer.getvalue()
        client.put_object(Bucket=bucket, Key=albumKey, Body=albumContent)

        artistKey = "transformed_data/artist_data/artist_transformed_" + str(datetime.now()) + ".csv"
        artistBuffer=StringIO()
        artistDf.to_csv(artistBuffer, index=False)
        artistContent = albumBuffer.getvalue()
        client.put_object(Bucket=bucket, Key=artistKey, Body=artistContent)
        
        songsKey = "transformed_data/songs_data/songs_transformed_" + str(datetime.now()) + ".csv"
        #  using stringIO for covert data frame into string for save data into csv formate 
        songsBuffer=StringIO()
        songsDf.to_csv(songsBuffer, index=False)
        songsContent = songsBuffer.getvalue()
        client.put_object(Bucket=bucket, Key=songsKey, Body=songsContent)      
    
               
    s3Resource = boto3.resource('s3')
    for i in fileName:
        copySource = {"Bucket":bucket,"Key":i}
        s3Resource.meta.client.copy(copySource, bucket, 'row_data/processed/' + i.split("/")[-1]) 
        s3Resource.Object(bucket, i).delete()
        
        
            
            
     
    
