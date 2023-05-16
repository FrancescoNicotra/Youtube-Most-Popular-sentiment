import argparse
import os
import re
import json
import requests
import googleapiclient.discovery
import google.oauth2.credentials
import google_auth_oauthlib.flow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow


# The CLIENT_SECRETS_FILE variable specifies the name of a file that contains
# the OAuth 2.0 information for this application, including its client_id and
# client_secret. You can acquire an OAuth 2.0 client ID and client secret from
# the {{ Google Cloud Console }} at
# {{ https://cloud.google.com/console }}.
# Please ensure that you have enabled the YouTube Data API for your project.
# For more information about using OAuth2 to access the YouTube Data API, see:
#   https://developers.google.com/youtube/v3/guides/authentication
# For more information about the client_secrets.json file format, see:
#   https://developers.google.com/api-client-library/python/guide/aaa_client_secrets
CLIENT_SECRETS_FILE = 'client_secret_662717540184-9j4cp6ln4glhmj2aou93c9s5th0iju4a.apps.googleusercontent.com.json'

# This OAuth 2.0 access scope allows for read-only access to the authenticated
# user's account, but not other types of account access.
SCOPES = ['https://www.googleapis.com/auth/youtube.readonly']
API_SERVICE_NAME = 'youtube'
API_VERSION = 'v3'
DEVELOPER_KEY = "AIzaSyAXeARoQ62y5gSM6gFK4geGNQN2eGP1P7c"

VALID_BROADCAST_STATUSES = ('all', 'active', 'completed', 'upcoming',)

# Authorize the request and store authorization credentials.


def get_authenticated_service():
    flow = InstalledAppFlow.from_client_secrets_file(
        CLIENT_SECRETS_FILE, SCOPES)
    credentials = flow.run_local_server()
    return build(API_SERVICE_NAME, API_VERSION, credentials=credentials, cache_discovery=False)


def get_channel_id(youtube, channel_username):
    search_response = youtube.search().list(
        part='id',
        q=channel_username,
        type='channel'
    ).execute()

    channel_id = search_response['items'][0]['id']['channelId']
    return channel_id


# Retrieve a list of broadcasts with the specified status.

def send_to_logstash(data):
    logstash_url = 'http://localhost:9090'  # URL di Logstash

    try:
        response = requests.post(logstash_url, data=data)
        if response.status_code == 200:
            print("Dati inviati a Logstash con successo.")
        else:
            print("Errore durante l'invio dei dati a Logstash. Codice di risposta:",
                  response.status_code)
    except requests.exceptions.RequestException as e:
        print("Errore durante l'invio dei dati a Logstash:", str(e))


def list_latest_videos(youtube, channel_id):
    videos = []

    youtubeAPI = googleapiclient.discovery.build(
        API_SERVICE_NAME, API_VERSION, developerKey=DEVELOPER_KEY)
    search_response = youtube.search().list(
        part='id,snippet',
        channelId=channel_id,
        order='date',
        type='video',
        maxResults=10
    ).execute()

    for item in search_response.get('items', []):
        video = item['snippet']
        video_id = item['id']['videoId']
        video_item = {
            'title': video['title'],
            'videoId': video_id,
            'comments': []
        }

        request = youtubeAPI.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=10
        )
        response = request.execute()

        for comment_thread in response.get('items', []):
            comment = comment_thread['snippet']['topLevelComment']['snippet']
            video_item['comments'].append(comment['textDisplay'])

        videos.append(video_item)

    formatted_json = json.dumps(videos, indent=4)
    print(formatted_json)

    # Invia i dati a Logstash tramite una richiesta POST
    logstash_url = 'http://localhost:9090'
    headers = {'Content-Type': 'application/json'}
    response = requests.post(
        logstash_url, data=formatted_json, headers=headers)

    if response.status_code == 200:
        print('Dati inviati con successo a Logstash.')
    else:
        print('Errore durante l\'invio dei dati a Logstash.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--channel-username',
                        help='Channel username', required=True)
    args = parser.parse_args()

    youtube = get_authenticated_service()
    try:
        channel_id = get_channel_id(youtube, args.channel_username)
        list_latest_videos(youtube, channel_id)
    except HttpError as e:
        print('An HTTP error %d occurred:\n%s' % (e.resp.status, e.content))
