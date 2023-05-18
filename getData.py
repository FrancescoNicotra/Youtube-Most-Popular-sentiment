import argparse
import os
import re
import json
import requests
import googleapiclient.discovery
from dotenv import load_dotenv
import google.oauth2.credentials
import google_auth_oauthlib.flow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow
import uuid


load_dotenv('.env')
CLIENT_SECRETS_FILE = os.getenv('CLIENT')

# This OAuth 2.0 access scope allows for read-only access to the authenticated
# user's account, but not other types of account access.
SCOPES = ['https://www.googleapis.com/auth/youtube.readonly']
API_SERVICE_NAME = 'youtube'
API_VERSION = 'v3'
DEVELOPER_KEY = os.getenv('API_KEY')

VALID_BROADCAST_STATUSES = ('all', 'active', 'completed', 'upcoming',)

# Authorize the request and store authorization credentials.


def get_authenticated_service():
    flow = InstalledAppFlow.from_client_secrets_file(
        CLIENT_SECRETS_FILE, SCOPES)
    credentials = flow.run_local_server()
    return build(API_SERVICE_NAME, API_VERSION, credentials=credentials, cache_discovery=False)


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


def list_trending_videos(youtube):
    videos = []

    youtubeAPI = googleapiclient.discovery.build(
        API_SERVICE_NAME, API_VERSION, developerKey=DEVELOPER_KEY)
    search_response = youtube.videos().list(
        part='snippet,statistics',
        chart='mostPopular',
        maxResults=15
    ).execute()

    for item in search_response.get('items', []):
        video = item['snippet']
        video_id = item['id']
        video_statistics = item.get('statistics', {})
        video_item = {
            'title': video['title'],
            'videoId': video_id,
            'views': int(video_statistics.get('viewCount', 0)),
            'doc_id': str(uuid.uuid4().int)  # Genera un UUID intero univoco
        }

        request = youtubeAPI.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=100
        )
        response = request.execute()

        # for comment_thread in response.get('items', []):
        #   comment = comment_thread['snippet']['topLevelComment']['snippet']
        #  video_item['comments'].append(comment['textDisplay'])

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
    args = parser.parse_args()

    youtube = get_authenticated_service()
    try:
        list_trending_videos(youtube)
    except HttpError as e:
        print('An HTTP error %d occurred:\n%s' % (e.resp.status, e.content))
