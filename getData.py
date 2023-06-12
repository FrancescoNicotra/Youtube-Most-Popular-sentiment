import argparse
import os
import json
import requests
import googleapiclient.discovery
from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Load environment variables
load_dotenv('.env')
API_KEY = os.getenv('API_KEY')
API_SERVICE_NAME = 'youtube'
API_VERSION = 'v3'
CSV_FILENAME = 'trending_videos.csv'
COUNTRIES = ['IT']  # List of countries to analyze

# authenticate to the API


def get_authenticated_service():
    return build(API_SERVICE_NAME, API_VERSION, developerKey=API_KEY, cache_discovery=False)

# send data to logstash


def send_to_logstash(data):
    logstash_url = 'http://localhost:9090'

    try:
        response = requests.post(logstash_url, json=data)
        if response.status_code == 200:
            print("Dati inviati a Logstash con successo.")
        else:
            print("Errore durante l'invio dei dati a Logstash. Codice di risposta:",
                  response.status_code)
    except requests.exceptions.RequestException as e:
        print("Errore durante l'invio dei dati a Logstash:", str(e))

# list trending videos and retrieve comments


def list_trending_videos(youtube):
    videos = []

    youtubeAPI = googleapiclient.discovery.build(
        API_SERVICE_NAME, API_VERSION, developerKey=API_KEY)

    for country in COUNTRIES:
        search_response = youtube.videos().list(
            part='snippet,statistics',
            chart='mostPopular',
            maxResults=10,
            regionCode=country
        ).execute()

        for item in search_response.get('items', []):
            video_id = item['id']
            video_title = item['snippet']['title']
            video_item = {
                'videoId': video_id,
                'videoTitle': video_title,
                'comments': []
            }

            try:
                request = youtubeAPI.commentThreads().list(
                    part="snippet",
                    videoId=video_id,
                    maxResults=100
                )
                response = request.execute()

                for comment_thread in response.get('items', []):
                    comment = comment_thread['snippet']['topLevelComment']['snippet']
                    comment_item = {
                        'videoId': video_id,
                        'videoTitle': video_title,
                        'created_at': comment['publishedAt'],
                        'comment': comment['textDisplay']
                    }
                    videos.append(comment_item)
            except HttpError as e:
                error_message = json.loads(e.content)['error']['message']
                if 'disabled comments' in error_message:
                    print(
                        f"I commenti sono disabilitati per il video con ID '{video_id}'.")
                else:
                    print(
                        f"Errore durante il recupero dei commenti per il video con ID '{video_id}':", error_message)

    for comment in videos:
        send_to_logstash(comment)


# main
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    args = parser.parse_args()

    youtube = get_authenticated_service()
    try:
        list_trending_videos(youtube)
    except HttpError as e:
        print('Si è verificato un errore HTTP %d:\n%s' %
              (e.resp.status, e.content))
