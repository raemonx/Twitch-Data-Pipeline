import requests
import pandas as pd
from sqlalchemy import create_engine


def get_access_token(client_id, client_secret):
    url = 'https://id.twitch.tv/oauth2/token'
    payload = {
        'client_id': client_id,
        'client_secret': client_secret,
        'grant_type': 'client_credentials'
    }
    response = requests.post(url, data=payload)
    response.raise_for_status()
    return response.json().get('access_token')


def get_games_data(client_id, access_token):
    url = 'https://api.twitch.tv/helix/games/top'
    headers = {
        'Client-ID': client_id,
        'Authorization': f'Bearer {access_token}'
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json().get('data', [])

def get_streams_data(client_id, access_token, game_id):
    url = f'https://api.twitch.tv/helix/streams?game_id={game_id}'
    headers = {
        'Client-ID': client_id,
        'Authorization': f'Bearer {access_token}'
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json().get('data', [])

def run_twitch_etl():
    client_id = '**********************************'
    client_secret = '**********************************'

    access_token = get_access_token(client_id, client_secret)
    print(access_token)
    games_data = get_games_data(client_id, access_token)
    games_df = pd.DataFrame(games_data)

    streams = []
    for game in games_data:
        game_id = game['id']
        streams_data = get_streams_data(client_id, access_token, game_id)
        streams.extend(streams_data)

    streams_df = pd.DataFrame(streams)

    games_df.to_csv('s3://s3-twitch-data-analytics/games_data.csv', index=False)
    streams_df.to_csv('s3://s3-twitch-data-analytics/streams_data.csv', index=False)

    print("ETL completed")
    return games_df, streams_df


if __name__ == "__main__":
    games_df = run_twitch_etl()

