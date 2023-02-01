import boto3, psycopg2, requests
import pandas as pd
from airflow import DAG
import airflow.utils.dates
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
import datetime

dag1 = DAG(
    dag_id="api_loader",
    start_date=datetime.datetime(2023, 2, 1, 18, 00),
    schedule_interval="0 */12 * * *")

def _fetch_and_load_canabis():
    # get data
    res = requests.get('https://random-data-api.com/api/cannabis/random_cannabis?size=100')
    df = pd.json_normalize(res.json(), max_level=1)
    df['tech_load_ts'] = datetime.datetime.now()
    write_df_to_postgres(df, 'cannabis', 'stg_cannabis')

def _fetch_and_load_games():
    # get data
    res = requests.get('https://statsapi.web.nhl.com/api/v1/teams/21/stats')
    # normalizing data
    games = []
    splits = []
    teams = []
    for i in res.json()['stats']:
        # here we add hash instead of id because games don't have it in src.
        # we will need it later when we will need to get splits for game
        game_hash = hash(str(i.items()))
        games.append({
            'type': i['type'],
            'game_id': game_hash
        })
        for j in i['splits']:
            splits.append({
                'stat': j['stat'],
                'team_id': j['team']['id'],
                'game_id': game_hash
            })
            teams.append({
                'id': j['team']['id'],
                'name': j['team']['name'],
                'link': j['team']['link']
            })

    df_games = pd.json_normalize(games, sep='_')
    df_games['tech_load_ts'] = datetime.datetime.now()
    df_splits = pd.json_normalize(splits, sep='_')
    df_splits['tech_load_ts'] = datetime.datetime.now()
    df_teams = pd.json_normalize(teams, sep='_').drop_duplicates()
    df_teams['tech_load_ts'] = datetime.datetime.now()

    write_df_to_postgres(df_games, 'games', 'stg_games')
    write_df_to_postgres(df_splits, 'splits', 'stg_games')
    write_df_to_postgres(df_teams, 'teams', 'stg_games')

def write_df_to_postgres(df, table, schema):
    conn_string = 'postgresql://airflow:airflow@postgres:5432/airflow'
    engine = create_engine(conn_string)
    df.to_sql(table, engine, if_exists='append', schema=schema, index=False)

load_cannabis = PythonOperator(
    task_id="load_data_api1",
    python_callable=_fetch_and_load_canabis,
    dag=dag1)

load_games = PythonOperator(
    task_id="load_data_api2",
    python_callable=_fetch_and_load_games,
    dag=dag1)

load_cannabis >> load_games
