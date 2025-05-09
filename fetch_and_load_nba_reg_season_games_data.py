from nba_api.stats.endpoints import leaguegamefinder
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import numpy as np
import psycopg2
import os

nba_teams = [
    "ATL", "BKN", "BOS", "CHA", "CHI", "CLE", "DAL", "DEN", "DET",
    "GSW", "HOU", "IND", "LAC", "LAL", "MEM", "MIA", "MIL", "MIN",
    "NOP", "NYK", "OKC", "ORL", "PHI", "PHX", "POR", "SAC", "SAS",
    "TOR", "UTA", "WAS"
]


def fetch_nba_data():
    gamefinder = leaguegamefinder.LeagueGameFinder(season_nullable='2024-25')
    games = gamefinder.get_data_frames()[0]

    nba_games = games[games['TEAM_ABBREVIATION'].isin(nba_teams)]
    start_date = "2024-10-22"
    end_date = "2025-04-14"


    nba_regular_season_games = nba_games[(nba_games['GAME_DATE'] > start_date) & (nba_games['GAME_DATE'] < end_date)]

    return nba_regular_season_games




def load_to_postgres(nba_regular_season_games):

    pg_hook = PostgresHook(postgres_conn_id='postgres_nba_financial')
    conn = pg_hook.get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS nba_regular_season_games (
            SEASON_ID VARCHAR,
            TEAM_ID INT,
            TEAM_ABBREVIATION VARCHAR,
            TEAM_NAME VARCHAR,
            GAME_ID VARCHAR,
            GAME_DATE DATE,
            MATCHUP VARCHAR,
            WL VARCHAR,
            MIN INT,
            PTS INT,
            FGM INT,
            FGA INT,
            FG_PCT DOUBLE PRECISION,
            FG3M INT,
            FG3A INT,
            FG3_PCT DOUBLE PRECISION,
            FTM INT,
            FTA INT,
            FT_PCT DOUBLE PRECISION,
            OREB INT,
            DREB INT,
            REB INT,
            AST INT,
            STL INT,
            BLK INT,
            TOV INT,
            PF INT,
            PLUS_MINUS DOUBLE PRECISION,
            CONSTRAINT nba_regular_season_games_unique UNIQUE (GAME_ID, TEAM_ABBREVIATION)
        )
    """)


    nba_game_data = nba_regular_season_games.to_records(index=False).tolist()

    cur.executemany("""
        INSERT INTO nba_regular_season_games (
            SEASON_ID, TEAM_ID, TEAM_ABBREVIATION, TEAM_NAME, GAME_ID, GAME_DATE, 
            MATCHUP, WL, MIN, PTS, FGM, FGA, FG_PCT, FG3M, FG3A, FG3_PCT, FTM, FTA, 
            FT_PCT, OREB, DREB, REB, AST, STL, BLK, TOV, PF, PLUS_MINUS
        ) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (GAME_ID, TEAM_ABBREVIATION) DO UPDATE SET 
            SEASON_ID = EXCLUDED.SEASON_ID,
            TEAM_ID = EXCLUDED.TEAM_ID,
            TEAM_ABBREVIATION = EXCLUDED.TEAM_ABBREVIATION,
            TEAM_NAME = EXCLUDED.TEAM_NAME,
            GAME_DATE = EXCLUDED.GAME_DATE,
            MATCHUP = EXCLUDED.MATCHUP,
            WL = EXCLUDED.WL,
            MIN = EXCLUDED.MIN,
            PTS = EXCLUDED.PTS,
            FGM = EXCLUDED.FGM,
            FGA = EXCLUDED.FGA,
            FG_PCT = EXCLUDED.FG_PCT,
            FG3M = EXCLUDED.FG3M,
            FG3A = EXCLUDED.FG3A,
            FG3_PCT = EXCLUDED.FG3_PCT,
            FTM = EXCLUDED.FTM,
            FTA = EXCLUDED.FTA,
            FT_PCT = EXCLUDED.FT_PCT,
            OREB = EXCLUDED.OREB,
            DREB = EXCLUDED.DREB,
            REB = EXCLUDED.REB,
            AST = EXCLUDED.AST,
            STL = EXCLUDED.STL,
            BLK = EXCLUDED.BLK,
            TOV = EXCLUDED.TOV,
            PF = EXCLUDED.PF,
            PLUS_MINUS = EXCLUDED.PLUS_MINUS;
    """, nba_game_data)


    conn.commit()
    cur.close()
    conn.close()

print("Data loaded to PostgeSQL!")


if __name__ == "__main__":
    data = fetch_nba_data()
    load_to_postgres(data)