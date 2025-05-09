from nba_api.stats.endpoints import playergamelogs
import pandas as pd
import numpy as np
import psycopg2
from airflow.providers.postgres.hooks.postgres import PostgresHook
import os


nba_teams = [
    "ATL", "BKN", "BOS", "CHA", "CHI", "CLE", "DAL", "DEN", "DET",
    "GSW", "HOU", "IND", "LAC", "LAL", "MEM", "MIA", "MIL", "MIN",
    "NOP", "NYK", "OKC", "ORL", "PHI", "PHX", "POR", "SAC", "SAS",
    "TOR", "UTA", "WAS"
]


season = "2024-25"  

def fetch_player_data():
    player_stats = playergamelogs.PlayerGameLogs(season_nullable=season)
    games_for_players = player_stats.get_data_frames()[0]

    nba_games_for_players = games_for_players[games_for_players['TEAM_ABBREVIATION'].isin(nba_teams)]

    start_date = "2024-10-22"
    end_date = "2025-04-14"

    players_regular_season_games = nba_games_for_players[(nba_games_for_players['GAME_DATE'] > start_date) & (nba_games_for_players['GAME_DATE'] < end_date)]


    filtered_player_stats = players_regular_season_games[[
                'SEASON_YEAR', 'PLAYER_ID', 'PLAYER_NAME', 'NICKNAME', 'TEAM_ID',
                'TEAM_ABBREVIATION', 'TEAM_NAME', 'GAME_ID', 'GAME_DATE', 'MATCHUP',
                'WL', 'MIN', 'FGM', 'FGA', 'FG_PCT', 'FG3M', 'FG3A', 'FG3_PCT', 'FTM',
                'FTA', 'FT_PCT', 'OREB', 'DREB', 'REB', 'AST', 'TOV', 'STL', 'BLK',
                'BLKA', 'PF', 'PFD', 'PTS', 'PLUS_MINUS', 'NBA_FANTASY_PTS', 'DD2',
                'TD3', 'MIN_SEC']]
    
    return filtered_player_stats

def player_load_to_postgres(filtered_player_stats):
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_nba_financial')
    conn = pg_hook.get_conn()

    cur = conn.cursor()

    cur.execute("""
            CREATE TABLE IF NOT EXISTS nba_regular_season_player_stats (
                SEASON_YEAR VARCHAR(9),
                PLAYER_ID INT,
                PLAYER_NAME VARCHAR(100),
                NICKNAME VARCHAR(100),
                TEAM_ID INT,
                TEAM_ABBREVIATION VARCHAR(10),
                TEAM_NAME VARCHAR(100),
                GAME_ID VARCHAR(20),
                GAME_DATE DATE,
                MATCHUP VARCHAR(50),
                WL VARCHAR(1),
                MIN DOUBLE PRECISION,
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
                TOV INT,
                STL INT,
                BLK INT,
                BLKA INT,
                PF INT,
                PFD INT,
                PTS INT,
                PLUS_MINUS DOUBLE PRECISION,
                NBA_FANTASY_PTS DOUBLE PRECISION,
                DD2 INT,
                TD3 INT,
                MIN_SEC VARCHAR(10),
                CONSTRAINT nba_regular_season_player_stats_unique UNIQUE (PLAYER_ID, GAME_ID))
        """)
    
    nba_player_data = filtered_player_stats.to_records(index=False).tolist()

    cur.executemany("""
        INSERT INTO nba_regular_season_player_stats (
            SEASON_YEAR, PLAYER_ID, PLAYER_NAME, NICKNAME, TEAM_ID, TEAM_ABBREVIATION, TEAM_NAME, 
            GAME_ID, GAME_DATE, MATCHUP, WL, MIN, FGM, FGA, FG_PCT, FG3M, FG3A, FG3_PCT, 
            FTM, FTA, FT_PCT, OREB, DREB, REB, AST, TOV, STL, BLK, BLKA, PF, PFD, PTS, 
            PLUS_MINUS, NBA_FANTASY_PTS, DD2, TD3, MIN_SEC
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (PLAYER_ID, GAME_ID) DO UPDATE SET
            WL = EXCLUDED.WL,
            MIN = EXCLUDED.MIN,
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
            TOV = EXCLUDED.TOV,
            STL = EXCLUDED.STL,
            BLK = EXCLUDED.BLK,
            BLKA = EXCLUDED.BLKA,
            PF = EXCLUDED.PF,
            PFD = EXCLUDED.PFD,
            PTS = EXCLUDED.PTS,
            PLUS_MINUS = EXCLUDED.PLUS_MINUS,
            NBA_FANTASY_PTS = EXCLUDED.NBA_FANTASY_PTS,
            DD2 = EXCLUDED.DD2,
            TD3 = EXCLUDED.TD3,
            MIN_SEC = EXCLUDED.MIN_SEC;
    """, nba_player_data)

    conn.commit()
    cur.close()
    conn.close()

print('Players data loaded to PostgreSQL!')
if __name__ == "__main__":
    data = fetch_player_data()
    player_load_to_postgres(data)
    

    

