import os
import pickle
import pandas as pd
import time
from nba_api.stats.endpoints import leaguegamefinder, boxscoreadvancedv2
from airflow.providers.postgres.hooks.postgres import PostgresHook
import psycopg2

nba_teams = [
    "ATL", "BKN", "BOS", "CHA", "CHI", "CLE", "DAL", "DEN", "DET",
    "GSW", "HOU", "IND", "LAC", "LAL", "MEM", "MIA", "MIL", "MIN",
    "NOP", "NYK", "OKC", "ORL", "PHI", "PHX", "POR", "SAC", "SAS",
    "TOR", "UTA", "WAS"
]

def fetch_players_metrics():
    cache_file = os.path.join(os.path.dirname(__file__), "player_cache.pkl")
    if os.path.exists(cache_file):
        with open(cache_file, "rb") as f:
            cached_data = pickle.load(f)
    else:
        cached_data = {}

    gamefinder = leaguegamefinder.LeagueGameFinder(season_nullable='2024-25')
    games = gamefinder.get_data_frames()[0]

    nba_games = games[games['TEAM_ABBREVIATION'].isin(nba_teams)]
    start_date = "2024-10-22"
    end_date = "2025-04-14"
    nba_regular_season_games = nba_games[(nba_games['GAME_DATE'] > start_date)  & (nba_games['GAME_DATE'] < end_date)]
    unique_game_ids = nba_regular_season_games['GAME_ID'].unique().tolist()

    all_advanced_player_stats = pd.DataFrame()

    start_time = time.time()

    for i, game_id in enumerate(unique_game_ids, start=1):
        if game_id in cached_data:
            player_metrics = cached_data[game_id]
        else:
            time.sleep(0.6)
            boxscore_adv = boxscoreadvancedv2.BoxScoreAdvancedV2(game_id=game_id)
            player_metrics = boxscore_adv.get_data_frames()[0]
            cached_data[game_id] = player_metrics 

        all_advanced_player_stats = pd.concat([all_advanced_player_stats, player_metrics])
        elapsed_time = time.time() - start_time
        remaining_time = (elapsed_time / i) * (len(unique_game_ids) - i)
        print(f"Processed {i}/{len(unique_game_ids)} games. Estimated time left: {remaining_time:.2f} seconds")


    with open(cache_file, "wb") as f:
        pickle.dump(cached_data, f)

    return all_advanced_player_stats

def advanced_player_metrics_to_postgres(all_advanced_player_stats):

    pg_hook = PostgresHook(postgres_conn_id='postgres_nba_financial')
    conn = pg_hook.get_conn()
    cur = conn.cursor()

    cur.execute("""
            CREATE TABLE IF NOT EXISTS nba_advanced_player_stats (
                GAME_ID VARCHAR(20),
                TEAM_ID INT,
                TEAM_ABBREVIATION VARCHAR(10),
                TEAM_CITY VARCHAR(50),
                PLAYER_ID INT,
                PLAYER_NAME VARCHAR(100),
                NICKNAME VARCHAR(50),
                START_POSITION VARCHAR(10),
                COMMENT VARCHAR(255),
                MIN DOUBLE PRECISION,
                E_OFF_RATING DOUBLE PRECISION,
                OFF_RATING DOUBLE PRECISION,
                E_DEF_RATING DOUBLE PRECISION,
                DEF_RATING DOUBLE PRECISION,
                E_NET_RATING DOUBLE PRECISION,
                NET_RATING DOUBLE PRECISION,
                AST_PCT DOUBLE PRECISION,
                AST_TOV DOUBLE PRECISION,
                AST_RATIO DOUBLE PRECISION,
                OREB_PCT DOUBLE PRECISION,
                DREB_PCT DOUBLE PRECISION,
                REB_PCT DOUBLE PRECISION,
                TM_TOV_PCT DOUBLE PRECISION,
                EFG_PCT DOUBLE PRECISION,
                TS_PCT DOUBLE PRECISION,
                USG_PCT DOUBLE PRECISION,
                E_USG_PCT DOUBLE PRECISION,
                E_PACE DOUBLE PRECISION,
                PACE DOUBLE PRECISION,
                PACE_PER40 DOUBLE PRECISION,
                POSS DOUBLE PRECISION,
                PIE DOUBLE PRECISION,
                CONSTRAINT nba_advanced_player_stats_unique UNIQUE (GAME_ID, PLAYER_ID)   
                )  
            """)
    all_advanced_player_stats['MIN'] = all_advanced_player_stats['MIN'].str.split(':').str[0].astype(float)
    
    advanced_player_metrics = all_advanced_player_stats.to_records(index=False).tolist()

    cur.executemany("""
            INSERT INTO nba_advanced_player_stats (
                GAME_ID, TEAM_ID, TEAM_ABBREVIATION, TEAM_CITY,
                PLAYER_ID, PLAYER_NAME, NICKNAME, START_POSITION,
                COMMENT, MIN, E_OFF_RATING, OFF_RATING,
                E_DEF_RATING, DEF_RATING, E_NET_RATING,
                NET_RATING, AST_PCT, AST_TOV,
                AST_RATIO, OREB_PCT, DREB_PCT,
                REB_PCT, TM_TOV_PCT, EFG_PCT,
                TS_PCT, USG_PCT, E_USG_PCT,
                E_PACE, PACE, PACE_PER40,
                POSS, PIE
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                      %s, %s)
                    
            ON CONFLICT (GAME_ID, PLAYER_ID) DO UPDATE
            SET
                TEAM_ID = EXCLUDED.TEAM_ID,
                TEAM_ABBREVIATION = EXCLUDED.TEAM_ABBREVIATION,
                TEAM_CITY = EXCLUDED.TEAM_CITY,
                PLAYER_NAME = EXCLUDED.PLAYER_NAME,
                NICKNAME = EXCLUDED.NICKNAME,
                START_POSITION = EXCLUDED.START_POSITION,
                COMMENT = EXCLUDED.COMMENT,
                MIN = EXCLUDED.MIN,
                E_OFF_RATING = EXCLUDED.E_OFF_RATING,
                OFF_RATING = EXCLUDED.OFF_RATING,
                E_DEF_RATING = EXCLUDED.E_DEF_RATING,
                DEF_RATING = EXCLUDED.DEF_RATING,
                E_NET_RATING = EXCLUDED.E_NET_RATING,
                NET_RATING = EXCLUDED.NET_RATING,
                AST_PCT = EXCLUDED.AST_PCT,
                AST_TOV = EXCLUDED.AST_TOV,
                AST_RATIO = EXCLUDED.AST_RATIO,
                OREB_PCT = EXCLUDED.OREB_PCT,
                DREB_PCT = EXCLUDED.DREB_PCT,
                REB_PCT = EXCLUDED.REB_PCT,
                TM_TOV_PCT = EXCLUDED.TM_TOV_PCT,
                EFG_PCT = EXCLUDED.EFG_PCT,
                TS_PCT = EXCLUDED.TS_PCT,
                USG_PCT = EXCLUDED.USG_PCT,
                E_USG_PCT = EXCLUDED.E_USG_PCT,
                E_PACE = EXCLUDED.E_PACE,
                PACE = EXCLUDED.PACE,
                PACE_PER40 = EXCLUDED.PACE_PER40,
                POSS = EXCLUDED.POSS,
                PIE = EXCLUDED.PIE
            """, advanced_player_metrics)
    conn.commit()
    conn.close()
    cur.close()


if __name__ == "__main__":
    advanced_player_stats = fetch_players_metrics()
    advanced_player_metrics_to_postgres(advanced_player_stats)
    print("Data loaded to PostgreSQL!")
    
