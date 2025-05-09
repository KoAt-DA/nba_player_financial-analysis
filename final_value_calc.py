import pandas as pd
import numpy as np
import psycopg2
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from airflow.providers.postgres.hooks.postgres import PostgresHook



def get_data_from_postgres():

    pg_hook = PostgresHook(postgres_conn_id='postgres_nba_financial')
    conn = pg_hook.get_conn()

    cur = conn.cursor()

    cur.execute("""
                    SELECT season_year, player_id, player_name, team_id, team_abbreviation, team_name,
                        game_id, game_date, matchup, wl, min, fgm, fga, fg_pct, fg3m, fg3a, fg3_pct,
                        ftm, fta, ft_pct, oreb, dreb, reb, ast, tov, stl, blk, pf, pts, plus_minus
                    FROM nba_regular_season_player_stats""")
            
    rows = cur.fetchall()
    
    player_boxscore = pd.DataFrame(rows, columns=[desc[0] for desc in cur.description])

    

    cur.execute("""
                    SELECT  game_id, player_id, player_name, team_id, team_abbreviation, min,
                        off_rating, def_rating, net_rating, ast_pct, ast_ratio,
                        oreb_pct, dreb_pct, reb_pct, efg_pct, ts_pct, usg_pct, pie
                    FROM nba_advanced_player_stats""")
                
    rows = cur.fetchall()

    advanced_player_stats = pd.DataFrame(rows, columns=[desc[0] for desc in cur.description])

    

    cur.close()
    conn.close()

    combined_df = pd.merge(
                    player_boxscore, 
                    advanced_player_stats,
                    on=['game_id', 'player_id', 'player_name', 'team_id', 'team_abbreviation'],
                    suffixes=('', '_adv')
                    )
    

    return combined_df


data = get_data_from_postgres()

def calculate_player_stats_value(combined_df):
    player_game_ratings = {}
    player_data_list = []


    box_score_off_weights = {
            'pts': 0.033,         
            'ast': 0.07,           
            'oreb': 0.05,                    
            'tov': -0.07,                   
            'fg_pct': 0.5,         
            'fg3_pct': 0.4,        
            'ft_pct': 0.2,        
            'min': 0.0025,         
            'plus_minus': 0.04
        }

    box_score_def_weights = {
            'dreb': 0.1,
            'stl': 0.15,
            'blk': 0.10,
            'pf': -0.02,
            'min': 0.0025
        }
        
    advanced_weights = {
            'off_rating': 0.15,    
            'def_rating': -0.15,   
            'net_rating': 0.0,     
            'ast_pct': 0.12,      
            'reb_pct': 0.10,       
            'ts_pct': 0.25,         
            'usg_pct': 0.08,       
            'pie': 0.35,
            'min': 0.0025
        }
    
    data['home_game'] = data['matchup'].apply(lambda x: "Y" if 'vs.' in x else "N")
    data['wl_numeric'] = data['wl'].apply(lambda x: 1 if x == 'W' else 0)
    
    for index, game in data.iterrows():
            player_id = game['player_id']
            player_name = game['player_name']
            team_abbreviation = game['team_abbreviation']
            game_id = game['game_id']
            game_date = game['game_date']
            home_game = game['home_game']
            wl = game['wl_numeric']


            minutes = float(game['min'])

            
            def extract_opponent(game):
                if '@' in game['matchup']:
                    return game['matchup'].split(' @ ')[1]
                elif 'vs' in game['matchup']:
                    return game['matchup'].split(' vs. ')[1]
                return None


            off_box_score_rating = 0
            for stat, weight in box_score_off_weights.items():
                if stat in game and not pd.isna(game[stat]):
                    off_box_score_rating += game[stat] * weight

            def_box_score_rating = 0
            for stat, weight in box_score_def_weights.items():
                if stat in game and not pd.isna(game[stat]):
                    def_box_score_rating += game[stat] * weight
            
            advanced_rating = 0
            for stat, weight in advanced_weights.items():
                if stat in game and not pd.isna(game[stat]):
                    advanced_rating += game[stat] * weight

            wl_bonus = 2 if game['wl'] == 'W' else 0

                       
            off_game_rating = off_box_score_rating + advanced_rating + wl_bonus
            def_game_rating = def_box_score_rating + advanced_rating + wl_bonus
            game_rating = off_game_rating + def_game_rating + advanced_rating + wl_bonus

            minutes_factor = min(1.0, minutes / 36)
            game_rating *= minutes_factor

            if player_id not in player_game_ratings:
                player_game_ratings[player_id] = {}

            player_game_ratings[player_id][game_id] = (off_game_rating, def_game_rating, game_rating)

            player_data_list.append({
                'player_id': player_id,
                'player_name': player_name,
                'team': team_abbreviation,
                'game_id': game_id,
                'game_date': game_date,
                'is_home_game': home_game,
                'wl_numeric': wl,
                'minutes': minutes,
                'rating': game_rating,
            })
    game_ratings_df = pd.DataFrame(player_data_list)

    game_ratings_df['opponent_team_abbreviation'] = data.apply(extract_opponent, axis=1)

    df_base = game_ratings_df.copy()

    opponents_df = df_base[['player_id', 'team', 'game_id', 'rating']].copy()


    df_merged = df_base.merge(
    opponents_df,
    left_on=['opponent_team_abbreviation', 'game_id'],  
    right_on=['team', 'game_id'],         
    how='left',
    suffixes=('', '_opponent')
)

    opponent_strength = (
        df_merged
        .groupby(['player_id', 'game_id'])['rating_opponent']
        .mean()
        .reset_index()
        .rename(columns={'rating_opponent': 'opponent_strength'})
)
    
    df_with_opp_str = df_base.merge(opponent_strength, on=['player_id', 'game_id'], how='left')

    opponent_strength_weight = 0.15

    df_with_opp_str['player_game_value'] = df_with_opp_str['rating'] + (df_with_opp_str['opponent_strength'] * opponent_strength_weight)

    csv_filename = f"player_values.csv"
    df_with_opp_str.to_csv(csv_filename, index=False)

    return df_with_opp_str



def upload_to_postgresql(file_path, table_name, conn_id='postgres_nba_financial'):
    df = pd.read_csv(file_path)

        
    pg_hook = PostgresHook(postgres_conn_id=conn_id)
        
    conn = pg_hook.get_conn()
    engine = create_engine(
            f"postgresql+psycopg2://{pg_hook.get_uri().split('://')[1]}"
        )
    
    df.to_sql(table_name, engine, if_exists='replace', index=False)

    engine.dispose()


if __name__ == "__main__":
    players_data = get_data_from_postgres()
    calculate_player_stats_value(players_data)
    upload_to_postgresql("player_values.csv", "player_values", conn_id='postgres_nba_financial')
    print("Data loaded to PostgreSQL!")
