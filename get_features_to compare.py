import pandas as pd
import numpy as np
import psycopg2
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from airflow.providers.postgres.hooks.postgres import PostgresHook
import re
import unicodedata



def get_data_from_postgres():
    pg_hook = PostgresHook(postgres_conn_id='postgres_nba_financial')
    conn = pg_hook.get_conn()

    cur = conn.cursor()

    def clean_player_name(name):
        name = unicodedata.normalize('NFD', name)
        name = ''.join(c for c in name if unicodedata.category(c) != 'Mn')

        name = re.sub(r'\b(Jr\.?|Sr\.?|III|II|IV)\b', '', name)

        name = re.sub(r'\s+', ' ', name)

        return name.strip().lower()

    cur.execute("""
                SELECT player_name AS "Player", team AS "team_abbreviation" ,AVG(player_game_value) AS "value_avg"
                FROM player_values
                GROUP BY player_name,team""")
    
    rows = cur.fetchall()

    player_value = pd.DataFrame(rows, columns=[desc[0] for desc in cur.description])

    player_value['Team'] = player_value['Team'].replace('LA Clippers', 'Los Angeles Clippers')

    cur.execute("""
                SELECT * FROM nba_salaries""")
    rows = cur.fetchall()

    salaries = pd.DataFrame(rows, columns=[desc[0] for desc in cur.description])

    player_value['player_clean'] = player_value['Player'].apply(clean_player_name)
    salaries['player_clean'] = salaries['Player'].apply(clean_player_name)


    team_name_to_abbr = {
    'Atlanta Hawks': 'ATL',
    'Boston Celtics': 'BOS',
    'Brooklyn Nets': 'BKN',
    'Charlotte Hornets': 'CHA',
    'Chicago Bulls': 'CHI',
    'Cleveland Cavaliers': 'CLE',
    'Dallas Mavericks': 'DAL',
    'Denver Nuggets': 'DEN',
    'Detroit Pistons': 'DET',
    'Golden State Warriors': 'GSW',
    'Houston Rockets': 'HOU',
    'Indiana Pacers': 'IND',
    'Los Angeles Clippers': 'LAC',
    'Los Angeles Lakers': 'LAL',
    'Memphis Grizzlies': 'MEM',
    'Miami Heat': 'MIA',
    'Milwaukee Bucks': 'MIL',
    'Minnesota Timberwolves': 'MIN',
    'New Orleans Pelicans': 'NOP',
    'New York Knicks': 'NYK',
    'Oklahoma City Thunder': 'OKC',
    'Orlando Magic': 'ORL',
    'Philadelphia 76ers': 'PHI',
    'Phoenix Suns': 'PHX',
    'Portland Trail Blazers': 'POR',
    'Sacramento Kings': 'SAC',
    'San Antonio Spurs': 'SAS',
    'Toronto Raptors': 'TOR',
    'Utah Jazz': 'UTA',
    'Washington Wizards': 'WAS'
}
    
    salaries['team_abbreviation'] = salaries['Team'].map(team_name_to_abbr)

    cur.execute("""
                WITH avg_salary_by_pos AS (
                    SELECT "Pos", AVG("Cap Hit") AS avg_salary
                    FROM nba_salaries
                    GROUP BY "Pos"
                )           
                SELECT "Pos", avg_salary
                FROM avg_salary_by_pos
                ORDER BY avg_salary DESC;
                """)
    rows = cur.fetchall()

    avg_salary_by_pos = pd.DataFrame(rows, columns=[desc[0] for desc in cur.description])

    cur.close()
    conn.close()

    combined_df = pd.merge(
                    player_value, 
                    salaries,
                    on=['player_clean'],
                    how='left',
                    suffixes=('', '_salaries')
    )
   
    final_df = pd.merge(
                    combined_df, 
                    avg_salary_by_pos,
                    on=['Pos'],
                    suffixes=('', '_avg_salary')
                    )

    final_df = final_df.dropna(subset=['team_abbreviation_salaries'])
    final_df = final_df[final_df["team_abbreviation"] == final_df["team_abbreviation_salaries"]]
  
    final_df = final_df.rename(columns={"avg_salary": "pos_salary_avg", "Cap Hit": "salary"})
    final_df['pos_salary_avg'] = final_df['pos_salary_avg'].astype(float)
    final_df['pos_salary_avg'] = final_df['pos_salary_avg'].round(2)

    final_df['pos_value_avg'] = final_df.groupby('Pos')['value_avg'].transform('mean')
    final_df['pos_value_avg'] = final_df['pos_value_avg'].round(2)

    final_df['value_per_dollar'] = final_df['value_avg'] / (final_df['salary'] / 1_000_000)
    final_df['value_per_dollar'] = final_df['value_per_dollar'].round(2)

    final_df['value_per_dollar_pos_avg'] = final_df['pos_value_avg'] / (final_df['pos_salary_avg'] / 1_000_000)
    final_df['value_per_dollar_pos_avg'] = final_df['value_per_dollar_pos_avg'].round(2)

    final_df['team_value_avg'] = final_df.groupby('team_abbreviation')['value_avg'].transform('sum')
    final_df['team_value_avg'] = final_df['team_value_avg'].round(2)

    final_df['value_pct_in_team'] = (final_df['value_avg'] / final_df['team_value_avg']) * 100
    final_df['value_pct_in_team'] = final_df['value_pct_in_team'].round(2)

    csv_filename = f"player_features_to_compare.csv"
    final_df.to_csv(csv_filename, index=False)

 
    return final_df 



def upload_to_postgresql(file_path, table_name):
    df = pd.read_csv(file_path)

    pg_hook = PostgresHook(postgres_conn_id='postgres_nba_financial')
    
    engine = create_engine(pg_hook.get_uri())
    
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f"Successfully uploaded data to PostgreSQL table: {table_name}")

    engine.dispose()

if __name__ == "__main__":
    data = get_data_from_postgres
    table_name = "player_features"
    upload_to_postgresql("player_features_to_compare.csv", table_name)