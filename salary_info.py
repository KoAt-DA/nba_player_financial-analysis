import requests
from bs4 import BeautifulSoup
import pandas as pd
import requests
import time
import random
import re
from sqlalchemy import create_engine
from airflow.providers.postgres.hooks.postgres import PostgresHook


def clean_player_name(name):
    if '\n' in name:
        return name.split('\n')[-1].strip()
    
    cleaned = re.sub(r'^\d+\s+', '', name)
    return cleaned.strip()

def get_team_salary_data(team_id, team_name, year=2024):
    """Get salary data for a specific team"""
    url = f"https://www.spotrac.com/nba/{team_id}/overview/_/year/{year}"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    print(f"Fetching data from: {url}")
    

    response = requests.get(url, headers=headers)
    response.raise_for_status()
        
    soup = BeautifulSoup(response.text, 'html.parser')
        
    tables = soup.find_all('table')
        
    salary_table = None
    for i, table in enumerate(tables):
            headers = [th.text.strip() for th in table.find_all('th') if th.text.strip()]
            if headers and 'Player' in headers[0]:
                print(f"Table {i} has headers: {headers}")
                salary_table = table
                break
        
    headers = []
    for th in salary_table.find_all('th'):
            header_text = th.text.strip()
            header_text = header_text.replace('\n', ' ')
            
            if header_text.startswith('Player'):
                header_text = 'Player'
                
            headers.append(header_text)
        
    print(f"Selected table with headers: {headers}")
        
    rows = salary_table.find_all('tr')
        
    if rows and any(th.name == 'th' for th in rows[0].find_all(['th', 'td'])):
            rows = rows[1:]
        
    salary_data = []
        
    for row in rows:
            cols = row.find_all('td')
            
            if len(cols) < 3: 
                continue
            
            row_data = {'Team': team_name} 
            
            for i, col in enumerate(cols):
                if i < len(headers):
                    header = headers[i]
                    value = col.text.strip()
                    
                    if header == 'Player':
                        value = clean_player_name(value)
                    
                    if '$' in value:
                        value = re.sub(r'[$,]', '', value)
                    
                    if '%' in value:
                        value = value.replace('%', '')
                    
                    row_data[header] = value
            
            salary_data.append(row_data)
        
    df = pd.DataFrame(salary_data)
        
    for col in df.columns:
            if any(keyword in col.lower() for keyword in ['cap', 'salary', 'cash', 'hit', 'pct']):
                try:
                    df[col] = pd.to_numeric(df[col], errors='ignore')
                except:
                    pass  
        
    return df
    

def main():
    year = 2024
    
    teams = [
        {"name": "Atlanta Hawks", "id": "atlanta-hawks"},
        {"name": "Boston Celtics", "id": "boston-celtics"},
        {"name": "Brooklyn Nets", "id": "brooklyn-nets"},
        {"name": "Charlotte Hornets", "id": "charlotte-hornets"},
        {"name": "Chicago Bulls", "id": "chicago-bulls"},
        {"name": "Cleveland Cavaliers", "id": "cleveland-cavaliers"},
        {"name": "Dallas Mavericks", "id": "dallas-mavericks"},
        {"name": "Denver Nuggets", "id": "denver-nuggets"},
        {"name": "Detroit Pistons", "id": "detroit-pistons"},
        {"name": "Golden State Warriors", "id": "golden-state-warriors"},
        {"name": "Houston Rockets", "id": "houston-rockets"},
        {"name": "Indiana Pacers", "id": "indiana-pacers"},
        {"name": "Los Angeles Clippers", "id": "la-clippers"},
        {"name": "Los Angeles Lakers", "id": "los-angeles-lakers"},
        {"name": "Memphis Grizzlies", "id": "memphis-grizzlies"},
        {"name": "Miami Heat", "id": "miami-heat"},
        {"name": "Milwaukee Bucks", "id": "milwaukee-bucks"},
        {"name": "Minnesota Timberwolves", "id": "minnesota-timberwolves"},
        {"name": "New Orleans Pelicans", "id": "new-orleans-pelicans"},
        {"name": "New York Knicks", "id": "new-york-knicks"},
        {"name": "Oklahoma City Thunder", "id": "oklahoma-city-thunder"},
        {"name": "Orlando Magic", "id": "orlando-magic"},
        {"name": "Philadelphia 76ers", "id": "philadelphia-76ers"},
        {"name": "Phoenix Suns", "id": "phoenix-suns"},
        {"name": "Portland Trail Blazers", "id": "portland-trail-blazers"},
        {"name": "Sacramento Kings", "id": "sacramento-kings"},
        {"name": "San Antonio Spurs", "id": "san-antonio-spurs"},
        {"name": "Toronto Raptors", "id": "toronto-raptors"},
        {"name": "Utah Jazz", "id": "utah-jazz"},
        {"name": "Washington Wizards", "id": "washington-wizards"}
    ]
    
    print(f"Starting to scrape data for {len(teams)} NBA teams")
    
    all_teams_data = []
    successful_teams = 0
    
    for i, team in enumerate(teams):
        print(f"\nProcessing {i+1}/{len(teams)}: {team['name']}")
        
        team_data = get_team_salary_data(team['id'], team['name'], year)
        
        if team_data is not None and not team_data.empty:
            columns_to_keep = ['Team', 'Player', 'Pos', 'Age', 'Cap Hit', 'Cap Hit Pct League Cap', 
                              'Apron Salary', 'Luxury Tax', 'Cash Total', 'Cash Guaranteed', 'Free Agent Year']
            
            available_columns = [col for col in columns_to_keep if col in team_data.columns]
            filtered_team_data = team_data[available_columns]
            
            all_teams_data.append(filtered_team_data)
            successful_teams += 1
            print(f"Successfully scraped data for {team['name']}: {len(filtered_team_data)} players")
            
        else:
            print(f"No data found for {team['name']}")
        
        if i < len(teams) - 1:  
            delay = random.uniform(2, 4)  
            print(f"Waiting {delay:.2f} seconds before next request...")
            time.sleep(delay)
    
    print(f"\nCompleted scraping {successful_teams} out of {len(teams)} teams")
    
    if all_teams_data:
        combined_df = pd.concat(all_teams_data, ignore_index=True)
        
        csv_filename = f"nba_team_salaries_{year}.csv"
        combined_df.to_csv(csv_filename, index=False)
        print(f"\nCombined data saved to {csv_filename}")
        
        print(f"\nCombined data: {len(combined_df)} players across {successful_teams} teams")
        print("\nFirst few rows:")
        print(combined_df.head())


def upload_to_postgresql(file_path, table_name):
    df = pd.read_csv(file_path)

    pg_hook = PostgresHook(postgres_conn_id='postgres_nba_financial')
    
    engine = create_engine(pg_hook.get_uri())
    
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f"Successfully uploaded data to PostgreSQL table: {table_name}")

    engine.dispose()

if __name__ == "__main__":
    main()