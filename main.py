import requests
import pandas as pd
from datetime import datetime
import time
import json
from pathlib import Path
from collections import deque

class SofaScorePenaltyScraper:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://www.sofascore.com/'
        }
        self.tournament_id = 52  # Super Lig
        self.season_id = 63814   # 2023-2024 season
        self.base_url = "https://api.sofascore.com/api/v1"
        
        self.data_dir = Path("data")
        self.data_dir.mkdir(exist_ok=True)

    def _make_request(self, url, max_retries=3):
        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=self.headers)
                response.raise_for_status()
                return response.json()
            except Exception as e:
                print(f"Error on attempt {attempt + 1} for {url}: {str(e)}")
                if attempt == max_retries - 1:
                    raise e
                time.sleep(2 ** attempt)

    def get_team_matches(self, team_id, team_name):
        """Get all matches for a specific team"""
        url = f"{self.base_url}/team/{team_id}/events/last/0"
        print(f"Fetching matches for {team_name}...")
        response = self._make_request(url)
        matches = response.get('events', [])
        
        # Filter only Super Lig matches
        super_lig_matches = [
            match for match in matches 
            if match.get('tournament', {}).get('uniqueTournament', {}).get('id') == self.tournament_id
        ]
        
        print(f"Found {len(super_lig_matches)} Super Lig matches for {team_name}")
        return super_lig_matches

    def get_match_incidents(self, event_id, match_info):
        """Get penalty incidents for a specific match"""
        url = f"{self.base_url}/event/{event_id}/incidents"
        response = self._make_request(url)
        incidents = response.get('incidents', [])
        
        penalties = []
        for incident in incidents:
            # Check for various types of penalty incidents
            is_penalty = False
            if incident.get('incidentType') == 'goal':
                is_penalty = incident.get('from') == 'penalty'
            elif incident.get('incidentType') == 'varDecision':
                is_penalty = incident.get('incidentClass') == 'penaltyNotAwarded'
            elif incident.get('incidentClass') == 'penalty':
                is_penalty = True
            
            if is_penalty:
                penalty_data = {
                    'match_id': event_id,
                    'match_date': datetime.fromtimestamp(match_info.get('startTimestamp', 0)).strftime('%Y-%m-%d'),
                    'home_team': match_info['homeTeam']['name'],
                    'away_team': match_info['awayTeam']['name'],
                    'round': match_info.get('roundInfo', {}).get('round'),
                    'match_minute': incident.get('time'),
                    'taker_name': incident.get('player', {}).get('name'),
                    'taker_team': match_info['homeTeam']['name'] if incident.get('isHome') else match_info['awayTeam']['name'],
                    'incident_type': incident.get('incidentType'),
                    'incident_class': incident.get('incidentClass'),
                    'from': incident.get('from'),
                    'result': 'scored' if incident.get('incidentType') == 'goal' else 'not awarded/missed',
                    'score_at_time': f"{incident.get('homeScore', 0)}-{incident.get('awayScore', 0)}"
                }
                penalties.append(penalty_data)
                print(f"Found penalty: {penalty_data['taker_name']} ({penalty_data['taker_team']}) - {penalty_data['result']}")
        
        return penalties

    def collect_all_super_lig_penalties(self):
        """Collect penalties from all Super Lig teams"""
        # Start with some known teams
        teams_to_process = deque([
            (3061, "Galatasaray"),
            (3052, "Fenerbahçe"),
            (3050, "Beşiktaş"),
            (3051, "Trabzonspor"),
            (3085, "Konyaspor")
        ])
        
        processed_teams = set()
        processed_matches = set()
        all_penalties = []
        
        while teams_to_process:
            team_id, team_name = teams_to_process.popleft()
            
            if team_id in processed_teams:
                continue
                
            matches = self.get_team_matches(team_id, team_name)
            processed_teams.add(team_id)
            
            for match in matches:
                match_id = match['id']
                
                if match_id in processed_matches:
                    continue
                
                if match.get('status', {}).get('type') == 'finished':
                    print(f"\nProcessing: {match['homeTeam']['name']} vs {match['awayTeam']['name']}")
                    penalties = self.get_match_incidents(match_id, match)
                    if penalties:
                        all_penalties.extend(penalties)
                    
                    # Add new teams we discover
                    for team in [match['homeTeam'], match['awayTeam']]:
                        if team['id'] not in processed_teams:
                            teams_to_process.append((team['id'], team['name']))
                    
                    processed_matches.add(match_id)
                    time.sleep(1)
        
        return pd.DataFrame(all_penalties)

def main():
    scraper = SofaScorePenaltyScraper()
    
    try:
        print("Starting to collect all Super Lig penalty data...")
        penalties_df = scraper.collect_all_super_lig_penalties()
        
        if penalties_df.empty:
            print("\nNo penalties found.")
        else:
            print(f"\nFound {len(penalties_df)} penalties!")
            
            # Sort by date
            penalties_df = penalties_df.sort_values('match_date')
            
            # Save to CSV
            filename = f'superlig_penalties_{datetime.now().strftime("%Y%m%d")}.csv'
            penalties_df.to_csv(scraper.data_dir / filename, index=False)
            print(f"\nSaved to {filename}")
            
            # Save detailed JSON
            json_filename = f'superlig_penalties_{datetime.now().strftime("%Y%m%d")}.json'
            penalties_df.to_json(scraper.data_dir / json_filename, orient='records', indent=2)
            print(f"Saved detailed data to {json_filename}")
            
            # Print summary statistics
            print("\nPenalties by team:")
            print(penalties_df['taker_team'].value_counts())
            
            print("\nPenalty success rate by team:")
            for team in penalties_df['taker_team'].unique():
                team_pens = penalties_df[penalties_df['taker_team'] == team]
                scored = len(team_pens[team_pens['result'] == 'scored'])
                total = len(team_pens)
                if total > 0:
                    success_rate = (scored / total) * 100
                    print(f"{team}: {scored}/{total} ({success_rate:.1f}%)")
            
            # Show matches with penalties
            print("\nMatches with penalties:")
            for _, row in penalties_df.iterrows():
                print(f"{row['match_date']}: {row['home_team']} vs {row['away_team']} - {row['taker_name']} ({row['result']})")
            
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        raise e

if __name__ == "__main__":
    main()