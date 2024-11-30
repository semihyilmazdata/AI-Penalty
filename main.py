import requests
import pandas as pd
from datetime import datetime
import time
import json
from pathlib import Path

class SofaScorePenaltyScraper:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://www.sofascore.com/'
        }
        self.tournament_id = 52
        self.season_id = 63814
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

    def get_team_matches(self, team_id):
        """Get all matches for a specific team"""
        url = f"{self.base_url}/team/{team_id}/events/last/0"
        response = self._make_request(url)
        matches = response.get('events', [])
        
        # Save raw data for debugging
        with open(self.data_dir / f'raw_matches_{team_id}.json', 'w', encoding='utf-8') as f:
            json.dump(matches, f, indent=2)
            
        # Filter only Super Lig matches
        super_lig_matches = [
            match for match in matches 
            if match.get('tournament', {}).get('uniqueTournament', {}).get('id') == self.tournament_id
        ]
        
        print(f"Found {len(super_lig_matches)} Super Lig matches for team {team_id}")
        return super_lig_matches

    def get_match_incidents(self, event_id, match_info):
        """Get penalty incidents for a specific match"""
        url = f"{self.base_url}/event/{event_id}/incidents"
        response = self._make_request(url)
        incidents = response.get('incidents', [])
        
        # Save raw data for debugging
        with open(self.data_dir / f'incidents_{event_id}.json', 'w', encoding='utf-8') as f:
            json.dump(incidents, f, indent=2)
        
        penalties = []
        for incident in incidents:
            # Debugging
            print(f"Checking incident: {json.dumps(incident, indent=2)}")
            
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
                    'timestamp': match_info.get('startTimestamp'),
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
                    'score_at_time': f"{incident.get('homeScore', 0)}-{incident.get('awayScore', 0)}",
                    'reason': incident.get('reason'),
                    'raw_incident': json.dumps(incident)  # Store raw data for debugging
                }
                penalties.append(penalty_data)
                print(f"Found penalty in {match_info['homeTeam']['name']} vs {match_info['awayTeam']['name']}")
                print("Penalty details:", json.dumps(penalty_data, indent=2))
        
        return penalties

    def collect_team_penalties(self, team_id, team_name):
        """Collect all penalties from matches involving specified team"""
        all_penalties = []
        matches = self.get_team_matches(team_id)
        
        for match in matches:
            print(f"\nProcessing: {match['homeTeam']['name']} vs {match['awayTeam']['name']}")
            print(f"Match ID: {match['id']}")
            penalties = self.get_match_incidents(match['id'], match)
            if penalties:
                all_penalties.extend(penalties)
            time.sleep(1)
        
        # Convert to DataFrame and filter
        df = pd.DataFrame(all_penalties)
        if not df.empty:
            team_penalties = df[
                (df['home_team'] == team_name) | 
                (df['away_team'] == team_name)
            ].copy()
            
            # Add additional columns
            team_penalties['is_for_team'] = team_penalties['taker_team'] == team_name
            team_penalties['opposition'] = team_penalties.apply(
                lambda x: x['away_team'] if x['home_team'] == team_name else x['home_team'],
                axis=1
            )
        else:
            team_penalties = df
            
        return team_penalties

def main():
    scraper = SofaScorePenaltyScraper()
    team_id = 3061  # Galatasaray
    team_name = "Galatasaray"
    
    try:
        print(f"Starting to collect penalty data for {team_name}...")
        penalties_df = scraper.collect_team_penalties(team_id, team_name)
        
        if penalties_df.empty:
            print("\nNo penalties found.")
            
            # Check raw data files
            print("\nChecking raw data files in the data directory:")
            for file in sorted(scraper.data_dir.glob('*.json')):
                print(f"\nContents of {file.name}:")
                with open(file, 'r') as f:
                    data = json.load(f)
                    print(json.dumps(data, indent=2))
        else:
            print(f"\nFound {len(penalties_df)} penalties!")
            
            # Sort by date
            penalties_df = penalties_df.sort_values('timestamp')
            
            # Save to CSV
            filename = f'penalties_{team_name.lower()}_{datetime.now().strftime("%Y%m%d")}.csv'
            penalties_df.to_csv(scraper.data_dir / filename, index=False)
            print(f"\nSaved to {filename}")
            
            # Save detailed JSON
            json_filename = f'penalties_{team_name.lower()}_{datetime.now().strftime("%Y%m%d")}.json'
            penalties_df.to_json(scraper.data_dir / json_filename, orient='records', indent=2)
            print(f"Saved detailed data to {json_filename}")
            
            # Print summary
            print("\nPenalties breakdown:")
            print(f"Total penalties: {len(penalties_df)}")
            print(f"\nFor {team_name}:")
            team_pens = penalties_df[penalties_df['is_for_team']]
            print(f"Total: {len(team_pens)}")
            print(f"Scored: {len(team_pens[team_pens['result'] == 'scored'])}")
            print(f"Missed/Saved/Not awarded: {len(team_pens[team_pens['result'] != 'scored'])}")
            
            print(f"\nAgainst {team_name}:")
            against_pens = penalties_df[~penalties_df['is_for_team']]
            print(f"Total: {len(against_pens)}")
            print(f"Scored: {len(against_pens[against_pens['result'] == 'scored'])}")
            print(f"Missed/Saved/Not awarded: {len(against_pens[against_pens['result'] != 'scored'])}")
            
            # Show matches with penalties
            print("\nMatches with penalties:")
            for _, row in penalties_df.iterrows():
                print(f"{row['match_date']}: {row['home_team']} vs {row['away_team']} - {row['taker_name']} ({row['result']})")
            
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        raise e

if __name__ == "__main__":
    main()