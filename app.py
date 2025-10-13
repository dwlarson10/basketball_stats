"""
NBA Stats Dashboard - Flask Application
--------------------------------------
A Flask web application for monitoring daily basketball statistics using
DuckDB and Parquet files for data storage.

Usage:
    flask run --debug
    or
    python app.py
"""
from flask import Flask, render_template, request, jsonify
import duckdb
import os
import subprocess
import sys
from datetime import datetime
from typing import List, Dict, Any

app = Flask(__name__)

# Configuration
DATA_DIR = "data"
DUCKDB_PATH = os.path.join(DATA_DIR, "nba.duckdb")
PLAYERS_PARQUET = os.path.join(DATA_DIR, "players_40y.parquet")
TEAMS_PARQUET = os.path.join(DATA_DIR, "teams_40y.parquet")

def get_db_connection():
    """Create a DuckDB connection with data loaded."""
    if os.path.exists(DUCKDB_PATH):
        return duckdb.connect(DUCKDB_PATH, read_only=True)
    else:
        # Fallback to reading Parquet files directly
        con = duckdb.connect(":memory:")
        con.execute(f"CREATE TABLE players_40y AS SELECT * FROM read_parquet('{PLAYERS_PARQUET}')")
        con.execute(f"CREATE TABLE teams_40y AS SELECT * FROM read_parquet('{TEAMS_PARQUET}')")
        return con

def get_table_name(con, base_name: str) -> str:
    """Determine the correct table name (with or without schema prefix)."""
    # Tables are in main schema, not nba schema
    return base_name

def get_leagues() -> List[str]:
    """Get list of available leagues."""
    # For now, return the hardcoded list of leagues
    # In the future, this could be dynamic based on database content
    return ["NBA", "WNBA", "International"]

def get_teams(league: str = "NBA") -> List[Dict[str, Any]]:
    """Get list of unique teams from the most recent season for a specific league."""
    con = get_db_connection()
    table_name = get_table_name(con, "teams_40y")

    # Define team ID ranges for each league
    # NBA: 1610612700-1610612799
    # WNBA: 1611661300-1611661399
    # International/Other: everything else
    league_filters = {
        "NBA": "AND CAST(TEAM_ID AS BIGINT) BETWEEN 1610612700 AND 1610612799",
        "WNBA": "AND CAST(TEAM_ID AS BIGINT) BETWEEN 1611661300 AND 1611661399",
        "International": "AND (CAST(TEAM_ID AS BIGINT) < 1610612700 OR CAST(TEAM_ID AS BIGINT) BETWEEN 1612709800 AND 1612709999)"
    }

    league_filter = league_filters.get(league, "")

    query = f"""
    SELECT DISTINCT TEAM_ID, TEAM_NAME
    FROM {table_name}
    WHERE SEASON = (SELECT MAX(SEASON) FROM {table_name})
      AND MEASURE = 'Base'
      {league_filter}
    ORDER BY TEAM_NAME
    """

    result = con.execute(query).fetchall()
    con.close()

    return [{"id": int(row[0]), "name": row[1]} for row in result]

def get_player_stats(team_id: int, season: str = None) -> List[Dict[str, Any]]:
    """Get player statistics for a specific team."""
    # TODO: INVESTIGATE HERE - This is where the SQL query runs and could fail with "string did not match"
    # The error might be a DuckDB error caused by:
    # 1. TEAM_ID type mismatch - check if TEAM_ID in the database is BIGINT but you're passing INT
    # 2. SEASON format issue - ensure season string matches format in database (e.g., "2024-25")
    # 3. Column name mismatch between what's queried and what exists in the table
    # TIP: Try running this query directly in DuckDB with test values to see the exact error
    # TIP: Add try-except around con.execute() to catch and log the specific DuckDB error
    con = get_db_connection()
    table_name = get_table_name(con, "players_40y")

    # If no season specified, get the most recent one
    if not season:
        season_query = f"SELECT MAX(SEASON) FROM {table_name}"
        season = con.execute(season_query).fetchone()[0]

    # Get player stats - merge Base and Advanced measures
    query = f"""
    WITH base_stats AS (
        SELECT
            PLAYER_ID,
            PLAYER_NAME,
            TEAM_ABBREVIATION,
            AGE,
            GP,
            MIN,
            FGM, FGA, FG_PCT,
            FG3M, FG3A, FG3_PCT,
            FTM, FTA, FT_PCT,
            OREB, DREB, REB,
            AST, STL, BLK, TOV,
            PTS
        FROM {table_name}
        WHERE TEAM_ID = ?
          AND SEASON = ?
          AND MEASURE = 'Base'
    ),
    advanced_stats AS (
        SELECT
            PLAYER_ID,
            OFF_RATING,
            DEF_RATING,
            NET_RATING,
            AST_PCT,
            AST_TO,
            AST_RATIO,
            OREB_PCT,
            DREB_PCT,
            REB_PCT,
            TM_TOV_PCT,
            EFG_PCT,
            TS_PCT,
            USG_PCT,
            PACE,
            PIE
        FROM {table_name}
        WHERE TEAM_ID = ?
          AND SEASON = ?
          AND MEASURE = 'Advanced'
    )
    SELECT
        b.*,
        a.OFF_RATING,
        a.DEF_RATING,
        a.NET_RATING,
        a.TS_PCT,
        a.USG_PCT,
        a.PIE
    FROM base_stats b
    LEFT JOIN advanced_stats a ON b.PLAYER_ID = a.PLAYER_ID
    ORDER BY b.PTS DESC
    """

    result = con.execute(query, [team_id, season, team_id, season]).fetchdf()
    con.close()

    return result.to_dict('records')

def get_available_seasons() -> List[str]:
    """Get list of all available seasons."""
    con = get_db_connection()
    table_name = get_table_name(con, "players_40y")

    query = f"""
    SELECT DISTINCT SEASON
    FROM {table_name}
    ORDER BY SEASON DESC
    """

    result = con.execute(query).fetchall()
    con.close()

    return [row[0] for row in result]

def get_team_stats(team_id: int, season: str = None) -> Dict[str, Any]:
    """Get aggregated team statistics for a specific team and season."""
    con = get_db_connection()
    table_name = get_table_name(con, "teams_40y")

    # If no season specified, get the most recent one
    if not season:
        season_query = f"SELECT MAX(SEASON) FROM {table_name}"
        season = con.execute(season_query).fetchone()[0]

    # Get team stats - merge Base and Advanced measures
    query = f"""
    WITH base_stats AS (
        SELECT
            TEAM_ID,
            TEAM_NAME,
            GP,
            W,
            L,
            W_PCT,
            MIN,
            FGM, FGA, FG_PCT,
            FG3M, FG3A, FG3_PCT,
            FTM, FTA, FT_PCT,
            OREB, DREB, REB,
            AST, STL, BLK, TOV, PF,
            PTS,
            PLUS_MINUS
        FROM {table_name}
        WHERE TEAM_ID = ?
          AND SEASON = ?
          AND MEASURE = 'Base'
    ),
    advanced_stats AS (
        SELECT
            TEAM_ID,
            OFF_RATING,
            DEF_RATING,
            NET_RATING,
            AST_PCT,
            AST_TO,
            AST_RATIO,
            OREB_PCT,
            DREB_PCT,
            REB_PCT,
            TM_TOV_PCT,
            EFG_PCT,
            TS_PCT,
            PACE,
            PIE
        FROM {table_name}
        WHERE TEAM_ID = ?
          AND SEASON = ?
          AND MEASURE = 'Advanced'
    )
    SELECT
        b.*,
        a.OFF_RATING,
        a.DEF_RATING,
        a.NET_RATING,
        a.AST_PCT,
        a.AST_TO,
        a.OREB_PCT,
        a.DREB_PCT,
        a.REB_PCT,
        a.EFG_PCT,
        a.TS_PCT,
        a.PACE,
        a.PIE
    FROM base_stats b
    LEFT JOIN advanced_stats a ON b.TEAM_ID = a.TEAM_ID
    """

    result = con.execute(query, [team_id, season, team_id, season]).fetchdf()
    con.close()

    if len(result) == 0:
        return {}

    return result.to_dict('records')[0]

@app.route('/')
def index():
    """Main dashboard page."""
    leagues = get_leagues()
    seasons = get_available_seasons()
    return render_template('index.html', leagues=leagues, seasons=seasons)

@app.route('/matchup')
def matchup():
    """Head-to-head matchup page."""
    leagues = get_leagues()
    seasons = get_available_seasons()
    return render_template('matchup.html', leagues=leagues, seasons=seasons)

@app.route('/api/players/<int:team_id>')
def api_players(team_id: int):
    """API endpoint to get player stats for a team."""
    # TODO: INVESTIGATE HERE - The "string did not match" error likely occurs in this endpoint
    # Check the following:
    # 1. Is team_id being passed correctly as an integer?
    # 2. Is the season parameter being parsed correctly from the query string?
    # 3. Does the database query in get_player_stats() handle the inputs properly?
    # TIP: Add error handling and logging here to see what values are being received
    season = request.args.get('season', None)
    players = get_player_stats(team_id, season)
    return jsonify(players)

@app.route('/api/leagues')
def api_leagues():
    """API endpoint to get all leagues."""
    leagues = get_leagues()
    return jsonify(leagues)

@app.route('/api/teams')
def api_teams():
    """API endpoint to get all teams for a specific league."""
    league = request.args.get('league', 'NBA')
    teams = get_teams(league)
    return jsonify(teams)

@app.route('/api/seasons')
def api_seasons():
    """API endpoint to get available seasons."""
    seasons = get_available_seasons()
    return jsonify(seasons)

@app.route('/api/team-stats/<int:team_id>')
def api_team_stats(team_id: int):
    """API endpoint to get aggregated team stats."""
    season = request.args.get('season', None)
    stats = get_team_stats(team_id, season)
    return jsonify(stats)

@app.route('/api/matchup')
def api_matchup():
    """API endpoint to get head-to-head team comparison."""
    team1_id = request.args.get('team1_id', type=int)
    team2_id = request.args.get('team2_id', type=int)
    season = request.args.get('season', None)

    if not team1_id or not team2_id:
        return jsonify({'error': 'Both team1_id and team2_id are required'}), 400

    team1_stats = get_team_stats(team1_id, season)
    team2_stats = get_team_stats(team2_id, season)

    return jsonify({
        'team1': team1_stats,
        'team2': team2_stats,
        'season': season or get_available_seasons()[0]
    })

@app.route('/api/refresh-data', methods=['POST'])
def api_refresh_data():
    """API endpoint to trigger data refresh from NBA API."""
    try:
        # Get the most recent season in the database to determine what to pull
        con = get_db_connection()
        table_name = get_table_name(con, "players_40y")
        latest_season = con.execute(f"SELECT MAX(SEASON) FROM {table_name}").fetchone()[0]
        con.close()

        # Parse the latest season (e.g., "2024-25" -> 2024)
        if latest_season:
            start_year = int(latest_season.split('-')[0])
        else:
            start_year = datetime.now().year

        # Only pull current season (incremental update)
        # NBA/WNBA seasons run from October/November through June, so:
        # - If it's before October, the current season started last year
        # - If it's October or later, the current season starts this year
        # Note: In early October, the new season may not have games/stats yet in the API
        now = datetime.now()
        if now.month < 10:
            current_year = now.year - 1
        else:
            current_year = now.year

        # Run the harvester script for both NBA and WNBA
        harvester_path = os.path.join(os.path.dirname(__file__), 'scrapper', 'harvester.py')

        all_output = []
        all_errors = []

        # Fetch NBA data (league_id=00)
        for league_id, league_name in [('00', 'NBA'), ('10', 'WNBA')]:
            result = subprocess.run(
                [
                    sys.executable,
                    harvester_path,
                    '--start-year', str(current_year),
                    '--end-year', str(current_year),
                    '--league-id', league_id,
                    '--out', DATA_DIR,
                    '--duckdb', DUCKDB_PATH
                ],
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout per league
            )

            if result.returncode == 0:
                all_output.append(f'{league_name}: {result.stdout}')
            else:
                all_errors.append(f'{league_name} error: {result.stderr}')

        if not all_errors:
            return jsonify({
                'success': True,
                'message': f'Data refreshed successfully for {current_year}-{str(current_year+1)[-2:]} season (NBA & WNBA)',
                'output': '\n'.join(all_output)
            })
        else:
            return jsonify({
                'success': False,
                'message': 'Failed to refresh some data',
                'output': '\n'.join(all_output),
                'error': '\n'.join(all_errors)
            }), 500

    except subprocess.TimeoutExpired:
        return jsonify({
            'success': False,
            'message': 'Data refresh timed out after 5 minutes'
        }), 500
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Error refreshing data: {str(e)}'
        }), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
