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
    # Try to find the table with nba schema first, then without
    try:
        con.execute(f"SELECT 1 FROM nba.{base_name} LIMIT 1")
        return f"nba.{base_name}"
    except:
        return base_name

def get_teams() -> List[Dict[str, Any]]:
    """Get list of unique teams from the most recent season."""
    con = get_db_connection()
    table_name = get_table_name(con, "teams_40y")

    query = f"""
    SELECT DISTINCT TEAM_ID, TEAM_NAME
    FROM {table_name}
    WHERE SEASON = (SELECT MAX(SEASON) FROM {table_name})
      AND MEASURE = 'Base'
    ORDER BY TEAM_NAME
    """

    result = con.execute(query).fetchall()
    con.close()

    return [{"id": row[0], "name": row[1]} for row in result]

def get_player_stats(team_id: int, season: str = None) -> List[Dict[str, Any]]:
    """Get player statistics for a specific team."""
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

@app.route('/')
def index():
    """Main dashboard page."""
    teams = get_teams()
    seasons = get_available_seasons()
    return render_template('index.html', teams=teams, seasons=seasons)

@app.route('/api/players/<int:team_id>')
def api_players(team_id: int):
    """API endpoint to get player stats for a team."""
    season = request.args.get('season', None)
    players = get_player_stats(team_id, season)
    return jsonify(players)

@app.route('/api/teams')
def api_teams():
    """API endpoint to get all teams."""
    teams = get_teams()
    return jsonify(teams)

@app.route('/api/seasons')
def api_seasons():
    """API endpoint to get available seasons."""
    seasons = get_available_seasons()
    return jsonify(seasons)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
