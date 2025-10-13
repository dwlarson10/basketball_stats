"""
NBA Stats Harvester — 40 Seasons (Players & Teams)
-------------------------------------------------
Pulls per-season player and team stats from NBA.com Stats via the
`nba_api` Python package (no HTML scraping), then saves to Parquet and
(optionally) into DuckDB.

Usage
-----
# install deps
pip install nba_api pandas pyarrow duckdb tqdm tenacity

# run (defaults: 1985-2024 seasons → 1985-86 .. 2024-25)
python nba_stats_harvester.py --out data/

# custom window (e.g., 1990–2010 start years)
python nba_stats_harvester.py --start-year 1990 --end-year 2009 --out data/

# also write to DuckDB
python nba_stats_harvester.py --out data/ --duckdb data/nba.duckdb

Notes
-----
- Uses LeagueDash* endpoints (Base + Advanced). Advanced includes OffRtg,
  DefRtg, NetRtg, TS%, USG%, etc. (see docs for details).
- Adds a polite delay + retries to avoid rate-limits.
- Season strings follow NBA format: "YYYY-YY" (e.g., 1995-96).
- Default SeasonType is "Regular Season"; override with --season-type.
- If an endpoint hiccups for a given season, we log and continue.
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from dataclasses import dataclass
from typing import List

import pandas as pd
from tqdm import tqdm

# Optional deps
try:
    import duckdb  # type: ignore
except Exception:  # pragma: no cover
    duckdb = None

from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# nba_api endpoints
from nba_api.stats.endpoints import (
    leaguedashplayerstats,
    leaguedashteamstats,
)

# ----------------------
# Helpers & Retry Wrapper
# ----------------------

def season_str(year_start: int) -> str:
    return f"{year_start}-{str(year_start + 1)[-2:]}"

@dataclass
class PullConfig:
    start_year: int = 1985
    end_year: int = 2030  # inclusive start year; last season will be end_year–(end_year+1)
    season_type: str = "Regular Season"  # or "Playoffs"
    per_mode: str = "PerGame"           # or "Totals"
    league_id: str = "00"               # "00" = NBA, "10" = WNBA, "20" = G-League
    out_dir: str = "data"
    duckdb_path: str | None = None
    sleep_sec: float = 1.2

class NbaApiError(Exception):
    pass

# Generic retry for flaky network/HTTP hiccups
@retry(
    reraise=True,
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=1.5, min=1, max=10),
    retry=retry_if_exception_type(Exception),
)
def _call_players(season: str, season_type: str, measure: str, per_mode: str, league_id: str) -> pd.DataFrame:
    resp = leaguedashplayerstats.LeagueDashPlayerStats(
        season=season,
        season_type_all_star=season_type,
        per_mode_detailed=per_mode,
        measure_type_detailed_defense=measure,
        league_id_nullable=league_id,
        timeout=30,
    )
    df = resp.get_data_frames()[0]
    df.insert(0, "SEASON", season)
    df.insert(1, "SEASON_TYPE", season_type)
    df.insert(2, "MEASURE", measure)
    df.insert(3, "PER_MODE", per_mode)
    df.insert(4, "LEAGUE_ID", league_id)
    return df

@retry(
    reraise=True,
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=1.5, min=1, max=10),
    retry=retry_if_exception_type(Exception),
)
def _call_teams(season: str, season_type: str, measure: str, per_mode: str, league_id: str) -> pd.DataFrame:
    resp = leaguedashteamstats.LeagueDashTeamStats(
        season=season,
        season_type_all_star=season_type,
        per_mode_detailed=per_mode,
        measure_type_detailed_defense=measure,
        league_id_nullable=league_id,
        timeout=30,
    )
    df = resp.get_data_frames()[0]
    df.insert(0, "SEASON", season)
    df.insert(1, "SEASON_TYPE", season_type)
    df.insert(2, "MEASURE", measure)
    df.insert(3, "PER_MODE", per_mode)
    df.insert(4, "LEAGUE_ID", league_id)
    return df

# -------------
# Core pipeline
# -------------

def harvest(cfg: PullConfig) -> None:
    os.makedirs(cfg.out_dir, exist_ok=True)
    seasons: List[str] = [season_str(y) for y in range(cfg.start_year, cfg.end_year + 1)]

    players_frames: List[pd.DataFrame] = []
    teams_frames: List[pd.DataFrame] = []

    # Pull both Base and Advanced measures
    measures = ["Base", "Advanced"]

    for s in tqdm(seasons, desc="Seasons"):
        for m in measures:
            try:
                pdf = _call_players(s, cfg.season_type, m, cfg.per_mode, cfg.league_id)
                players_frames.append(pdf)
            except Exception as e:
                print(f"[WARN] Players {s} {m}: {e}", file=sys.stderr)
            time.sleep(cfg.sleep_sec)

            try:
                tdf = _call_teams(s, cfg.season_type, m, cfg.per_mode, cfg.league_id)
                teams_frames.append(tdf)
            except Exception as e:
                print(f"[WARN] Teams {s} {m}: {e}", file=sys.stderr)
            time.sleep(cfg.sleep_sec)

    if not players_frames:
        raise NbaApiError("No player frames collected.")
    if not teams_frames:
        raise NbaApiError("No team frames collected.")

    players = pd.concat(players_frames, ignore_index=True)
    teams = pd.concat(teams_frames, ignore_index=True)

    # Save Parquet
    players_path = os.path.join(cfg.out_dir, "players_40y.parquet")
    teams_path = os.path.join(cfg.out_dir, "teams_40y.parquet")
    players.to_parquet(players_path, index=False)
    teams.to_parquet(teams_path, index=False)
    print(f"Saved players → {players_path} ({len(players):,} rows)")
    print(f"Saved teams   → {teams_path} ({len(teams):,} rows)")
    
    # Optional: DuckDB sink
    if cfg.duckdb_path:
        if duckdb is None:
            raise RuntimeError("duckdb package not installed; pip install duckdb")
        con = duckdb.connect(cfg.duckdb_path)

        # Check if tables exist
        tables_exist = con.execute("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_name IN ('players_40y', 'teams_40y')
        """).fetchone()[0] > 0

        if tables_exist:
            # Incremental update: delete existing data for these seasons and league, then insert new
            seasons_list = ', '.join([f"'{s}'" for s in seasons])
            con.execute(f"DELETE FROM players_40y WHERE SEASON IN ({seasons_list}) AND LEAGUE_ID = '{cfg.league_id}'")
            con.execute(f"DELETE FROM teams_40y WHERE SEASON IN ({seasons_list}) AND LEAGUE_ID = '{cfg.league_id}'")

            # Insert new data
            con.register("players_df", players)
            con.register("teams_df", teams)
            con.execute("INSERT INTO players_40y SELECT * FROM players_df")
            con.execute("INSERT INTO teams_40y SELECT * FROM teams_df")
            print(f"Updated seasons {seasons_list} in {cfg.duckdb_path}")
        else:
            # Full rebuild: create tables from scratch
            con.register("players_df", players)
            con.register("teams_df", teams)
            con.execute("CREATE OR REPLACE TABLE players_40y AS SELECT * FROM players_df;")
            con.execute("CREATE OR REPLACE TABLE teams_40y AS SELECT * FROM teams_df;")
            print(f"Created tables players_40y & teams_40y → {cfg.duckdb_path}")

        con.close()

# -------------
# CLI
# -------------

def parse_args() -> PullConfig:
    p = argparse.ArgumentParser(description="Harvest NBA player & team stats across seasons using nba_api.")
    p.add_argument("--start-year", type=int, default=1985, help="First season start year (e.g., 1985 → 1985-86)")
    p.add_argument("--end-year", type=int, default=2030, help="Last season start year (e.g., 2024 → 2024-25)")
    p.add_argument("--season-type", type=str, default="Regular Season", choices=["Regular Season", "Playoffs"], help="SeasonType")
    p.add_argument("--per-mode", type=str, default="PerGame", choices=["PerGame", "Totals"], help="Per-mode for endpoints")
    p.add_argument("--league-id", type=str, default="00", choices=["00", "10", "20"], help="League ID: 00=NBA, 10=WNBA, 20=G-League")
    p.add_argument("--out", type=str, required=True, help="Output directory for Parquet files")
    p.add_argument("--duckdb", type=str, default=None, help="Optional DuckDB file path to write tables")
    p.add_argument("--sleep", type=float, default=1.2, help="Seconds to sleep between calls")
    a = p.parse_args()
    return PullConfig(
        start_year=a.start_year,
        end_year=a.end_year,
        season_type=a.season_type,
        per_mode=a.per_mode,
        league_id=a.league_id,
        out_dir=a.out,
        duckdb_path=a.duckdb,
        sleep_sec=a.sleep,
    )

if __name__ == "__main__":
    cfg = parse_args()
    harvest(cfg)
