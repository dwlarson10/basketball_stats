# Basketball Stats Dashboard & Analysis

A Flask-based web application for exploring NBA and WNBA basketball statistics with plans for predictive modeling. This project is an exploration in **vibe coding** - building with intuition, iteration, and the joy of discovery.

## What It Does

This application provides comprehensive basketball statistics analysis across both NBA and WNBA leagues:

### Current Features
- **Player Statistics Dashboard**: View detailed player stats by team, league, and season
  - Per-game averages: points, rebounds, assists, steals, blocks
  - Shooting percentages: FG%, 3P%, FT%, TS%, eFG%
  - Advanced metrics: offensive/defensive rating, usage %, pace, PIE

- **Team Head-to-Head Matchups**: Compare two teams side-by-side
  - Win-loss records and winning percentage
  - Offensive and defensive statistics
  - Advanced team metrics with color-coded comparisons
  - Visual identification of statistical advantages

- **Multi-League Support**:
  - NBA (30 teams)
  - WNBA (13 teams including 2025 expansion)
  - Historical data from 2023-present

- **Live Data Refresh**: Pull the latest statistics from the NBA Stats API
  - Automatic fetching for both NBA and WNBA
  - Incremental updates without overwriting historical data

### Coming Soon
- **Predictive Modeling**: ML-powered game outcome predictions
- **Player Comparisons**: Head-to-head player stat analysis
- **Trend Visualization**: Historical performance tracking
- **Advanced Analytics**: Strength of schedule, momentum metrics, and more

## How to Run

### Prerequisites
- Python 3.8+
- Virtual environment (recommended)

### Installation

1. **Clone the repository**
   ```bash
   cd basketball_stats
   ```

2. **Set up virtual environment**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

### Running the Application

**Start the Flask server:**
```bash
python app.py
```

The application will be available at:
- Main Dashboard: http://localhost:5000/
- Team Matchup: http://localhost:5000/matchup

### Data Management

**Initial Data Load** (if starting fresh):
```bash
# Activate virtual environment first
source .venv/bin/activate

# Fetch NBA data (2023-2025)
python scrapper/harvester.py --start-year 2023 --end-year 2025 --league-id 00 --out data/ --duckdb data/nba.duckdb

# Fetch WNBA data (2023-2025)
python scrapper/harvester.py --start-year 2023 --end-year 2025 --league-id 10 --out data/ --duckdb data/nba.duckdb
```

**Refresh Current Season** (via web UI):
- Click the "↻ Refresh Data" button in the top right of the dashboard
- Automatically fetches both NBA and WNBA for the current season

**Manual Refresh** (via command line):
```bash
source .venv/bin/activate

# Current season for both leagues
python scrapper/harvester.py --start-year 2025 --end-year 2025 --league-id 00 --out data/ --duckdb data/nba.duckdb
python scrapper/harvester.py --start-year 2025 --end-year 2025 --league-id 10 --out data/ --duckdb data/nba.duckdb
```

## Project Structure

```
basketball_stats/
├── app.py                      # Flask application and API routes
├── scrapper/
│   └── harvester.py           # NBA Stats API data collection
├── templates/
│   ├── index.html             # Player statistics dashboard
│   └── matchup.html           # Team head-to-head comparison
├── data/
│   ├── nba.duckdb             # DuckDB database with all stats
│   ├── players_40y.parquet    # Player data (Parquet format)
│   └── teams_40y.parquet      # Team data (Parquet format)
├── requirements.txt           # Python dependencies
└── readme.md                  # This file
```

## Tech Stack

- **Backend**: Flask (Python web framework)
- **Database**: DuckDB (embedded analytical database)
- **Data Format**: Parquet (columnar storage)
- **Data Source**: NBA Stats API via `nba_api` package
- **Frontend**: Vanilla JavaScript with responsive CSS

## About This Project

This is an exploration in **vibe coding** - a development philosophy that embraces:
- **Intuition over rigidity**: Building what feels right, iterating based on real use
- **Joy of discovery**: Learning ML, data engineering, and web development through doing
- **Continuous evolution**: Starting simple, adding complexity as needs emerge
- **Practical experimentation**: Real data, real insights, real fun

The goal is to learn about machine learning, statistical analysis, and predictive modeling through the lens of basketball - a domain rich with data, strategy, and human performance.

### Project Evolution
- ✅ **Phase 1**: Data engineering pipeline (NBA Stats API → DuckDB)
- ✅ **Phase 2**: Web dashboard for exploring stats
- 🔄 **Phase 3**: Team comparison and matchup analysis (in progress)
- 🔜 **Phase 4**: Predictive modeling with ML
- 🔜 **Phase 5**: Advanced analytics and visualizations
- 💭 **Future**: LLM-powered natural language queries

## Contributing

This is a personal learning project, but feedback and ideas are always welcome! Feel free to open issues or suggest features.

## License

This project is for educational and personal use. All basketball statistics are sourced from publicly available NBA Stats API data.

---

**Note**: This project is not affiliated with the NBA or WNBA. All team names, logos, and statistics are property of their respective owners.
