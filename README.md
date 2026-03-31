# IC27 Train Arrival Prediction — Case Study

Predicts whether InterCity IC27 will arrive at Tampere (TPE) in time for Jaana to reach a basketball game at 16:15 on Wednesday 2026-04-08. Transfer from the station takes 15 minutes, so IC27 must arrive by 16:00.

## Prerequisites

- [uv](https://docs.astral.sh/uv/getting-started/installation/) — Python package manager
- [Make](https://www.gnu.org/software/make/) (optional) — for running build targets

## Setup

```bash
# Install dependencies, fetch data, and launch dashboard
make all


# Or run steps individually:
make setup      # Install dependencies (uv sync)
make run        # Fetch data & generate prediction (main.py)
make dashboard  # Launch Streamlit dashboard
make clean      # Remove generated files (database, plots, pipeline state)
```

### Without Make

```bash
uv sync
uv run python main.py
uv run streamlit run dashboard.py
```

The dashboard in http://localhost:8501 will be displayed.

Pipeline configuration is in `.dlt/config.toml`, dashboard configuration is in `config/dashboard_config.toml`.

## How It Works

### 1. Data Ingestion (dlt + Digitraffic API)

The pipeline uses [dlt](https://dlthub.com/) to fetch historical train data from the [Finnish Transport Agency's Digitraffic API](https://www.digitraffic.fi/en/railway-traffic/). It fetches IC27 timetable data for every day from 2023-01-01 to 2026-03-31.

Configuration is in `.dlt/config.toml`.

### 2. Database (DuckDB)

Data is loaded into a local DuckDB database (`trains.duckdb`). DuckDB is chosen for its:
- Zero-configuration embedded operation
- Excellent analytical query performance
- Native integration with dlt

**Note**: dbt is not used in this project — with a single data source and no complex data modeling requirements. In a production environment with multiple sources, dbt would handle staging, cleaning, and building analytical models (see [Production Architecture](#production-architecture)).

### 3. Visualization (Streamlit)

An interactive Streamlit dashboard (`make dashboard`) presents the analysis with date range sliders to explore any time period. The dashboard walks through the data step by step — from key metrics to delay distribution, time series, rolling averages, and a probability gauge that updates as you adjust the date range.

### 4. Forecast Justification

**The data**: ~452 IC27 arrival records at Tampere over 15 months (Jan 2025 – Mar 2026).

**Key findings**:
- Average delay: **+3.5 minutes** (exceeds the 2-minute buffer)
- Median delay: **+2.0 minutes** (right at the limit)
- **P(delay ≤ 2 min) ≈ 56%** — roughly a coin flip

**Recommendation**: Jaana should take an **earlier train**. A 56% chance of making it is too risky for a one-time event — she'd have nearly a 1-in-2 chance of missing the basketball game.

**Visualizations** (`prediction_results.png`) support this conclusion:

1. **Histogram** — the delay distribution is right-skewed. Most arrivals cluster near schedule, but a long tail of late arrivals (up to 15+ minutes) pulls the average above the deadline
2. **Time series** — delays show no improving trend over the 15-month period. There is no reason to expect April 8th will be better than the historical average
3. **90-day rolling average** — smoothed trends for both average delay and on-time percentage, showing how punctuality evolves over time

## Production Architecture

If deployed in a production analytical environment (tools are subject to change based on organizational standards, including orchestration), the architecture would look like this:

```
Digitraffic API ──→ Ingestion Pipeline (dlt) ──→ Data Lake (S3/GCS/ADLS)
                                                        │
                                                  Data Warehouse
                                                        │
                                                  dbt transformations
                                                        │    
                                                    Dashboard            
                                              
```

- **Ingestion**: dlt pipelines on a daily schedule with an orchestrator of choice for incremental loading (only new dates fetched each run)
- **Storage**: S3/GCS/ADLS as the data lake / landing zone, with DuckDB for development and a data warehouse for production
- **Transformation**: dbt models in two layers — staging (raw API data → cleaned, typed records) and marts (delay aggregations, station-level metrics, day-of-week patterns)
- **Prediction**: Batch nightly job recomputing delay distributions from the latest data. On the day of travel, a real-time layer consumes Digitraffic's MQTT feed for live position updates
- **Presentation**: Streamlit or any visualization tool (depending on other requirements) as serving layer for analysts to explore delay patterns
- **Quality**: dbt tests for data freshness and completeness, Great Expectations for schema validation, monitoring for distribution drift in delay patterns

## Open Data Discussion

The Finnish Transport Agency's open railway data (Digitraffic) has significant potential for creating business value beyond this case study.

### Opportunities
- **Commuter apps**: Delay prediction and alternative routing — "your train is likely 5 min late, here's a backup connection"
- **Urban planning**: Station usage patterns and peak-hour congestion data to inform infrastructure investment
- **Tourism**: Train reliability scores for travel planning tools, helping visitors choose dependable connections

### Strengths
- **High granularity**: Per-station arrival and departure timestamps, not just origin/destination
- **Historical depth**: Multiple years of data enable seasonal and trend analysis
- **Real-time feeds**: Live train positions and delay updates
- **Cause codes**: Delay reason categories (weather, infrastructure, rolling stock) enable root-cause analysis

### Challenges
- **Rate limits**: Fetching large historical date ranges requires careful pacing to avoid throttling
- **Schedule changes**: Timetable revisions across years make longitudinal comparisons harder — the same train number may run different routes or times
- **No passenger data**: Privacy regulations mean no ridership counts, limiting demand-side analysis
- **Timezone handling**: All timestamps are UTC, requiring DST-aware conversion for Finnish local time display
- **Uneven coverage**: Data completeness varies across smaller regional stations compared to major hubs

## Project Structure

```
.dlt/config.toml          # dlt pipeline configuration (API, dates)
config/dashboard_config.toml        # Dashboard configuration (train, station, deadline)
Makefile                  # Build targets: setup, run, dashboard, clean, all
main.py                   # Data pipeline: API → DuckDB → Analysis → Visualization
dashboard.py              # Streamlit dashboard for interactive exploration
```

## Possible Improvements

- **More features**: Incorporate weather data (FMI open data), holidays, and track maintenance schedules
- **Real-time**: Use Digitraffic's feed for live delay updates on the day of travel
- **Alternative trains**: Analyze earlier IC trains as backup options with their own delay distributions
- **dbt models**: Add staging and mart layers with dbt for cleaner data transformation and data modeling if needed.
- **Testing**: Add data quality tests and pipeline monitoring
