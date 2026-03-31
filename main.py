"""
IC27 Train Arrival Prediction — Case Study

Predicts whether InterCity IC27 will arrive at Tampere (TPE) in time
for Jaana to reach a basketball game at 16:15 on Wednesday 2026-04-08.
Transfer from station takes 15 minutes -> must arrive by 16:00.

Uses dlt to load data into DuckDB.
"""

from collections.abc import Iterator
from datetime import date, datetime, timedelta
from pathlib import Path

import dlt
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from dlt.sources.rest_api import rest_api_source, RESTAPIConfig

from analysis import get_delay_data, analyze_delays


# ============================================================
# Data Fetching & Loading via dlt REST API Source
# ============================================================

@dlt.resource
def train_dates(start: date, end: date):
    """Parent resource: yields dates for the child resource to fetch."""
    yield [
        {"date": (start + timedelta(days=i)).isoformat()}
        for i in range((end - start).days + 1)
    ]


def flatten_train_rows(train: dict[str, object]) -> Iterator[dict[str, object]]:
    """Flatten timeTableRows from a single train response into individual records."""
    for row in train.get("timeTableRows", []):
        yield {
            "departure_date": train.get("departureDate"),
            "train_number": train.get("trainNumber"),
            "train_type": train.get("trainType"),
            "train_category": train.get("trainCategory"),
            "cancelled": train.get("cancelled", False),
            "station_short_code": row.get("stationShortCode"),
            "row_type": row.get("type"),
            "scheduled_time": row.get("scheduledTime"),
            "actual_time": row.get("actualTime"),
            "difference_in_minutes": row.get("differenceInMinutes"),
            "commercial_stop": row.get("commercialStop", False),
            "train_stopping": row.get("trainStopping", False),
            "row_cancelled": row.get("cancelled", False),
        }


def create_source(api_base: str, train_number: int, start: date, end: date) -> dlt.sources.DltSource:
    """Create a dlt REST API source for fetching train data."""
    config: RESTAPIConfig = {
        "client": {
            "base_url": api_base,
            "headers": {"Digitraffic-User": "case-study"},
        },
        "resources": [
            train_dates(start, end),
            {
                "name": "train_arrivals",
                "endpoint": {
                    "path": f"trains/{{resources.train_dates.date}}/{train_number}",
                    "data_selector": "$[0]",
                    "paginator": "single_page",
                    "response_actions": [
                        {"status_code": 404, "action": "ignore"},
                    ],
                },
                "processing_steps": [
                    {"yield_map": flatten_train_rows},
                ],
            },
        ],
    }
    return rest_api_source(config)


# ============================================================
# Visualization
# ============================================================

def create_visualizations(analysis: dict, train_num: int, deadline_min: int, output_path: Path) -> None:
    delays = analysis["delays"]
    dates = analysis["dates"]

    date_objs = [d if isinstance(d, date) and not isinstance(d, datetime)
                 else date.fromisoformat(str(d)[:10]) for d in dates]

    fig = plt.figure(figsize=(20, 5))
    gs = fig.add_gridspec(1, 4, width_ratios=[1, 1, 1, 0.8])
    ax1 = fig.add_subplot(gs[0])
    ax2 = fig.add_subplot(gs[1])
    ax3 = fig.add_subplot(gs[2])
    ax4 = fig.add_subplot(gs[3])
    fig.suptitle(f"IC{train_num} Arrival Delay at Tampere (TPE)", fontsize=13, fontweight="bold")

    # Plot 1: Delay histogram
    ax1.hist(delays, bins=30, edgecolor="black", alpha=0.7, color="steelblue")
    ax1.axvline(x=deadline_min, color="red", linestyle="--", linewidth=2, label=f"+{deadline_min} min deadline")
    ax1.axvline(x=0, color="green", linestyle="--", linewidth=1, label="On time")
    ax1.set_title("Delay Distribution")
    ax1.set_xlabel("Delay (minutes)")
    ax1.set_ylabel("Frequency")
    ax1.legend()

    # Plot 2: Delays over time
    ax2.scatter(date_objs, delays, alpha=0.4, s=10, color="steelblue")
    ax2.axhline(y=deadline_min, color="red", linestyle="--", linewidth=1, label=f"+{deadline_min} min deadline")
    ax2.axhline(y=0, color="green", linestyle="--", linewidth=0.5)
    ax2.set_title("Delay Over Time")
    ax2.set_xlabel("Date")
    ax2.set_ylabel("Delay (minutes)")
    ax2.legend(fontsize=8)
    plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha="right")

    # Plot 3: 90-day rolling average delay & punctuality
    window = 90
    sorted_indices = np.argsort(date_objs)
    sorted_dates = [date_objs[i] for i in sorted_indices]
    sorted_delays = delays[sorted_indices]

    rolling_avg = np.convolve(sorted_delays, np.ones(window) / window, mode="valid")
    rolling_punctual = np.convolve(
        (sorted_delays <= deadline_min).astype(float), np.ones(window) / window, mode="valid"
    ) * 100
    rolling_dates = sorted_dates[window - 1:]

    color_delay = "steelblue"
    ax3.plot(rolling_dates, rolling_avg, color=color_delay, linewidth=2, label="Avg delay")
    ax3.axhline(y=deadline_min, color="red", linestyle="--", linewidth=1)
    ax3.set_xlabel("Date")
    ax3.set_ylabel("Avg Delay (min)", color=color_delay)
    ax3.tick_params(axis="y", labelcolor=color_delay)

    ax3_right = ax3.twinx()
    color_punct = "green"
    ax3_right.plot(rolling_dates, rolling_punctual, color=color_punct, linewidth=2, label="Punctuality %")
    ax3_right.set_ylabel("On-time %", color=color_punct)
    ax3_right.tick_params(axis="y", labelcolor=color_punct)
    ax3_right.set_ylim(0, 100)

    ax3.set_title(f"{window}-Day Rolling Average")
    lines_1, labels_1 = ax3.get_legend_handles_labels()
    lines_2, labels_2 = ax3_right.get_legend_handles_labels()
    ax3.legend(lines_1 + lines_2, labels_1 + labels_2, fontsize=8)
    plt.setp(ax3.xaxis.get_majorticklabels(), rotation=45, ha="right")

    # Plot 4: Probability gauge
    prob = analysis["pct_within_deadline"]
    color = "green" if prob >= 70 else "orange" if prob >= 50 else "red"
    verdict = "SAFE" if prob >= 70 else "RISKY"

    theta_start = np.pi
    theta_end = 0
    theta_fill = theta_start - (prob / 100) * np.pi

    bg_theta = np.linspace(theta_start, theta_end, 100)
    ax4.plot(np.cos(bg_theta), np.sin(bg_theta), color="lightgray", linewidth=15, solid_capstyle="round")

    fill_theta = np.linspace(theta_start, theta_fill, 100)
    ax4.plot(np.cos(fill_theta), np.sin(fill_theta), color=color, linewidth=15, solid_capstyle="round")

    ax4.text(0, 0.15, f"{prob:.0f}%", ha="center", va="center", fontsize=28, fontweight="bold", color=color)
    ax4.text(0, -0.15, verdict, ha="center", va="center", fontsize=14, fontweight="bold", color=color)
    ax4.text(0, -0.4, f"P(delay <= {deadline_min} min)", ha="center", va="center", fontsize=9, color="gray")
    ax4.set_xlim(-1.3, 1.3)
    ax4.set_ylim(-0.6, 1.3)
    ax4.set_aspect("equal")
    ax4.axis("off")
    ax4.set_title("On-Time Probability")

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    print(f"  Visualization saved to {output_path}")


# ============================================================
# Recommendation
# ============================================================

def print_recommendation(analysis: dict, train_num: int, deadline_min: int) -> None:
    prob = analysis["pct_within_deadline"]

    print("\n" + "=" * 70)
    print(f"  PREDICTION: IC{train_num} ARRIVAL AT TAMPERE (TPE)")
    print("=" * 70)
    print(f"  Data points:    {analysis['count']}")
    print(f"  Average delay:  {analysis['mean']:+.1f} min")
    print(f"  Median delay:   {analysis['median']:+.1f} min")
    print(f"  P(delay <= {deadline_min}min): {prob:.1f}%")

    if prob >= 70:
        print(f"\n  RECOMMENDATION: IC{train_num} is LIKELY SAFE.")
        print(f"  Jaana can take this train ({prob:.0f}% chance of making it).")
    else:
        print(f"\n  RECOMMENDATION: IC{train_num} is TOO RISKY.")
        print(f"  With only {prob:.0f}% chance of arriving within {deadline_min} min of schedule,")
        print(f"  Jaana should take an EARLIER train.")
    print("=" * 70)


# ============================================================
# Main
# ============================================================

def main() -> None:
    config = dlt.config
    train_num = config.get("digitraffic.train_number", int) or 27
    target_station = config.get("digitraffic.target_station", str) or "TPE"
    target_date = config.get("prediction.target_date", str) or "2026-04-08"
    deadline_min = config.get("prediction.deadline_minutes", int) or 2
    api_base = config.get("digitraffic.api_base_url", str) or "https://rata.digitraffic.fi/api/v1"
    fetch_start = date.fromisoformat(config.get("digitraffic.fetch_start_date", str) or "2025-01-01")
    fetch_end = date.fromisoformat(config.get("digitraffic.fetch_end_date", str) or "2026-03-31")

    print("=" * 70)
    print("  IC27 TRAIN ARRIVAL PREDICTION — CASE STUDY")
    print("=" * 70)
    print(f"\n  Target: Arrive at {target_station} by 16:00 on {target_date}")
    print(f"  Train:  InterCity IC{train_num}")
    print(f"  Buffer: {deadline_min} minutes from scheduled arrival")

    # Step 1: Fetch from API & load into DuckDB via dlt
    print("\n[1/3] Fetching data from Digitraffic API & loading into DuckDB...")
    pipeline = dlt.pipeline(
        pipeline_name="digitraffic_trains",
        destination="duckdb",
        dataset_name="trains",
        pipelines_dir=".dlt/pipelines",
    )
    source = create_source(api_base, train_num, fetch_start, fetch_end)
    load_info = pipeline.run(source, write_disposition="replace")
    print(f"  {load_info}", flush=True)

    # Step 2: Analyze
    print("\n[2/3] Analyzing delay patterns...")
    db_path = Path("digitraffic_trains.duckdb")
    rows = get_delay_data(str(db_path), train_num, target_station)
    print(f"  Found {len(rows)} {target_station} arrival records.")
    analysis = analyze_delays(rows, deadline_min)

    if analysis is None:
        print("  No data to analyze. Exiting.")
        return

    # Step 3: Visualize & recommend
    print("\n[3/3] Creating visualizations...")
    output_image = Path("prediction_results.png")
    create_visualizations(analysis, train_num, deadline_min, output_image)
    print_recommendation(analysis, train_num, deadline_min)

    print(f"\n  Database: {db_path}")
    print(f"  Visualization: {output_image}")


if __name__ == "__main__":
    main()
