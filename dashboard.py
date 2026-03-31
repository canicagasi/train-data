"""
Streamlit dashboard for IC27 Train Arrival Prediction.

Reads from the DuckDB database populated by main.py.
Configuration: config/dashboard_config.toml
Run with: uv run streamlit run dashboard.py
"""

from datetime import date, datetime
from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import numpy as np
import streamlit as st

from analysis import get_delay_data, analyze_delays

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib

CONFIG_PATH = Path("config/dashboard_config.toml")


def load_config() -> dict:
    """Load configuration from config/dashboard_config.toml."""
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Config file not found: {CONFIG_PATH}")
    with open(CONFIG_PATH, "rb") as f:
        return tomllib.load(f)


_config = load_config()

DB_PATH = _config["database"]["path"]
TRAIN_NUM = _config["train"]["number"]
STATION = _config["train"]["station"]
DEADLINE_MIN = _config["prediction"]["deadline_minutes"]


def main() -> None:
    st.set_page_config(page_title="IC27 Prediction", layout="wide")

    if not Path(DB_PATH).exists():
        st.error("Database not found. Run `uv run python main.py` first to fetch data.")
        return

    rows = get_delay_data(DB_PATH, TRAIN_NUM, STATION)
    if not rows:
        st.error("No data found in database.")
        return

    # Total days IC27 operated (for context)
    con = duckdb.connect(DB_PATH, read_only=True)
    tables = con.execute("SELECT table_schema, table_name FROM information_schema.tables").fetchall()
    schema, table = next((s, t) for s, t in tables if "train_arrival" in t.lower())
    fqn = f'"{schema}"."{table}"'
    total_days = con.execute(
        f"SELECT COUNT(DISTINCT departure_date) FROM {fqn} WHERE train_number = ?", [TRAIN_NUM]
    ).fetchone()[0]
    con.close()

    analysis = analyze_delays(rows, DEADLINE_MIN)
    if analysis is None:
        st.error("Could not analyze delays.")
        return

    delays = analysis["delays"]
    dates = analysis["dates"]
    prob = analysis["pct_within_deadline"]
    date_objs = [d if isinstance(d, date) and not isinstance(d, datetime)
                 else date.fromisoformat(str(d)[:10]) for d in dates]
    pct_late = 100 - prob
    worst_delay = float(np.max(delays))
    pct_over_5 = float(np.mean(delays > 5) * 100)

    # ── The Story ──────────────────────────────────────────────

    st.title("Should Jaana take IC27?")
    st.markdown(
        "Jaana needs to be at a basketball game in Tampere by **16:15** on Wednesday 2026-04-08. "
        "The transfer from the station takes 15 minutes, so IC27 must arrive by **16:00**. "
        "It's scheduled at **15:58** — giving her just a **2-minute buffer**."
    )
    missed_days = total_days - analysis["count"]
    st.markdown(
        "IC27 departs from Helsinki every day — over the last 15 months it ran on "
        f"**{total_days} days**, arriving at Tampere on **{analysis['count']}** of them "
        f"({missed_days} days had cancellations or missing data). "
        "We analyzed those arrivals to find out how often that 2-minute buffer holds up."
    )

    st.divider()

    # ── Chapter 1: The odds ────────────────────────────────────

    st.header("1. The odds are not in her favor")
    st.markdown(
        f"Out of {analysis['count']} recorded arrivals, IC27 arrived within 2 minutes of schedule "
        f"only **{prob:.0f}%** of the time. That means **{pct_late:.0f}%** of the time, "
        f"Jaana would miss her connection."
    )

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("On-Time Probability", f"{prob:.0f}%")
    col2.metric("Average Delay", f"{analysis['mean']:+.1f} min")
    col3.metric("Median Delay", f"{analysis['median']:+.1f} min")
    col4.metric("Worst Delay", f"+{worst_delay:.0f} min")

    st.divider()

    # ── Chapter 2: Where the delays cluster ────────────────────

    st.header("2. Most delays exceed her buffer")
    st.markdown(
        f"The histogram below shows how IC27 delays are distributed. "
        f"The red dashed line marks Jaana's 2-minute buffer. "
        f"Everything to the right of that line means she misses the game. "
        f"**{pct_over_5:.0f}%** of arrivals were more than 5 minutes late."
    )

    fig1, ax1 = plt.subplots(figsize=(8, 4))
    ax1.hist(delays, bins=30, edgecolor="black", alpha=0.7, color="steelblue")
    ax1.axvline(x=DEADLINE_MIN, color="red", linestyle="--", linewidth=2, label=f"+{DEADLINE_MIN} min (Jaana's limit)")
    ax1.axvline(x=0, color="green", linestyle="--", linewidth=1, label="On time")
    ax1.set_xlabel("Delay (minutes)")
    ax1.set_ylabel("Number of arrivals")
    ax1.legend()
    fig1.tight_layout()
    st.pyplot(fig1)

    st.divider()

    # ── Chapter 3: It's not getting better ─────────────────────

    st.header("3. Delays are not meaningfully improving over a long time period")
    st.markdown(
        "Use the slider to zoom into any date range — the pattern holds throughout the date range."
    )

    min_date, max_date = min(date_objs), max(date_objs)
    date_range = st.slider(
        "Select date range",
        min_value=min_date,
        max_value=max_date,
        value=(min_date, max_date),
        format="YYYY-MM-DD",
    )

    mask = np.array([(date_range[0] <= d <= date_range[1]) for d in date_objs])
    filtered_dates = [d for d, m in zip(date_objs, mask) if m]
    filtered_delays = delays[mask]

    if len(filtered_delays) > 0:
        f_prob = float(np.mean(filtered_delays <= DEADLINE_MIN) * 100)

        fc1, fc2, fc3 = st.columns(3)
        fc1.metric("Selected Period", f"{len(filtered_delays)} arrivals")
        fc2.metric("Avg Delay", f"{np.mean(filtered_delays):+.1f} min")
        fc3.metric("Median Delay", f"{np.median(filtered_delays):+.1f} min")

        col_chart, col_gauge = st.columns([3, 1])

        with col_chart:
            fig2, ax2 = plt.subplots(figsize=(10, 4))
            ax2.scatter(filtered_dates, filtered_delays, alpha=0.4, s=10, color="steelblue")
            ax2.axhline(y=DEADLINE_MIN, color="red", linestyle="--", linewidth=1, label=f"+{DEADLINE_MIN} min (Jaana's limit)")
            ax2.axhline(y=0, color="green", linestyle="--", linewidth=0.5)
            ax2.set_xlabel("Date")
            ax2.set_ylabel("Delay (minutes)")
            ax2.legend(fontsize=8)
            plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha="right")
            fig2.tight_layout()
            st.pyplot(fig2)

        with col_gauge:
            gauge_color = "green" if f_prob >= 70 else "orange" if f_prob >= 50 else "red"
            gauge_verdict = "SAFE" if f_prob >= 70 else "RISKY"

            fig_g, ax_g = plt.subplots(figsize=(4, 3))
            theta_start = np.pi
            theta_end = 0
            theta_fill = theta_start - (f_prob / 100) * np.pi

            bg_theta = np.linspace(theta_start, theta_end, 100)
            ax_g.plot(np.cos(bg_theta), np.sin(bg_theta), color="lightgray", linewidth=15, solid_capstyle="round")

            fill_theta = np.linspace(theta_start, theta_fill, 100)
            ax_g.plot(np.cos(fill_theta), np.sin(fill_theta), color=gauge_color, linewidth=15, solid_capstyle="round")

            ax_g.text(0, 0.15, f"{f_prob:.0f}%", ha="center", va="center", fontsize=28, fontweight="bold", color=gauge_color)
            ax_g.text(0, -0.15, gauge_verdict, ha="center", va="center", fontsize=14, fontweight="bold", color=gauge_color)
            ax_g.text(0, -0.4, f"P(delay <= {DEADLINE_MIN} min)", ha="center", va="center", fontsize=9, color="gray")
            ax_g.set_xlim(-1.3, 1.3)
            ax_g.set_ylim(-0.6, 1.3)
            ax_g.set_aspect("equal")
            ax_g.axis("off")
            fig_g.tight_layout()
            st.pyplot(fig_g)
    else:
        st.warning("No data in selected range.")

    st.divider()

    # ── Chapter 4: The trend confirms it ───────────────────────

    st.header("4. The rolling average tells the same story")
    st.markdown(
        "Smoothing out day-to-day noise with a 90-day rolling average reveals the underlying trend. "
        "The blue line (average delay) stays **above** the 2-minute buffer for the entire period. "
        "The green line (on-time percentage) never consistently reaches 70% — "
        "the level we'd need to call this a safe bet."
    )

    window = 90
    sorted_indices = np.argsort(date_objs)
    sorted_dates = [date_objs[i] for i in sorted_indices]
    sorted_delays = delays[sorted_indices]

    rolling_avg = np.convolve(sorted_delays, np.ones(window) / window, mode="valid")
    rolling_punctual = np.convolve(
        (sorted_delays <= DEADLINE_MIN).astype(float), np.ones(window) / window, mode="valid"
    ) * 100
    rolling_dates = sorted_dates[window - 1:]

    fig3, ax3 = plt.subplots(figsize=(10, 4))
    color_delay = "steelblue"
    ax3.plot(rolling_dates, rolling_avg, color=color_delay, linewidth=2, label="Avg delay (min)")
    ax3.axhline(y=DEADLINE_MIN, color="red", linestyle="--", linewidth=1, label="2-min buffer")
    ax3.set_xlabel("Date")
    ax3.set_ylabel("Avg Delay (min)", color=color_delay)
    ax3.tick_params(axis="y", labelcolor=color_delay)

    ax3_right = ax3.twinx()
    color_punct = "green"
    ax3_right.plot(rolling_dates, rolling_punctual, color=color_punct, linewidth=2, label="On-time %")
    ax3_right.set_ylabel("On-time %", color=color_punct)
    ax3_right.tick_params(axis="y", labelcolor=color_punct)
    ax3_right.set_ylim(0, 100)

    lines_1, labels_1 = ax3.get_legend_handles_labels()
    lines_2, labels_2 = ax3_right.get_legend_handles_labels()
    ax3.legend(lines_1 + lines_2, labels_1 + labels_2, fontsize=9)
    plt.setp(ax3.xaxis.get_majorticklabels(), rotation=45, ha="right")
    fig3.tight_layout()
    st.pyplot(fig3)

    st.divider()

    # ── Conclusion ─────────────────────────────────────────────

    st.header("Conclusion")

    if prob >= 70:
        st.success(
            f"**Jaana can take IC27.** With a {prob:.0f}% on-time rate, "
            f"the train is reliable enough for her 2-minute buffer."
        )
    else:
        st.error(
            f"**Jaana should take an earlier train.** "
            f"IC27 arrives within her 2-minute buffer only {prob:.0f}% of the time — "
            f"that's nearly a coin flip. With a one-time event like a basketball game, "
            f"a {pct_late:.0f}% chance of missing it is too high. "
            f"An earlier InterCity train would give her a comfortable margin."
        )

    st.caption(
        f"Based on {analysis['count']} IC27 arrivals at Tampere (TPE) "
        f"from {date_objs[0]} to {date_objs[-1]}. "
        f"Source: Finnish Transport Agency Digitraffic API."
    )


if __name__ == "__main__":
    main()
