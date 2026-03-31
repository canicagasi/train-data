.PHONY: setup run dashboard clean all

all: setup run dashboard

setup:
	uv sync

run:
	uv run python main.py

dashboard:
	uv run streamlit run dashboard.py

clean:
	rm -f digitraffic_trains.duckdb digitraffic_trains.duckdb.wal prediction_results.png
	rm -rf .dlt/pipelines/
