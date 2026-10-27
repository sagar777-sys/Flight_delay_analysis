# Airline Flight Delay Analytics (Apache Spark + Streamlit)

Batch big-data analytics: PySpark reads and cleans flight records, then computes on-time
performance by airline, delays by airport, route, month and hour, and the causes of delays.
Streamlit shows it as a web dashboard with filters, CSV upload and report downloads.

**Data:** `flights.csv` is generated SAMPLE data (20,000 flights, `generate_sample_data.py`).
Airline names are fictional and the numbers are not real. It has 8 intentionally bad rows to
demonstrate cleaning. For a bigger demo run `python generate_sample_data.py 500000` on a
computer with Spark.

## Architecture
CSV -> PySpark (clean, compute delays, group, aggregate) -> small summary tables -> Streamlit

## Files
- `app.py` - Streamlit dashboard
- `spark_engine.py` - PySpark processing (the core)
- `pandas_engine.py` - fallback used only if Spark cannot start on the host
- `common.py` - small shared helper (percentage columns)
- `generate_sample_data.py` - creates `flights.csv`
- `requirements.txt`, `packages.txt` (Java for hosting), `Dockerfile` (optional, Render)

## CSV columns
`flight_id, airline, origin, destination, scheduled_departure, actual_departure,
scheduled_arrival, actual_arrival, cancelled (0/1), delay_reason, distance_km`
(timestamps as `YYYY-MM-DD HH:MM`; actual times may be blank for cancelled flights;
delay_reason can be `None`)

## Definitions
- Arrival delay = actual arrival - scheduled arrival (minutes).
- A flight is "delayed" if it arrives more than the chosen threshold late (default 15 min).
- On-time % and delayed % are calculated on flights that actually flew (not cancelled).

## Cleaning rules
Removes flights with missing airline/airport/ID, same origin and destination, invalid times,
invalid cancelled flag, distance <= 0, flown flights with missing actual times, or delays
beyond -90 / +1440 minutes.

## Run locally
    pip install -r requirements.txt   # also needs Java 17+
    streamlit run app.py
