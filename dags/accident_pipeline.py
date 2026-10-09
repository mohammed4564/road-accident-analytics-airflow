
import csv
import json
import os
from collections import Counter
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator


INPUT_FILE = Path("/opt/airflow/data/raw/accidents.csv")
OUTPUT_DIR = Path("/opt/airflow/output")
CLEAN_DIR = OUTPUT_DIR / "cleaned"
QUALITY_DIR = OUTPUT_DIR / "quality"
ANALYTICS_DIR = OUTPUT_DIR / "analytics"

REQUIRED_COLUMNS = [
    "Severity", "StartTime", "EndTime", "StartLat", "StartLng",
    "Distance", "Street", "City", "State", "Zipcode",
    "Temperature", "Humidity", "Pressure", "Visibility", "WindSpeed",
    "Precipitation", "WeatherCondition", "Amenity", "Bump",
    "Crossing", "GiveWay", "Junction", "NoExit", "Railway",
    "Roundabout", "Station", "Stop", "TrafficCalming", "TrafficSignal",
]

DERIVED_COLUMNS = [
    "AccidentDate", "StartHour", "DayOfWeek",
    "Month", "DurationMinutes", "IsWeekend",
]


def parse_datetime(value):
    """Parse common ISO-style timestamps without assuming a timezone."""
    if not value or not value.strip():
        return None

    value = value.strip()

    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def write_counter(path, key_name, counter):
    """Write an aggregate summary as a small CSV file."""
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow([key_name, "AccidentCount"])

        for key, count in counter.most_common():
            writer.writerow([key, count])


def validate_input():
    if not INPUT_FILE.is_file():
        raise FileNotFoundError(
            f"Input CSV not found: {INPUT_FILE}. "
            "Check the Windows data folder and Docker volume mounts."
        )

    if INPUT_FILE.stat().st_size == 0:
        raise ValueError("The input CSV is empty.")

    with INPUT_FILE.open(
        "r", newline="", encoding="utf-8-sig"
    ) as file:
        reader = csv.reader(file)
        header = next(reader, None)

    if not header:
        raise ValueError("The input CSV has no header.")

    if len(header) != len(set(header)):
        raise ValueError("Duplicate column names found in CSV header.")

    missing = sorted(set(REQUIRED_COLUMNS) - set(header))

    if missing:
        raise ValueError(f"Required columns are missing: {missing}")

    extra = sorted(set(header) - set(REQUIRED_COLUMNS))

    print(f"Input size: {INPUT_FILE.stat().st_size:,} bytes")
    print(f"Columns found: {len(header)}")
    print(f"Additional columns: {extra}")
    print("Input schema validation passed.")


def process_accidents():
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)
    QUALITY_DIR.mkdir(parents=True, exist_ok=True)
    ANALYTICS_DIR.mkdir(parents=True, exist_ok=True)

    cleaned_file = CLEAN_DIR / "accidents_cleaned.csv"

    counts = Counter()
    missing_values = Counter()
    severity_counts = Counter()
    city_counts = Counter()
    hour_counts = Counter()
    weather_counts = Counter()
    weekday_counts = Counter()
    road_feature_counts = Counter()

    invalid_start_time = 0
    invalid_end_time = 0
    invalid_coordinates = 0
    negative_durations = 0
    processed_rows = 0

    with INPUT_FILE.open(
        "r", newline="", encoding="utf-8-sig"
    ) as source:
        reader = csv.DictReader(source)

        output_columns = list(reader.fieldnames) + [
            column for column in DERIVED_COLUMNS
            if column not in reader.fieldnames
        ]

        with cleaned_file.open(
            "w", newline="", encoding="utf-8"
        ) as destination:
            writer = csv.DictWriter(
                destination,
                fieldnames=output_columns,
                extrasaction="ignore",
            )
            writer.writeheader()

            for row in reader:
                processed_rows += 1

                # Track missing values without loading rows into memory.
                for column in REQUIRED_COLUMNS:
                    if not (row.get(column) or "").strip():
                        missing_values[column] += 1

                severity = (row.get("Severity") or "").strip()
                city = (row.get("City") or "").strip()
                weather = (
                    row.get("WeatherCondition") or ""
                ).strip()

                severity_counts[severity or "Unknown"] += 1
                city_counts[city or "Unknown"] += 1
                weather_counts[weather or "Unknown"] += 1

                start = parse_datetime(row.get("StartTime"))
                end = parse_datetime(row.get("EndTime"))

                if start is None:
                    invalid_start_time += 1

                if end is None:
                    invalid_end_time += 1

                duration = ""

                if start is not None:
                    row["AccidentDate"] = start.date().isoformat()
                    row["StartHour"] = start.hour
                    row["DayOfWeek"] = start.strftime("%A")
                    row["Month"] = start.strftime("%Y-%m")
                    hour_counts[start.hour] += 1
                    weekday_counts[start.strftime("%A")] += 1
                else:
                    row["AccidentDate"] = ""
                    row["StartHour"] = ""
                    row["DayOfWeek"] = ""
                    row["Month"] = ""

                if start is not None and end is not None:
                    # Avoid comparing naive and timezone-aware timestamps.
                    if (
                        (start.tzinfo is None) != (end.tzinfo is None)
                    ):
                        duration = ""
                    else:
                        seconds = (end - start).total_seconds()

                        if seconds < 0:
                            negative_durations += 1
                        else:
                            duration = round(seconds / 60, 2)

                row["DurationMinutes"] = duration
                row["IsWeekend"] = (
                    start.weekday() >= 5 if start else ""
                )

                try:
                    lat = float(row["StartLat"])
                    lng = float(row["StartLng"])

                    if not (-90 <= lat <= 90 and -180 <= lng <= 180):
                        invalid_coordinates += 1
                except (ValueError, TypeError):
                    invalid_coordinates += 1

                # Count presence of each road feature.
                for feature in [
                    "Amenity", "Bump", "Crossing", "GiveWay",
                    "Junction", "NoExit", "Railway", "Roundabout",
                    "Station", "Stop", "TrafficCalming", "TrafficSignal",
                ]:
                    value = (row.get(feature) or "").strip().lower()

                    if value in ("true", "1", "yes"):
                        road_feature_counts[feature] += 1

                writer.writerow(row)

                if processed_rows % 500000 == 0:
                    print(f"Processed {processed_rows:,} rows")

    counts["processed_rows"] = processed_rows
    counts["invalid_start_time"] = invalid_start_time
    counts["invalid_end_time"] = invalid_end_time
    counts["invalid_coordinates"] = invalid_coordinates
    counts["negative_durations"] = negative_durations

    report = {
        "source_file": INPUT_FILE.name,
        "processed_rows": processed_rows,
        "invalid_start_time": invalid_start_time,
        "invalid_end_time": invalid_end_time,
        "invalid_coordinates": invalid_coordinates,
        "negative_durations": negative_durations,
        "missing_values": dict(missing_values),
        "cleaned_file": str(cleaned_file),
    }

    with (QUALITY_DIR / "data_quality.json").open(
        "w", encoding="utf-8"
    ) as file:
        json.dump(report, file, indent=2)

    write_counter(
        ANALYTICS_DIR / "accidents_by_severity.csv",
        "Severity", severity_counts,
    )
    write_counter(
        ANALYTICS_DIR / "accidents_by_city.csv",
        "City", city_counts,
    )
    write_counter(
        ANALYTICS_DIR / "accidents_by_hour.csv",
        "StartHour", hour_counts,
    )
    write_counter(
        ANALYTICS_DIR / "accidents_by_weather.csv",
        "WeatherCondition", weather_counts,
    )
    write_counter(
        ANALYTICS_DIR / "accidents_by_weekday.csv",
        "DayOfWeek", weekday_counts,
    )
    write_counter(
        ANALYTICS_DIR / "road_feature_presence.csv",
        "RoadFeature", road_feature_counts,
    )

    print(json.dumps(counts, indent=2))
    print("Transformation and aggregation completed.")


def verify_outputs():
    cleaned_file = CLEAN_DIR / "accidents_cleaned.csv"
    quality_file = QUALITY_DIR / "data_quality.json"

    expected_files = [
        cleaned_file,
        quality_file,
        ANALYTICS_DIR / "accidents_by_severity.csv",
        ANALYTICS_DIR / "accidents_by_city.csv",
        ANALYTICS_DIR / "accidents_by_hour.csv",
        ANALYTICS_DIR / "accidents_by_weather.csv",
        ANALYTICS_DIR / "accidents_by_weekday.csv",
        ANALYTICS_DIR / "road_feature_presence.csv",
    ]

    for path in expected_files:
        if not path.is_file() or path.stat().st_size == 0:
            raise ValueError(f"Missing or empty output: {path}")

    with quality_file.open(encoding="utf-8") as file:
        report = json.load(file)

    if report["processed_rows"] == 0:
        raise ValueError("No data rows were processed.")

    print(f"Verified {len(expected_files)} output files.")
    print(f"Rows processed: {report['processed_rows']:,}")
    print("Output validation passed.")


with DAG(
    dag_id="road_accident_analytics",
    description="Validate, clean and summarize road accident data",
    start_date=datetime(2026, 1, 1),
    schedule="*/2 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["data-engineering", "road-safety", "etl"],
) as dag:

    validate_task = PythonOperator(
        task_id="validate_input",
        python_callable=validate_input,
    )

    process_task = PythonOperator(
        task_id="process_and_aggregate",
        python_callable=process_accidents,
    )

    verify_task = PythonOperator(
        task_id="verify_outputs",
        python_callable=verify_outputs,
    )

    validate_task >> process_task >> verify_task