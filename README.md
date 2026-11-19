# SIS2 GBIF Biodiversity Data Quality Pipeline

This project collects authentic biodiversity occurrence records from the GBIF REST API, sends them through a real Apache Kafka broker, validates them using Pandera, processes them with pandas and NumPy, stores them in SQLite, and analyzes them through three visualizations.

**Research question:** What are the taxonomic, temporal, spatial, and data-quality characteristics **within a fixed-quota sample** of GBIF occurrences from Kazakhstan?

## Project components

| Component | File | Role |
| --- | --- | --- |
| Orchestration | `main.py` | Runs the end-to-end pipeline |
| Collection | `src/api_client.py` | GBIF API, pagination, error handling, quotas, preserved JSON |
| Kafka Producer | `src/producer.py` | Publishes JSON messages to the real Kafka topic |
| Kafka Consumer | `src/consumer.py` | Reads matching messages with termination and timeout |
| Validation | `src/validator.py` | Pandera schema, invalid-record quarantine, three deliberate invalid tests |
| Cleaning | `src/cleaner.py` | Missing data, duplicates, types, normalization, NumPy distance, GroupBy, pivot |
| Database | `src/loader.py` | SQLite table, primary key, conflict checks, repeat-load verification |
| SQL verification | `src/verify_db.py` | Displays SQL queries and their results |
| EDA | `src/analysis.py` | Descriptive tables and three PNG figures |
| Optional metrics PDF | `src/reporter.py` | Generates `report/project_report.pdf` from actual pipeline metrics |
| Tests | `tests/test_pipeline.py` | Basic offline functional tests |

The finished submission report is `report/DC_SIS_2_Report.pdf`. 

## Software

- Python 3.10+ and the packages in `requirements.txt`
- Docker Desktop with Docker Compose
- Apache Kafka 4.1.2, configured in `docker-compose.yml`
- Network access for a new GBIF download; **not required** when using the supplied cached raw JSON with `--reuse`

## Installation (Mac / VS Code)

Open a terminal **in the extracted project directory**, where `main.py` is located:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

Start Docker Desktop. Then start the Kafka broker:

```bash
docker compose up -d
docker compose ps
```

If a broker named `gbif-course-kafka` is **already running** from an older project folder on port `9092`, avoid starting a second copy. Check with `docker ps --filter name=gbif-course-kafka` and use the existing broker, or stop the old Compose stack before starting this one. Do not use `docker compose down -v` unnecessarily.

## Run the full pipeline

The uploaded repository includes the authentic 300-record source sample in `data/raw_data.json` and its matching `data/collection_manifest.json`. To reproduce the existing result while **still publishing and consuming through a real Kafka broker**:

```bash
python main.py --count 300 --reuse
```

To collect a **new** sample from the live GBIF API:

```bash
python main.py --count 300 --database data/new_snapshot.db
```

A new API response can differ from the saved sample. A separate database avoids conflicts with previously stored GBIF keys. When using a new snapshot, `--reuse` replays whatever sample is currently saved in `data/raw_data.json`.

**Run the automated tests and inspect SQLite:**

```bash
python -m pytest -q
python -m src.verify_db
```

The `verify_db` command checks the default `data/output_v2.db`. To inspect a different SQLite file, use SQLite directly or adapt the verification script accordingly.

## Run Kafka Producer and Consumer separately (optional demonstration)

The regular pipeline launches both from `main.py`. To show them separately, use two terminals and the **same** `run_id`. Start the Consumer **first**:

```bash
python -m src.consumer --run-id manual_demo_001 --expected 300 --bootstrap localhost:9092 --topic gbif_observations --output data/manual_consumed.jsonl --timeout 180
```

Then start the Producer in the second terminal:

```bash
python -m src.producer --run-id manual_demo_001 --input data/raw_data.json --bootstrap localhost:9092 --topic gbif_observations
```

## Dataset and sampling design

GBIF API endpoint: `https://api.gbif.org/v1/occurrence/search`

Requests include `country=KZ`, `hasCoordinate=true`, period/year and `basisOfRecord` filters, and pagination (`limit`, `offset`). The fixed-quota design crosses three periods (**2016–2018**, **2019–2021**, **2022–2024**) with two categories (**HUMAN_OBSERVATION** and **PRESERVED_SPECIMEN**), producing six strata of 50 records each for `--count 300`.

## Validation, transformation and SQL

The Pandera schema checks positive/unique identifiers, scientific-name requirements, allowed categories, valid coordinates, KZ country code, year range and parseable dates. Invalid original records are quarantined in `data/invalid_records.csv`; detailed errors are in `data/validation_errors.csv`.

When valid authentic rows exist, `src/validator.py` also tests **three deliberately invalid records** (latitude 120, year 3000, invalid basis), recording results in `data/test_validation_errors.csv`. These synthetic tests **do not** enter the production Kafka stream or SQLite database.

Cleaning checks and handles missing values and duplicate GBIF keys, normalizes text, converts mixed date representations and numerical fields, and computes Haversine distances from Almaty using NumPy. Analysis includes a filter for human observations within 500 km, grouped statistics and a `pivot_table`.

`data/output_v2.db` contains an `occurrences` table keyed by GBIF ID. Identical repeated loads are safe. Conflicting values for an existing ID raise a `RecordConflictError` without partially inserting the batch. The pipeline checks the same data twice to demonstrate idempotency and queries the stored batch back from SQLite for EDA.

## EDA and deliverables

Generated EDA outputs:

- `figures/01_period_kingdom.png` — kingdom composition within sampled periods
- `figures/02_kingdom_locations.png` — sample geography by kingdom
- `figures/03_basis_distances.png` — distances from Almaty by observation basis
- `figures/descriptive_statistics.csv`, `figures/grouped_statistics.csv`, `figures/period_kingdom_pivot.csv`, `figures/categorical_statistics.csv` — supporting tables

Data and verification artifacts include `data/metrics.json`, the original JSON and manifest, Kafka-consumed JSONL, Pandera output CSVs, cleaned records, and SQLite. `docs/data_dictionary.md` defines the dataset fields.

## References

- [GBIF Occurrence API](https://techdocs.gbif.org/en/openapi/v1/occurrence)
- [Apache Kafka](https://kafka.apache.org/documentation/)
- [Pandera](https://pandera.readthedocs.io/)
- [pandas](https://pandas.pydata.org/docs/)
- [NumPy](https://numpy.org/doc/)
- [SQLite3](https://docs.python.org/3/library/sqlite3.html)
