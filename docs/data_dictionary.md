# GBIF SIS2 V2 — Data Dictionary

| Original field | Type | Meaning | Quality / transformation |
|---|---|---|---|
| key | Integer | GBIF unique occurrence key | Positive and unique, SQLite PRIMARY KEY |
| scientificName | String | Recorded organism scientific name | Nonempty length >=2, whitespace standardized |
| kingdom | String | Taxonomic kingdom | Missing values explicitly labeled Unknown after validation |
| basisOfRecord | String | GBIF record evidence type | Must be accepted GBIF category; balanced sampling uses HUMAN_OBSERVATION and PRESERVED_SPECIMEN |
| eventDate | Date-time/string | GBIF event date | Optional but parseable when present; UTC conversion |
| year | Integer | Event year | Optional, valid 1600–2026; derive from eventDate if missing |
| decimalLatitude | Float | North/south coordinate degrees | -90 to 90 |
| decimalLongitude | Float | East/west coordinate degrees | -180 to 180 |
| countryCode | String | ISO two-letter country code | KZ only |
| datasetKey | String | GBIF source dataset identifier | Optional |
| period | String (derived) | 2016–2018, 2019–2021, 2022–2024 | Derived from year; used for cross-period grouping |
| distance_to_almaty_km | Float (derived) | Great-circle distance to fixed Almaty reference point | Haversine formula via NumPy; km |

Sampling: fixed quotas across 3 periods x 2 record bases. Counts by basis and period **are fixed by construction**, not natural frequencies. Raw JSON objects remain unmodified in `data/raw_data.json`. CSVs/SQLite are transformed outputs.
