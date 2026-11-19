
import json
from pathlib import Path
import pandas as pd
import pandera.pandas as pa
from pandera import Check

BASIS = {"HUMAN_OBSERVATION", "MACHINE_OBSERVATION", "PRESERVED_SPECIMEN",
         "LIVING_SPECIMEN", "FOSSIL_SPECIMEN", "MATERIAL_SAMPLE", "MATERIAL_CITATION",
         "MATERIAL_ENTITY", "OCCURRENCE", "OBSERVATION"}


def date_ok(value):
    if value is None or pd.isna(value) or value == "":
        return True
    return not pd.isna(pd.to_datetime(value, utc=True, errors="coerce"))

SCHEMA = pa.DataFrameSchema({
    "key": pa.Column(int, nullable=False, unique=True, coerce=True,
                     checks=Check.ge(1)),
    "scientificName": pa.Column(str, nullable=False, checks=Check.str_length(min_value=2)),
    "kingdom": pa.Column(str, nullable=True),
    "basisOfRecord": pa.Column(str, nullable=False, checks=Check.isin(sorted(BASIS))),
    "eventDate": pa.Column(object, nullable=True, checks=Check(lambda s: s.map(date_ok))),
    "year": pa.Column(float, nullable=True, coerce=True,
                      checks=[Check.ge(1600), Check.le(2026)]),
    "decimalLatitude": pa.Column(float, nullable=False, coerce=True,
                                  checks=Check.in_range(-90, 90)),
    "decimalLongitude": pa.Column(float, nullable=False, coerce=True,
                                   checks=Check.in_range(-180, 180)),
    "countryCode": pa.Column(str, nullable=False, checks=Check.eq("KZ")),
    "datasetKey": pa.Column(str, nullable=True)
}, strict=True)


def validate(records, output_dir="data", prefix=""):
    """Return valid DataFrame and rejected DataFrame, preserving source indices."""
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(records)
    if frame.empty:
        raise ValueError("No records to validate")
    missing = set(SCHEMA.columns) - set(frame.columns)
    if missing:
        raise ValueError(f"Required fields absent: {sorted(missing)}")
    # No cleaning, imputation, duplicate removal or type repairs before validation.
    try:
        checked = SCHEMA.validate(frame.copy(), lazy=True)
        rejected_indices = set()
        failures = pd.DataFrame(columns=["index", "column", "check", "failure_case"])
    except pa.errors.SchemaErrors as error:
        failures = error.failure_cases.copy()
        candidate = failures["index"].dropna()
        rejected_indices = set(int(i) for i in candidate if str(i).lstrip("-").isdigit())
        # Fatal dataframe-level problems must never be hidden or counted as valid.
        if failures["index"].isna().any():
            raise ValueError(f"Dataset-level schema failure requires investigation: {failures.to_string()}") from error
        checked = frame.copy()
    rejected = frame.loc[sorted(rejected_indices)].copy()
    valid = checked.drop(index=sorted(rejected_indices)).copy()
    failures.to_csv(out / f"{prefix}validation_errors.csv", index=False)
    rejected.to_csv(out / f"{prefix}invalid_records.csv", index=False)
    valid.to_csv(out / f"{prefix}valid_records.csv", index=False)
    print(f"Pandera valid={len(valid)} invalid={len(rejected)}", flush=True)
    return valid, rejected, failures


def run_invalid_tests(valid, output_dir="data"):
    """Three deliberately invalid records are tested but never inserted or streamed."""
    if valid.empty:
        raise ValueError("Cannot create test rows from empty valid data")
    base = valid.iloc[0].to_dict()
    cases = []
    row = base.copy(); row["key"] = int(base["key"]) + 10**12; row["decimalLatitude"] = 120.0; cases.append(row)
    row = base.copy(); row["key"] = int(base["key"]) + 10**12 + 1; row["year"] = 3000.0; cases.append(row)
    row = base.copy(); row["key"] = int(base["key"]) + 10**12 + 2; row["basisOfRecord"] = "NOT_A_REAL_BASIS"; cases.append(row)
    good, bad, errors = validate(cases, output_dir=output_dir, prefix="test_")
    if len(bad) != 3 or len(good) != 0:
        raise AssertionError(f"Expected all three deliberately invalid rows to fail, got {len(bad)}")
    return {"deliberate_test_count": 3, "detected_invalid": len(bad),
            "test_errors": len(errors)}
