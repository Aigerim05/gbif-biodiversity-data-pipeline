
import json
import math
from datetime import datetime, timezone
from pathlib import Path
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

ENDPOINT = "https://api.gbif.org/v1/occurrence/search"
PERIODS = [(2016, 2018), (2019, 2021), (2022, 2024)]
BASES = ["HUMAN_OBSERVATION", "PRESERVED_SPECIMEN"]


def strata_for(count):
    if count < 200:
        raise ValueError("At least 200 records are required")
    n = len(PERIODS) * len(BASES)
    base, remainder = divmod(count, n)
    return [(lo, hi, basis, base + (index < remainder))
            for index, (lo, hi, basis) in enumerate((lo, hi, basis)
                for lo, hi in PERIODS for basis in BASES)]


def collect(count=300, raw_path="data/raw_data.json", reuse=False):
    target = Path(raw_path)
    manifest = target.with_name("collection_manifest.json")
    if reuse:
        if not target.is_file() or not manifest.is_file():
            raise FileNotFoundError("--reuse needs data/raw_data.json and data/collection_manifest.json from V2")
        metadata = json.loads(manifest.read_text(encoding="utf-8"))
        if metadata.get("sampling_version") != 2 or metadata.get("record_count") != count:
            raise ValueError("Cached dataset was collected with another strategy/count; run without --reuse")
        rows = json.loads(target.read_text(encoding="utf-8"))
        if len(rows) != count:
            raise ValueError("Saved raw data and manifest counts differ")
        return rows
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=Retry(
        total=4, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"])))
    rows, details, used = [], [], set()
    for lo, hi, basis, goal in strata_for(count):
        taken, offset, duplicates = 0, 0, 0
        # Pagination handles rare overlap; no replacement/fabrication if a stratum is sparse.
        while taken < goal:
            limit = min(300, max(goal - taken + duplicates, 40))
            params = {"country": "KZ", "year": f"{lo},{hi}", "basisOfRecord": basis,
                      "hasCoordinate": "true", "limit": limit, "offset": offset}
            response = session.get(ENDPOINT, params=params, timeout=(10, 45),
                                   headers={"User-Agent": "GBIF-SIS2-educational-v2/2.0"})
            response.raise_for_status()
            payload = response.json()
            batch = payload.get("results")
            if not isinstance(batch, list) or not batch:
                raise RuntimeError(f"GBIF has too few results for {lo}-{hi} {basis}: {taken}/{goal}. "
                                   "Do not invent missing records; adjust strata after checking API.")
            for record in batch:
                key = record.get("key")
                if key in used:
                    duplicates += 1
                    continue
                rows.append(record)
                used.add(key)
                taken += 1
                if taken >= goal:
                    break
            offset += len(batch)
            if payload.get("endOfRecords") and taken < goal:
                raise RuntimeError(f"GBIF returned only {taken}/{goal} unique records for {lo}-{hi} {basis}")
        details.append({"period": f"{lo}-{hi}", "basis": basis, "requested": goal,
                        "obtained": taken, "duplicates_skipped": duplicates})
        print(f"GBIF {lo}-{hi} {basis}: {taken} records", flush=True)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(rows, indent=2, ensure_ascii=False), encoding="utf-8")
    manifest.write_text(json.dumps({"sampling_version": 2,
        "collected_at_utc": datetime.now(timezone.utc).isoformat(),
        "endpoint": ENDPOINT, "method": "fixed-quota six-stratum sample",
        "record_count": len(rows), "parameters_common": {"country": "KZ", "hasCoordinate": "true"},
        "strata": details, "note": "Quotas are design choices, not population proportions."},
        indent=2), encoding="utf-8")
    return rows


def select_fields(record):
    fields = ["key", "scientificName", "kingdom", "basisOfRecord", "eventDate", "year",
              "decimalLatitude", "decimalLongitude", "countryCode", "datasetKey"]
    return {field: record.get(field) for field in fields}
