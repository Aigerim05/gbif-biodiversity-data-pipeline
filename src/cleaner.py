
import numpy as np
import pandas as pd

# Approximate coordinates of Almaty.
REF_LAT, REF_LON = 43.238949, 76.889709


def clean(valid):
    frame = valid.copy()
    before = len(frame)
    missing = {k: int(v) for k,v in frame.isna().sum().items()}
    frame = frame.drop_duplicates(subset=["key"]).copy()
    for field in ["scientificName", "kingdom", "basisOfRecord", "countryCode"]:
        frame[field] = frame[field].astype("string").str.strip()
    frame["kingdom"] = frame["kingdom"].fillna("Unknown").replace("", "Unknown")
    frame["scientificName"] = frame["scientificName"].str.replace(r"\s+", " ", regex=True)
    frame["basisOfRecord"] = frame["basisOfRecord"].str.upper()
    frame["eventDate"] = pd.to_datetime(
        frame["eventDate"],
        utc=True,
        errors="coerce",
        format="mixed"
    )
    frame["year"] = pd.to_numeric(frame["year"], errors="coerce")
    frame["year"] = frame["year"].fillna(frame["eventDate"].dt.year).astype("Int64")
    frame["key"] = frame["key"].astype("int64")
    frame["decimalLatitude"] = frame["decimalLatitude"].astype(float)
    frame["decimalLongitude"] = frame["decimalLongitude"].astype(float)
    # Haversine formula, km from Almaty to each coordinate. Descriptive, not ecological distance.
    lat1, lon1 = np.deg2rad([REF_LAT, REF_LON])
    lat2 = np.deg2rad(frame["decimalLatitude"].to_numpy())
    lon2 = np.deg2rad(frame["decimalLongitude"].to_numpy())
    a = np.sin((lat2-lat1)/2)**2 + np.cos(lat1)*np.cos(lat2)*np.sin((lon2-lon1)/2)**2
    frame["distance_to_almaty_km"] = 6371.0088 * 2 * np.arcsin(np.sqrt(np.clip(a, 0, 1)))
    frame["period"] = pd.cut(frame["year"], bins=[2015, 2018, 2021, 2024],
                             labels=["2016-2018", "2019-2021", "2022-2024"]).astype("string")
    # Multi-condition filter: useful observational subset without deleting valid stored records.
    subset = frame[(frame["basisOfRecord"] == "HUMAN_OBSERVATION") &
                   (frame["distance_to_almaty_km"] <= 500)]
    grouped = frame.groupby(["period", "basisOfRecord"], dropna=False).agg(
        records=("key", "count"), median_distance_km=("distance_to_almaty_km", "median")
    ).reset_index()
    pivot = pd.pivot_table(frame, index="period", columns="kingdom", values="key",
                           aggfunc="count", fill_value=0, observed=True)
    metrics = {"before_cleaning": before, "after_cleaning": len(frame),
               "duplicates_removed": before-len(frame), "missing_before": missing,
               "missing_after": {k: int(v) for k,v in frame.isna().sum().items()},
               "filtered_analysis_count": len(subset), "filtered_definition": "human observations within 500 km of Almaty",
               "group_count": len(grouped), "pivot_shape": list(pivot.shape),
               "median_distance_to_almaty_km": round(float(frame["distance_to_almaty_km"].median()), 1),
               "distance_reference": "Almaty 43.238949 N, 76.889709 E"}
    return frame, metrics, grouped, pivot
