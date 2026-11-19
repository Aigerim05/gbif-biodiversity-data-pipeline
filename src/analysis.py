
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def analyze(frame, figures="figures", missing_before=None):
    path = Path(figures); path.mkdir(parents=True, exist_ok=True)
    if frame.empty: raise ValueError("Cannot analyze an empty database")
    desc = frame[["year","decimal_latitude","decimal_longitude","distance_to_almaty_km"]].describe().round(2)
    desc.to_csv(path / "descriptive_statistics.csv")
    group = frame.groupby(["period", "basis_of_record"], dropna=False).agg(
        records=("key", "count"), median_distance_km=("distance_to_almaty_km", "median")).reset_index()
    
    group.to_csv(path / "grouped_statistics.csv", index=False)
    counts = frame["kingdom"].value_counts()
    counts.to_csv(path / "categorical_statistics.csv")
    pivot = pd.crosstab(frame["period"], frame["kingdom"])
    pivot.to_csv(path / "period_kingdom_pivot.csv")
    interpretations = {}

    # Figure 1: natural kingdom variation within fixed period quotas; row normalized.
    fractions = pivot.div(pivot.sum(axis=1).replace(0, np.nan), axis=0).fillna(0)
    ax = fractions.plot.bar(stacked=True, figsize=(9,4.6), width=.72)
    ax.set(title="Kingdom composition within each sampled period", xlabel="Recorded period",
           ylabel="Share of sampled records (0–1)")
    ax.legend(title="Kingdom", bbox_to_anchor=(1.02,1), loc="upper left", fontsize=8)
    ax.figure.tight_layout(); ax.figure.savefig(path / "01_period_kingdom.png", dpi=160); plt.close(ax.figure)
    largest = {str(p): str(fractions.loc[p].idxmax()) for p in fractions.index}
    interpretations["01_period_kingdom.png"] = ("Within-period kingdom shares (not population shares); "
          f"largest kingdom by period: {largest}. Period/basis quotas are fixed by design.")
    
    # Figure 2: geographic scatter by kingdom, color legend only for groups present.
    fig, ax = plt.subplots(figsize=(8.5, 5))
    top_names = list(counts.head(5).index)
    for name in top_names:
        group_df = frame[frame["kingdom"] == name]
        ax.scatter(group_df["decimal_longitude"], group_df["decimal_latitude"], s=18, alpha=.6,
                   label=f"{name} (n={len(group_df)})")
    ax.set(title="GBIF sample locations by kingdom", xlabel="Longitude (degrees)", ylabel="Latitude (degrees)")
    ax.legend(loc="best", fontsize=8); fig.tight_layout()
    fig.savefig(path / "02_kingdom_locations.png", dpi=160); plt.close(fig)
    interpretations["02_kingdom_locations.png"] = ("Spatial locations of sampled occurrences. "
          "Overlapping points can be hidden; clusters may reflect observer activity, not organism abundance.")
    
    # Figure 3: distance distribution by observation basis, true useful numpy Haversine result.
    fig, ax = plt.subplots(figsize=(8.4,4.5))
    categories = frame["basis_of_record"].value_counts().index.tolist()
    values = [frame.loc[frame["basis_of_record"] == x,"distance_to_almaty_km"].dropna().to_numpy()
              for x in categories]
    ax.boxplot(values, showfliers=False)
    ax.set_xticks(range(1, len(categories) + 1), categories)
    ax.set(title="Distance of sampled records from Almaty by record basis", ylabel="Haversine distance (km)")
    fig.tight_layout(); fig.savefig(path / "03_basis_distances.png", dpi=160); plt.close(fig)
    median_by_basis = {name: round(float(np.median(vals)), 1) for name,vals in zip(categories,values)}
    interpretations["03_basis_distances.png"] = (f"Median distances in km by record basis: {median_by_basis}. "
         "Distance describes reporting locations only; sampling quotas do not represent geographic prevalence.")
    
    return {"database_records_analyzed": len(frame),
            "basis_counts": {str(k):int(v) for k,v in frame["basis_of_record"].value_counts().items()},
            "kingdom_counts": {str(k):int(v) for k,v in counts.items()},
            "period_counts": {str(k):int(v) for k,v in frame["period"].value_counts().items()},
            "median_distance_by_basis_km": median_by_basis,
            "numerical_describe": desc.to_dict(), "figure_interpretations": interpretations}
