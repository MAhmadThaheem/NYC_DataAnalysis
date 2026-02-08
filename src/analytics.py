import polars as pl
import os
import glob
from datetime import datetime
from src.geospatial import get_congestion_zone_ids, get_zone_lookup, get_border_zone_ids

# --- Configuration ---
PROCESSED_DIR = "data/processed"
RESULTS_DIR = "data/results"
os.makedirs(RESULTS_DIR, exist_ok=True)

# --- Load Data Safely ---
def load_data(pattern, year=None):
    """
    Lazy loads parquet files. Falls back to RAW data if PROCESSED is missing for 2024.
    """
    files = glob.glob(pattern)
    if not files:
        # Fallback for 2024 Raw Data if processed doesn't exist yet
        if year == 2024:
            raw_pattern = pattern.replace(PROCESSED_DIR, "data/raw").replace("clean_", "")
            if glob.glob(raw_pattern):
                return pl.scan_parquet(raw_pattern)
        return None
    return pl.scan_parquet(pattern)

def analyze_leakage():
    """
    Requirement 2: Leakage Audit.
    1. Surcharge Compliance Rate.
    2. Top 3 Locations with missing surcharges.
    """
    print("\n🔍 Starting Leakage Audit...")
    
    try:
        zone_ids = get_congestion_zone_ids()
    except Exception as e:
        print(f"   ❌ Geospatial Error: {e}")
        return

    # Scan 2025 Data
    file_pattern = os.path.join(PROCESSED_DIR, "clean_*_2025*.parquet")
    if not glob.glob(file_pattern):
        print(f"   ❌ No processed 2025 files found at {file_pattern}")
        return

    q = pl.scan_parquet(file_pattern)

    # Filter: Trips STARTING Outside and ENDING Inside Zone
    leakage_filter = (
        (pl.col("pickup_time") >= datetime(2025, 1, 5)) & 
        (~pl.col("pickup_loc").is_in(zone_ids)) &
        (pl.col("dropoff_loc").is_in(zone_ids))
    )
    
    df_audit = q.filter(leakage_filter).with_columns(
        (pl.col("congestion_surcharge") > 0).alias("is_compliant")
    )
    
    # Metric A: Compliance Rate
    stats = df_audit.select([
        pl.len().alias("total_trips"),
        pl.col("is_compliant").sum().alias("compliant_trips")
    ]).collect()
    
    if stats["total_trips"][0] > 0:
        rate = (stats["compliant_trips"][0] / stats["total_trips"][0]) * 100
        print(f"   📊 Surcharge Compliance Rate: {rate:.2f}%")
    
    # Metric B: Top 3 Leaky Locations
    leakage_by_loc = (
        df_audit
        .group_by("pickup_loc")
        .agg([
            pl.len().alias("volume"),
            (1 - pl.col("is_compliant").mean()).alias("leakage_rate")
        ])
        .filter(pl.col("volume") > 50)
        .sort("leakage_rate", descending=True)
        .limit(3)
    ).collect()
    
    # Join with Zone Names
    lookup = pl.from_pandas(get_zone_lookup()).with_columns(pl.col("LocationID").cast(pl.Int64))
    top_leakers = leakage_by_loc.join(lookup, left_on="pickup_loc", right_on="LocationID")
    
    print("   ⚠️  Top 3 Locations with Missing Surcharges:")
    print(top_leakers.select(["zone", "leakage_rate", "volume"]))
    
    top_leakers.write_csv(f"{RESULTS_DIR}/leakage_audit.csv")

def compare_volumes():
    """
    Yellow vs Green Decline (Q1 2024 vs Q1 2025).
    """
    print("\n📉 Starting Volume Decline Analysis (Yellow vs Green)...")
    zone_ids = get_congestion_zone_ids()
    results = []

    for taxi_type in ["yellow", "green"]:
        for year in [2024, 2025]:
            # Pattern for Q1 (Jan, Feb, Mar) only
            pattern = os.path.join(PROCESSED_DIR, f"clean_{taxi_type}_*{year}-0[1-3]*.parquet")
            q = load_data(pattern, year)
            
            if q is None: continue

            # Handle 2024 Raw Schema if fallback used
            if "clean" not in str(q) and "DOLocationID" in q.columns:
                 q = q.rename({"DOLocationID": "dropoff_loc"})

            volume = (
                q.filter(pl.col("dropoff_loc").is_in(zone_ids))
                .select(pl.len())
                .collect()
                .item()
            )
            results.append({"Taxi Type": taxi_type, "Year": year, "Volume": volume})
            print(f"   🗓️  {taxi_type} Q1 {year}: {volume:,} trips")

    # Save Results
    pl.DataFrame(results).write_csv(f"{RESULTS_DIR}/volume_comparison.csv")

def analyze_border_effect():
    """
    Visual Audit 1: The Border Effect.
    Comparing Drop-off counts in "Border Zones" (Full Year 2024 vs Full Year 2025).
    """
    print("\n🗺️  Starting Border Effect Analysis (Full Year Comparison)...")
    border_ids = get_border_zone_ids()
    results = []

    for year in [2024, 2025]:
        pattern = os.path.join(PROCESSED_DIR, f"clean_yellow_tripdata_{year}-*.parquet")
        q = load_data(pattern, year)
        
        if q is None:
            print(f"   ⚠️ No data for {year}. Using dummy 0.")
            agg = pl.DataFrame({"dropoff_loc": border_ids, f"trips_{year}": [0]*len(border_ids)})
        else:
            if "clean" not in str(q) and "DOLocationID" in q.columns:
                 q = q.rename({"DOLocationID": "dropoff_loc"})

            agg = (
                q.filter(pl.col("dropoff_loc").is_in(border_ids))
                .group_by("dropoff_loc")
                .agg(pl.len().alias(f"trips_{year}"))
                .collect()
            )
        results.append(agg)

    if len(results) == 2:
        df_24, df_25 = results
        joined = df_24.join(df_25, on="dropoff_loc", how="outer").fill_null(0)
        
        # Calculate % Change with safe division
        final_df = joined.with_columns(
            pl.when(pl.col("trips_2024") > 50)
            .then(((pl.col("trips_2025") - pl.col("trips_2024")) / pl.col("trips_2024")) * 100)
            .otherwise(0.0)
            .alias("pct_change")
        )

        lookup = pl.from_pandas(get_zone_lookup()).with_columns(pl.col("LocationID").cast(pl.Int64))
        final_df = final_df.join(lookup, left_on="dropoff_loc", right_on="LocationID")
        
        print("   ✅ Calculated Border Effect.")
        final_df.write_csv(f"{RESULTS_DIR}/border_effect.csv")

def analyze_velocity_heatmap():
    """
    Visual Audit 2: Congestion Velocity Heatmap (Q1 24 vs Q1 25).
    """
    print("\n🚦 Starting Congestion Velocity Analysis...")
    zone_ids = get_congestion_zone_ids()
    
    for year in [2024, 2025]:
        pattern = os.path.join(PROCESSED_DIR, f"clean_yellow_tripdata_{year}-0[1-3]*.parquet")
        q = load_data(pattern, year)
        if q is None: continue

        if "clean" not in str(q):
            # Raw schema mapping
            q = q.rename({
                "tpep_pickup_datetime": "pickup_time", "tpep_dropoff_datetime": "dropoff_time",
                "PULocationID": "pickup_loc", "DOLocationID": "dropoff_loc"
            })

        # Filter: Internal Zone Trips Only
        internal = q.filter(
            (pl.col("pickup_loc").is_in(zone_ids)) & (pl.col("dropoff_loc").is_in(zone_ids))
        )

        # Recalculate Speed
        speed_df = internal.with_columns(
            ((pl.col("dropoff_time") - pl.col("pickup_time")).dt.total_seconds() / 3600).alias("duration_hours")
        ).filter(pl.col("duration_hours") > 0.05).with_columns(
            (pl.col("trip_distance") / pl.col("duration_hours")).alias("speed_mph")
        )

        heatmap = (
            speed_df.with_columns([
                pl.col("pickup_time").dt.weekday().alias("day_of_week"),
                pl.col("pickup_time").dt.hour().alias("hour_of_day")
            ])
            .group_by(["day_of_week", "hour_of_day"])
            .agg(pl.col("speed_mph").mean().alias("avg_speed"))
            .sort(["day_of_week", "hour_of_day"])
            .collect()
        )
        heatmap.write_csv(f"{RESULTS_DIR}/velocity_heatmap_{year}.csv")
        print(f"   ✅ Generated Heatmap for {year}")

def analyze_tips_economics():
    """
    Visual Audit 3: Tip Crowding Out.
    """
    print("\n💰 Starting Tip Economics Analysis...")
    q = pl.scan_parquet(os.path.join(PROCESSED_DIR, "clean_yellow_tripdata_2025*.parquet"))
    
    try:
        monthly_stats = (
            q.filter(pl.col("fare") > 2.5)
            .with_columns([
                (pl.col("tip_amount") / pl.col("fare")).alias("tip_percent"),
                pl.col("pickup_time").dt.month().alias("month")
            ])
            .group_by("month")
            .agg([
                pl.col("congestion_surcharge").mean().alias("avg_surcharge"),
                pl.col("tip_percent").mean().alias("avg_tip_pct")
            ])
            .sort("month")
            .collect()
        )
        monthly_stats.write_csv(f"{RESULTS_DIR}/tips_economics.csv")
        print("   ✅ Calculated Tip Economics")
    except Exception as e:
        print(f"   ⚠️ Tip Analysis skipped: {e}")

if __name__ == "__main__":
    analyze_leakage()
    compare_volumes() 
    analyze_border_effect()
    analyze_velocity_heatmap()
    analyze_tips_economics()