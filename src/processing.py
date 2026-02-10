import os
import glob
import polars as pl

# --- Configuration ---
RAW_DIR = "data/raw"
PROCESSED_DIR = "data/processed"
AUDIT_DIR = "data/audit"

# Ensure directories exist
os.makedirs(PROCESSED_DIR, exist_ok=True)
os.makedirs(AUDIT_DIR, exist_ok=True)

def standardize_schema(df, taxi_type):
    """
    Renames columns to unified schema and ensures correct data types.
    """
    # 1. Define Mapping based on Taxi Type
    if taxi_type == "yellow":
        rename_map = {
            "tpep_pickup_datetime": "pickup_time",
            "tpep_dropoff_datetime": "dropoff_time",
            "PULocationID": "pickup_loc",
            "DOLocationID": "dropoff_loc",
            "trip_distance": "trip_distance",
            "fare_amount": "fare",
            "total_amount": "total_amount",
            "tip_amount": "tip_amount",
            "congestion_surcharge": "congestion_surcharge"
        }
    else: # Green Taxi
        rename_map = {
            "lpep_pickup_datetime": "pickup_time",
            "lpep_dropoff_datetime": "dropoff_time",
            "PULocationID": "pickup_loc",
            "DOLocationID": "dropoff_loc",
            "trip_distance": "trip_distance",
            "fare_amount": "fare",
            "total_amount": "total_amount",
            "tip_amount": "tip_amount",
            "congestion_surcharge": "congestion_surcharge"
        }

    # 2. Select & Rename
    # list comprehension to select only columns that exist in the file
    df = df.select([pl.col(k).alias(v) for k, v in rename_map.items() if k in df.columns])

    # 3. Force Types (Crucial for calculations to avoid errors)
    df = df.with_columns([
        pl.col("trip_distance").cast(pl.Float64),
        pl.col("fare").cast(pl.Float64),
        pl.col("total_amount").cast(pl.Float64),
        # Handle congestion_surcharge (fill nulls with 0 for safety)
        pl.col("tip_amount").cast(pl.Float64).fill_null(0.0),
        pl.col("congestion_surcharge").cast(pl.Float64).fill_null(0.0)
    ])
    
    return df

def apply_ghost_logic(df):
    """
    Tags rows with a 'ghost_reason' if they fail physics checks.
    """
    return df.with_columns([
        # 1. Calculate Derived Metrics (Duration in Min, Speed in MPH)
        ((pl.col("dropoff_time") - pl.col("pickup_time")).dt.total_seconds() / 60).alias("duration_min"),
        
        # Add small epsilon (0.0001) to avoid DivisionByZero errors
        (pl.col("trip_distance") / 
         ((pl.col("dropoff_time") - pl.col("pickup_time")).dt.total_seconds() / 3600 + 0.0001)
        ).alias("speed_mph")
    ]).with_columns(
        # 2. Create 'ghost_reason' column using Case/When logic
        pl.when(pl.col("speed_mph") > 65)
        .then(pl.lit("Impossible Physics (>65mph)"))
        
        .when((pl.col("duration_min") < 1) & (pl.col("fare") > 20))
        .then(pl.lit("Teleporter (<1min, >$20)"))
        
        .when((pl.col("trip_distance") == 0) & (pl.col("fare") > 0))
        .then(pl.lit("Stationary Ride (0mi, >$0)"))
        
        .when(pl.col("duration_min") < 0)
        .then(pl.lit("Time Travel (Negative Duration)")) 
        
        .otherwise(None) # Clean rows get Null
        .alias("ghost_reason")
    )

def process_data():
    """
    Main loop: Loads Raw data -> Unifies Schema -> Filters Ghosts -> Saves Clean (Parquet) & Dirty (CSV).
    """
    files = sorted(glob.glob(f"{RAW_DIR}/*.parquet"))
    print(f"🚀 Starting Processing Job for {len(files)} files...\n")

    for file_path in files:
        file_name = os.path.basename(file_path)
        taxi_type = "yellow" if "yellow" in file_name else "green"
        
        print(f"📄 Processing: {file_name}")

        try:
            # --- Step 1: Lazy Load & Unify ---
            q = pl.scan_parquet(file_path)
            q = standardize_schema(q, taxi_type)

            # --- Step 2: Logic ---
            df_batch = q.collect()
            
            # Tag the rows
            df_tagged = apply_ghost_logic(df_batch)

            # --- Step 3: The Split (Mutually Exclusive) ---
            # Ghost Rows: Where reason IS NOT null
            df_ghosts = df_tagged.filter(pl.col("ghost_reason").is_not_null())
            
            # Clean Rows: Where reason IS null
            df_clean = df_tagged.filter(pl.col("ghost_reason").is_null())

            # --- Step 4: Verification Stats ---
            total_rows = len(df_tagged)
            ghost_count = len(df_ghosts)
            clean_count = len(df_clean)

            print(f"   📊 Stats: Total={total_rows} | Clean={clean_count} | Ghosts={ghost_count}")
            
            if total_rows != (clean_count + ghost_count):
                print("   ⚠️  WARNING: Row count mismatch! Logic error possible.")

            # --- Step 5: Save Outputs ---
            
            # A. SAVE CLEAN DATA (Keep as Parquet for Pipeline Speed)
            clean_out = os.path.join(PROCESSED_DIR, f"clean_{file_name}")
            # Drop the temp columns to save disk space
            df_clean.drop(["duration_min", "speed_mph", "ghost_reason"]).write_parquet(clean_out)
            
            # B. SAVE AUDIT LOG (Save as CSV for Human Readability)
            if ghost_count > 0:
                # Change extension from .parquet to .csv
                audit_file_name = f"audit_{file_name}".replace(".parquet", ".csv")
                audit_out = os.path.join(AUDIT_DIR, audit_file_name)
                
                # Write to CSV
                df_ghosts.select([
                    "pickup_time", "trip_distance", "fare", "duration_min", "speed_mph", "ghost_reason"
                ]).write_csv(audit_out)
                
                print(f"   🗑️  Removed {ghost_count} ghost trips -> {audit_out} (Readable CSV)")
            else:
                print("   ✅  Clean file. No ghosts found.")

        except Exception as e:
            print(f"   ❌ Error processing {file_name}: {e}")
            # Optional: Print traceback for deeper debugging
            # import traceback
            # traceback.print_exc()

    print("\n✅ Processing Complete. Data is ready for Analysis.")

if __name__ == "__main__":
    process_data()