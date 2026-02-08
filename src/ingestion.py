import os
import requests
import pandas as pd
from datetime import datetime, timedelta
import glob
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

# --- Configuration ---
RAW_DIR = "data/raw"
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

def download_file(url, dest_path):
    if os.path.exists(dest_path):
        print(f"   ✅ Already exists: {dest_path}")
        return
    
    print(f"   ⬇️  Downloading {os.path.basename(dest_path)}...")
    try:
        r = requests.get(url, stream=True)
        r.raise_for_status()
        with open(dest_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
        print("      ✅ Done.")
    except Exception as e:
        print(f"      ❌ Failed: {e}")

def generate_imputed_december_2025():
    """
    Imputes missing Dec 2025 data using Weighted Average of Dec 2023 (30%) and Dec 2024 (70%).
    Requirement: Missing Data Handling
    """
    print("\n🔮 Generating Imputed Data for December 2025...")
    
    file_2023 = os.path.join(RAW_DIR, "yellow_tripdata_2023-12.parquet")
    file_2024 = os.path.join(RAW_DIR, "yellow_tripdata_2024-12.parquet")
    target_file = os.path.join(RAW_DIR, "yellow_tripdata_2025-12.parquet")

    if os.path.exists(target_file):
        print("   ✅ Imputed file already exists.")
        return

    # Download Dec 2023 and Dec 2024 if missing
    for year in [2023, 2024]:
        path = os.path.join(RAW_DIR, f"yellow_tripdata_{year}-12.parquet")
        url = f"{BASE_URL}/yellow_tripdata_{year}-12.parquet"
        download_file(url, path)

    try:
        # Load data (using Pandas here for complex sampling logic)
        df_23 = pd.read_parquet(file_2023)
        df_24 = pd.read_parquet(file_2024)

        # Basic Imputation Logic: Sample rows based on weights
        target_size = len(df_24)
        
        # Take 30% from 2023, 70% from 2024
        sample_23 = df_23.sample(n=int(target_size * 0.3), replace=True)
        sample_24 = df_24.sample(n=int(target_size * 0.7), replace=True)
        
        # Combine
        imputed_df = pd.concat([sample_23, sample_24])
        
        # Adjust Dates to Dec 2025
        if 'tpep_pickup_datetime' in imputed_df.columns:
            imputed_df['tpep_pickup_datetime'] = imputed_df['tpep_pickup_datetime'].apply(lambda x: x.replace(year=2025))
        if 'tpep_dropoff_datetime' in imputed_df.columns:
            imputed_df['tpep_dropoff_datetime'] = imputed_df['tpep_dropoff_datetime'].apply(lambda x: x.replace(year=2025))

        # Save
        table = pa.Table.from_pandas(imputed_df)
        pq.write_table(table, target_file)
        print("   ✅ Successfully created yellow_tripdata_2025-12.parquet (Imputed)")
        
    except Exception as e:
        print(f"   ⚠️ Imputation failed: {e}")

def run_ingestion():
    """
    Main ingestion script. 
    Downloads 2025 data, Imputes Dec 2025, and downloads Full 2024 baseline.
    """
    if not os.path.exists(RAW_DIR):
        os.makedirs(RAW_DIR)
        
    print(f"📥 Starting Data Ingestion to {RAW_DIR}...")

    # 1. Download 2025 Data (Jan-Nov) - Real Data
    print("   --- 2025 Data (Primary) ---")
    for month in range(1, 12): # Jan to Nov
        month_str = f"{month:02d}"
        for taxi_type in ['yellow', 'green']:
            fname = f"{taxi_type}_tripdata_2025-{month_str}.parquet"
            url = f"{BASE_URL}/{fname}"
            download_file(url, os.path.join(RAW_DIR, fname))

    # 2. Impute Dec 2025
    generate_imputed_december_2025()

    # 3. Download Full 2024 Data (Baseline)
    # Essential for fair "Border Effect" comparison (Full Year vs Full Year)
    print("   --- 2024 Data (Baseline) ---")
    for month in range(1, 13): # Jan to Dec (Full Year)
        month_str = f"{month:02d}"
        for taxi_type in ['yellow', 'green']:
            fname = f"{taxi_type}_tripdata_2024-{month_str}.parquet"
            url = f"{BASE_URL}/{fname}"
            download_file(url, os.path.join(RAW_DIR, fname))

    print("✅ Ingestion Complete.")

if __name__ == "__main__":
    run_ingestion()