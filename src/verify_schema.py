import polars as pl
import glob
import os

REQUIRED_SCHEMA = {
    "pickup_time", "dropoff_time", "pickup_loc", "dropoff_loc", 
    "trip_distance", "fare", "total_amount", "congestion_surcharge"
}

def verify():
    files = glob.glob("data/processed/*.parquet")
    if not files:
        print("❌ No processed files found!")
        return

    all_pass = True
    print(f"🔍 Verifying Schema for {len(files)} files...\n")

    for f in files:
        # scan_parquet is instant (reads metadata only)
        schema = set(pl.scan_parquet(f).columns)
        
        # Check if all required columns exist
        missing = REQUIRED_SCHEMA - schema
        
        if missing:
            print(f"❌ FAIL: {os.path.basename(f)}")
            print(f"   Missing columns: {missing}")
            all_pass = False
        else:
            print(f"✅ PASS: {os.path.basename(f)}")

    if all_pass:
        print("\n🎉 SUCCESS: All files have the correct schema!")
    else:
        print("\n⚠️  WARNING: Some files have incorrect schemas.")

if __name__ == "__main__":
    verify()