import polars as pl
import glob
import os

PROCESSED_DIR = "data/processed"
AUDIT_DIR = "data/audit"
RESULTS_DIR = "data/results"

def generate_executive_summary():
    print("\n📄 GENERATING EXECUTIVE SUMMARY METRICS...\n")
    
    # 1. Total Estimated 2025 Surcharge Revenue
    # Scan all 2025 clean files
    try:
        total_revenue = (
            pl.scan_parquet(os.path.join(PROCESSED_DIR, "clean_*_2025*.parquet"))
            .select(pl.col("congestion_surcharge").sum())
            .collect()
            .item()
        )
        print(f"💰 Total Estimated 2025 Surcharge Revenue: ${total_revenue:,.2f}")
    except Exception as e:
        print(f"⚠️ Could not calculate revenue: {e}")

    # 2. Rain Elasticity Score
    try:
        elasticity_df = pl.read_csv(os.path.join(RESULTS_DIR, "weather_elasticity.csv"))
        # Recalculate correlation from the saved CSV
        score = elasticity_df.select(pl.corr("precipitation_mm", "trip_count")).item()
        elasticity_type = "Elastic" if abs(score) > 0.5 else "Inelastic"
        print(f"🌧️ Rain Elasticity Score: {score:.4f} ({elasticity_type})")
    except Exception:
        print("⚠️ Weather data not found.")

    # 3. Top Suspicious Vendors (Ghost Trips)
    # Note: If 'VendorID' wasn't in your processing schema, this might just count rows.
    try:
        audit_files = glob.glob(os.path.join(AUDIT_DIR, "*.csv"))
        if audit_files:
            ghost_df = pl.read_csv(audit_files)
            print(f"👻 Total Ghost Trips Detected: {len(ghost_df):,}")
            
            # If VendorID exists, print top 5. If not, print top Ghost Reasons.
            if "VendorID" in ghost_df.columns:
                print("   Top 5 Suspicious Vendors:")
                print(ghost_df["VendorID"].value_counts().head(5))
            else:
                print("   Top Fraud Categories:")
                print(ghost_df["ghost_reason"].value_counts().head(5))
        else:
            print("✅ No Ghost Trips found (Clean Audit).")
    except Exception as e:
        print(f"⚠️ Audit log error: {e}")

if __name__ == "__main__":
    generate_executive_summary()