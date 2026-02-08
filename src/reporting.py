import polars as pl
import glob
import os

PROCESSED_DIR = "data/processed"
AUDIT_DIR = "data/audit"
RESULTS_DIR = "data/results"

def generate_executive_summary():
    print("\n--- [FINAL] Executive Summary ---\n")
    print("📄 GENERATING EXECUTIVE SUMMARY METRICS...\n")
    
    # 1. Total Estimated 2025 Surcharge Revenue
    try:
        # Use a wildcard pattern for robustness
        revenue_files = os.path.join(PROCESSED_DIR, "clean_*_2025*.parquet")
        
        # Check if files exist before scanning to avoid crashing
        if glob.glob(revenue_files):
            total_revenue = (
                pl.scan_parquet(revenue_files)
                .select(pl.col("congestion_surcharge").sum())
                .collect()
                .item()
            )
            print(f"💰 Total Estimated 2025 Surcharge Revenue: ${total_revenue:,.2f}")
        else:
            print("💰 Revenue: $0.00 (No 2025 data found)")
            
    except Exception as e:
        print(f"⚠️ Could not calculate revenue: {e}")

    # 2. Rain Elasticity Score
    try:
        elasticity_path = os.path.join(RESULTS_DIR, "weather_elasticity.csv")
        if os.path.exists(elasticity_path):
            elasticity_df = pl.read_csv(elasticity_path)
            # Recalculate correlation
            score = elasticity_df.select(pl.corr("precipitation_mm", "trip_count")).item()
            elasticity_type = "Elastic" if abs(score) > 0.5 else "Inelastic"
            print(f"🌧️ Rain Elasticity Score: {score:.4f} ({elasticity_type})")
        else:
            print("🌧️ Rain Elasticity: N/A (Run pipeline to generate)")
    except Exception:
        print("⚠️ Weather data error.")

    # 3. Top Suspicious Vendors (Ghost Trips) - FIXED SECTION
    try:
        # FIX: Use the string pattern directly, not a list
        audit_pattern = os.path.join(AUDIT_DIR, "*.csv")
        
        if glob.glob(audit_pattern):
            # FIX: Use scan_csv() which handles wildcards better than read_csv()
            ghost_df = pl.scan_csv(audit_pattern).collect()
            
            print(f"👻 Total Ghost Trips Detected: {len(ghost_df):,}")
            
            if "VendorID" in ghost_df.columns:
                print("   Top 5 Suspicious Vendors:")
                # Count and Sort strictly
                top_vendors = (
                    ghost_df["VendorID"]
                    .value_counts(sort=True) # Ensure we get the highest counts
                    .head(5)
                )
                print(top_vendors)
            else:
                print("   Top Fraud Categories:")
                print(ghost_df["ghost_reason"].value_counts(sort=True).head(5))
        else:
            print("✅ No Ghost Trips found (Clean Audit).")
            
    except Exception as e:
        print(f"⚠️ Audit log error: {e}")

if __name__ == "__main__":
    generate_executive_summary()