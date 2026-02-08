import os
import time
import subprocess
import sys
from src import ingestion, processing, geospatial, analytics, weather
from src import reporting 

def run_pipeline():
    """
    Master Orchestration Script for 2025 NYC Congestion Pricing Audit.
    """
    start_time = time.time()
    print("🚀 STARTING PIPELINE: 2025 NYC Congestion Pricing Audit\n")

    # --- PHASE 1: Data Engineering ---
    print("--- [PHASE 1] Data Ingestion & Engineering ---")
    
    # 1. Download Data & Impute Missing Dec 2025
    ingestion.run_ingestion()
    
    # 2. Process Data (Schema Unification & Ghost Trip Filtering)
    processing.process_data() 
    
    # 3. Download Geospatial Data
    geospatial.download_and_extract_shapefile()

    # --- PHASE 2: Congestion Zone Impact ---
    print("\n--- [PHASE 2] Analytics: Congestion Zone Impact ---")
    analytics.analyze_leakage()       # Req 2: Leakage Audit
    analytics.compare_volumes()       # Req 3: Yellow vs Green

    # --- PHASE 3: Visual Audit Aggregations ---
    print("\n--- [PHASE 3] Analytics: Visual Audit Prep ---")
    analytics.analyze_border_effect()     # Map Data
    analytics.analyze_velocity_heatmap()  # Flow Data
    analytics.analyze_tips_economics()    # Economics Data

    # --- PHASE 4: The Rain Tax ---
    print("\n--- [PHASE 4] Analytics: Weather Elasticity ---")
    weather.calculate_rain_elasticity()
    
    # --- REPORTING ---
    print("\n--- [FINAL] Executive Summary ---")
    reporting.generate_executive_summary()

    elapsed = (time.time() - start_time) / 60
    print(f"\n✅ PIPELINE COMPLETE in {elapsed:.2f} minutes.")
    
    # --- AUTO-LAUNCH DASHBOARD ---
    print("\n-------------------------------------------------")
    choice = input("👉 Do you want to launch the Interactive Dashboard now? (y/n): ").strip().lower()
    
    if choice == 'y':
        print("🚀 Launching Streamlit... (Press Ctrl+C to stop)")
        # This command runs "streamlit run dashboard/app.py" in the terminal
        try:
            subprocess.run([sys.executable, "-m", "streamlit", "run", "dashboard/app.py"])
        except KeyboardInterrupt:
            print("\n🛑 Dashboard stopped.")
    else:
        print("👋 Exiting. You can run the dashboard manually using:")
        print("   streamlit run dashboard/app.py")

if __name__ == "__main__":
    run_pipeline()