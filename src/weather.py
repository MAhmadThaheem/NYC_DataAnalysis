import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
import polars as pl
import os
import glob
from datetime import datetime

# --- Configuration ---
PROCESSED_DIR = "data/processed"
RESULTS_DIR = "data/results"
os.makedirs(RESULTS_DIR, exist_ok=True)

# Setup Open-Meteo API Client with Caching
cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

def fetch_weather_data():
    """
    Fetches daily precipitation (rain_sum) for Central Park, NY for 2025.
    Coordinates: 40.7831° N, 73.9712° W
    """
    print("\n🌦️  Fetching Weather Data (Open-Meteo API)...")
    
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": 40.7831,
        "longitude": -73.9712,
        "start_date": "2025-01-01",
        "end_date": "2025-12-31",
        "daily": "precipitation_sum",
        "timezone": "America/New_York"
    }
    
    responses = openmeteo.weather_api(url, params=params)
    response = responses[0]
    
    # Process daily data
    daily = response.Daily()
    daily_precipitation_sum = daily.Variables(0).ValuesAsNumpy()
    
    date_range = pd.date_range(
        start = pd.to_datetime(daily.Time(), unit = "s", utc = True),
        end = pd.to_datetime(daily.TimeEnd(), unit = "s", utc = True),
        freq = pd.Timedelta(seconds = daily.Interval()),
        inclusive = "left"
    )
    
    # Create Polars DataFrame
    weather_df = pl.DataFrame({
        "date": pl.Series(date_range).cast(pl.Date),
        "precipitation_mm": pl.Series(daily_precipitation_sum)
    })
    
    print(f"   ✅ Fetched {len(weather_df)} days of weather data.")
    return weather_df

def calculate_rain_elasticity():
    """
    Joins Weather Data with Daily Trip Counts to find correlation.
    """
    print("\n📈 Calculating Rain Elasticity...")
    
    # 1. Get Weather
    weather_df = fetch_weather_data()
    
    # 2. Get Daily Trip Counts (Aggregating all processed files)
    q = pl.scan_parquet(os.path.join(PROCESSED_DIR, "clean_*_2025*.parquet"))
    
    daily_trips = (
        q.with_columns(pl.col("pickup_time").dt.date().alias("date"))
        .group_by("date")
        .agg(pl.len().alias("trip_count"))
        .collect()
    )
    
    # 3. Join
    # weather_df is small (365 rows), daily_trips is small (365 rows)
    merged = weather_df.join(daily_trips, on="date", how="inner")
    
    # 4. Calculate Correlation (Elasticity Proxy)
    # Pearson Correlation between Rain (mm) and Trip Count
    correlation = merged.select(pl.corr("precipitation_mm", "trip_count")).item()
    
    print(f"   📊 Rain Elasticity Score (Correlation): {correlation:.4f}")
    if correlation > 0:
        print("      -> Positive Correlation: Rain INCREASES demand (People take taxis to stay dry).")
    else:
        print("      -> Negative Correlation: Rain DECREASES demand (People stay home).")
        
    # 5. Identify "Wettest Month" for Visualization
    # Add Month column
    merged = merged.with_columns(pl.col("date").dt.month().alias("month"))
    
    # Find month with highest total rain
    wettest_month = (
        merged.group_by("month")
        .agg(pl.col("precipitation_mm").sum().alias("total_rain"))
        .sort("total_rain", descending=True)
        .head(1)
        .select("month")
        .item()
    )
    
    print(f"   🌧️  Wettest Month Identified: {wettest_month}")
    
    # Save Full Dataset for Dashboard (Scatter Plot)
    merged.write_csv(os.path.join(RESULTS_DIR, "weather_elasticity.csv"))
    
    # Save Wettest Month Data specifically for the specific plot requirement
    wettest_data = merged.filter(pl.col("month") == wettest_month)
    wettest_data.write_csv(os.path.join(RESULTS_DIR, "wettest_month_data.csv"))
    print("   ✅ Data saved to data/results/weather_elasticity.csv")

if __name__ == "__main__":
    calculate_rain_elasticity()