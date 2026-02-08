import os
import requests
import zipfile
import geopandas as gpd
from shapely.geometry import Point

# --- Configuration ---
SHAPEFILE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip"
DATA_DIR = "data/shapefiles"
SHAPEFILE_PATH = os.path.join(DATA_DIR, "taxi_zones.shp")

# Latitude of 60th Street (Approximate cutoff for Congestion Zone)
# Any Manhattan zone with a centroid BELOW this latitude is "In the Zone"
LATITUDE_CUTOFF_60TH_ST = 40.764 

def download_and_extract_shapefile():
    """
    Downloads the official NYC Taxi Zones Shapefile and unzips it.
    """
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

    zip_path = os.path.join(DATA_DIR, "taxi_zones.zip")
    
    # 1. Download if missing
    if not os.path.exists(zip_path):
        print("⬇️  Downloading Taxi Zone Shapefile...")
        r = requests.get(SHAPEFILE_URL)
        with open(zip_path, 'wb') as f:
            f.write(r.content)
    
    # 2. Extract if .shp file is missing
    if not os.path.exists(SHAPEFILE_PATH):
        print("📂 Extracting Shapefile...")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(DATA_DIR)
            
    print("✅ Geospatial Data Ready.")

def get_congestion_zone_ids():
    """
    Dynamically identifies Zone IDs by calculating their geometric centroid.
    Logic: Borough == 'Manhattan' AND Centroid Latitude < 60th St.
    """
    # Ensure data exists
    download_and_extract_shapefile()
    
    # 1. Load Shapefile using Geopandas
    gdf = gpd.read_file(SHAPEFILE_PATH)
    
    # 2. Filter for Manhattan
    manhattan_zones = gdf[gdf['borough'] == 'Manhattan'].copy()
    
    # 3. Calculate Centroids (Center point of each zone)
    manhattan_zones['centroid'] = manhattan_zones.geometry.centroid.to_crs(epsg=4326)
    manhattan_zones['latitude'] = manhattan_zones['centroid'].y
    
    # 4. Apply the "South of 60th St" Cutoff
    congestion_zones = manhattan_zones[manhattan_zones['latitude'] < LATITUDE_CUTOFF_60TH_ST]
    
    # Get the list of IDs
    zone_ids = congestion_zones['LocationID'].tolist()
    
    print(f"🗺️  Identified {len(zone_ids)} zones inside the Congestion Zone (South of 60th St).")
    
    return zone_ids

def get_zone_lookup():
    
    download_and_extract_shapefile()
    gdf = gpd.read_file(SHAPEFILE_PATH)
    return gdf[['LocationID', 'zone', 'borough']]

def get_border_zone_ids():
    
    download_and_extract_shapefile()
    gdf = gpd.read_file(SHAPEFILE_PATH)
    
    # Filter for Manhattan
    manhattan = gdf[gdf['borough'] == 'Manhattan'].copy()
    
    # Calculate Centroids
    manhattan['centroid'] = manhattan.geometry.centroid.to_crs(epsg=4326)
    manhattan['latitude'] = manhattan['centroid'].y
    
    # 40.764 is 60th St.
    border_zones = manhattan[
        (manhattan['latitude'] >= 40.764) & 
        (manhattan['latitude'] < 40.790)
    ]
    
    ids = border_zones['LocationID'].tolist()
    print(f"🗺️  Identified {len(ids)} Border Zones (Buffer North of 60th St).")
    return ids

if __name__ == "__main__":
    ids = get_congestion_zone_ids()