import streamlit as st
import pandas as pd
import os
import sys

# Add project root to path so we can import src if needed
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import Tabs
from tabs import tab_map, tab_flow, tab_econ, tab_weather

# Page Config
st.set_page_config(page_title="NYC Congestion Audit 2025", layout="wide")

# Header
st.title("🚖 2025 NYC Congestion Pricing Audit")
st.markdown("**Lead Data Scientist Report** | *Generated for Client: NYC TLC Consultancy*")
st.markdown("---")

# Sidebar
st.sidebar.header("Audit Controls")
st.sidebar.info("Data Source: NYC TLC (Parquet)")
st.sidebar.markdown("---")
st.sidebar.write("Developed by M Ahmad")

# Tabs
tab1, tab2, tab3, tab4 = st.tabs([
    "🗺️ The Border Effect", 
    "🚦 Congestion Velocity", 
    "💰 Economics (Tips)", 
    "🌧️ The Rain Tax"
])

with tab1:
    tab_map.run()

with tab2:
    tab_flow.run()

with tab3:
    tab_econ.run()

with tab4:
    tab_weather.run()