import streamlit as st
import pandas as pd
import plotly.express as px
import os

RESULTS_DIR = "data/results"

def run():
    st.markdown("## 🚦 Congestion Velocity Audit")
    st.info("**Hypothesis:** Did the toll actually speed up traffic inside Manhattan?")

    path_24 = os.path.join(RESULTS_DIR, "velocity_heatmap_2024.csv")
    path_25 = os.path.join(RESULTS_DIR, "velocity_heatmap_2025.csv")

    if not os.path.exists(path_24) or not os.path.exists(path_25):
        st.error("⚠️ Missing analysis data. Please run 'pipeline.py' first.")
        return

    df_24 = pd.read_csv(path_24)
    df_25 = pd.read_csv(path_25)

    # Executive Metrics
    avg_speed_24 = df_24['avg_speed'].mean()
    avg_speed_25 = df_25['avg_speed'].mean()
    
    if avg_speed_24 > 0:
        delta = ((avg_speed_25 - avg_speed_24) / avg_speed_24) * 100
    else:
        delta = 0.0

    c1, c2, c3 = st.columns(3)
    c1.metric("Avg Speed (2024)", f"{avg_speed_24:.2f} mph")
    c2.metric("Avg Speed (2025)", f"{avg_speed_25:.2f} mph")
    c3.metric("Net Improvement", f"{delta:.1f}%", delta_color="normal")

    st.divider()

    # Heatmaps
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("2024 (Before Toll)")
        fig24 = px.density_heatmap(
            df_24, x="hour_of_day", y="day_of_week", z="avg_speed",
            nbinsx=24, nbinsy=7, color_continuous_scale="RdYlGn",
            labels={'hour_of_day': 'Hour', 'day_of_week': 'Day', 'avg_speed': 'Speed'},
            range_color=[5, 20], template="plotly_white"
        )
        st.plotly_chart(fig24, use_container_width=True)

    with col2:
        st.subheader("2025 (After Toll)")
        fig25 = px.density_heatmap(
            df_25, x="hour_of_day", y="day_of_week", z="avg_speed",
            nbinsx=24, nbinsy=7, color_continuous_scale="RdYlGn",
            labels={'hour_of_day': 'Hour', 'day_of_week': 'Day', 'avg_speed': 'Speed'},
            range_color=[5, 20], template="plotly_white"
        )
        st.plotly_chart(fig25, use_container_width=True)

    # Conclusion
    st.divider()
    st.subheader("📝 Audit Conclusion")
    
    if delta > 5:
        st.success(f"**SUCCESS:** Traffic speed improved by **{delta:.1f}%**. The toll is effectively reducing congestion.")
        st.info("💡 **Recommendation:** Maintain current pricing. The system is working as intended.")
    elif delta < -5:
        st.error(f"**CRITICAL FAILURE:** Traffic slowed down by **{abs(delta):.1f}%**. The toll might be causing localized bottlenecks.")
        st.warning("💡 **Recommendation:** Review signal timing and border zone traffic patterns.")
    else:
        st.warning(f"**STAGNATION:** Traffic speed only changed by **{delta:.1f}%**, which is negligible.")
        st.error("💡 **Recommendation:** The current toll ($15) is too low to deter drivers. Recommend increasing peak-hour tolls by 20%.")