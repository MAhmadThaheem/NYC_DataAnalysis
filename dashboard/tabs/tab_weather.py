import streamlit as st
import pandas as pd
import plotly.express as px
import os

RESULTS_DIR = "data/results"

def run():
    st.markdown("## 🌧️ The Rain Tax: Demand Elasticity")
    st.info("**Hypothesis:** Does rain force people into taxis (High Demand)? Or do they stay home (Low Demand)?")

    csv_path = os.path.join(RESULTS_DIR, "weather_elasticity.csv")
    if not os.path.exists(csv_path):
        st.error("Data missing.")
        return

    df = pd.read_csv(csv_path)

    # Metrics
    corr = df['precipitation_mm'].corr(df['trip_count'])
    wettest_day = df.loc[df['precipitation_mm'].idxmax()]
    
    c1, c2, c3 = st.columns(3)
    c1.metric("Rain Elasticity", f"{corr:.3f}", help="Positive = Rain increases demand")
    c2.metric("Wettest Day Rain", f"{wettest_day['precipitation_mm']:.1f} mm")
    c3.metric("Wettest Day Trips", f"{int(wettest_day['trip_count']):,}")

    if corr > 0:
        st.success("✅ **Status: INELASTIC (Essential Service)** - People take taxis *more* when it rains.")
    else:
        st.error("❌ **Status: ELASTIC (Optional Service)** - People stay home when it rains.")

    # Visuals with Marginal Histograms
    st.caption("Scatter plot showing relationship between Rain (X) and Trips (Y).")
    
    fig = px.scatter(
        df, x="precipitation_mm", y="trip_count",
        trendline="ols",
        labels={"precipitation_mm": "Rainfall (mm)", "trip_count": "Total Daily Trips"},
        title="Correlation: Rain vs. Taxi Demand",
        marginal_x="histogram", # Adds histogram on top
        marginal_y="histogram", # Adds histogram on right
        template="plotly_white",
        color_discrete_sequence=["#00CC96"]
    )
    st.plotly_chart(fig, use_container_width=True)

    # Conclusion
    st.divider()
    st.subheader("📝 Audit Conclusion")
    if corr > 0.1:
        st.info(f"**POSITIVE CORRELATION:** Demand INCREASES when it rains. Taxis are acting as 'Rain Shelters'.")
        st.success("💡 **Revenue Opportunity:** Implement a 'Bad Weather Surcharge' of +$2.50 during rain > 5mm.")
    else:
        st.warning(f"**NEUTRAL/NEGATIVE CORRELATION:** Demand drops or stays flat during rain.")
        st.info("💡 **Recommendation:** Do not increase prices during rain; it may drive passengers to the subway.")