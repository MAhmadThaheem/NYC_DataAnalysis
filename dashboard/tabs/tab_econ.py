import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import os

RESULTS_DIR = "data/results"

def run():
    st.markdown("## 💰 Economic Impact: The 'Crowding Out' Effect")
    st.info("**Hypothesis:** Do passengers tip *less* because they have to pay the *surcharge*?")

    csv_path = os.path.join(RESULTS_DIR, "tips_economics.csv")
    if not os.path.exists(csv_path):
        st.error("Data missing.")
        return

    df = pd.read_csv(csv_path)

    # Metrics
    avg_tip = df['avg_tip_pct'].mean() * 100
    avg_surcharge = df['avg_surcharge'].mean()
    corr = df['avg_surcharge'].corr(df['avg_tip_pct'])

    c1, c2, c3 = st.columns(3)
    c1.metric("Avg Tip Rate", f"{avg_tip:.1f}%")
    c2.metric("Avg Surcharge Paid", f"${avg_surcharge:.2f}")
    c3.metric("Correlation Score", f"{corr:.2f}", 
              help="-1.0 means strong negative impact (Tolls kill tips).")

    # Visuals
    fig = go.Figure()
    
    # Surcharge Bar (Red)
    fig.add_trace(go.Bar(
        x=df['month'], y=df['avg_surcharge'], 
        name='Surcharge ($)', marker_color='#EF553B', opacity=0.7
    ))

    # Tip Line (Blue)
    fig.add_trace(go.Scatter(
        x=df['month'], y=df['avg_tip_pct'], 
        name='Tip %', yaxis='y2', 
        line=dict(color='#636EFA', width=4), mode='lines+markers'
    ))

    fig.update_layout(
        title="Impact of Surcharge on Driver Tips",
        yaxis=dict(title="Surcharge ($)", showgrid=False),
        yaxis2=dict(title="Tip %", overlaying='y', side='right', tickformat=".0%", showgrid=False),
        legend=dict(x=0.01, y=0.99, bgcolor="rgba(255,255,255,0.8)"),
        template="plotly_white",
        hovermode="x unified"
    )
    st.plotly_chart(fig, use_container_width=True)

    # Conclusion
    st.divider()
    st.subheader("📝 Audit Conclusion")
    if corr < -0.3:
        st.error(f"**DRIVER HARM DETECTED (Correlation {corr:.2f}):** As surcharges rise, tips fall. Passengers are treating the total cost as a zero-sum game.")
        st.warning("💡 **Recommendation:** The TLC should increase the base fare to compensate drivers for lost tip revenue.")
    else:
        st.success(f"**NO NEGATIVE IMPACT (Correlation {corr:.2f}):** Tips have remained stable despite the surcharge.")