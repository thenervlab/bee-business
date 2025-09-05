import streamlit as st
import pandas as pd
import plotly.express as px

st.title("📊 Dashboard")

# Placeholder demo data
data = pd.DataFrame({
    "Species": ["Bee", "Wasp", "Ant", "Beetle"],
    "Observations": [15, 8, 22, 5]
})

st.subheader("Observations Overview")
fig = px.bar(data, x="Species", y="Observations", title="Species Observed")
st.plotly_chart(fig, use_container_width=True)

st.info("This dashboard will update as more citizen scientists enter data.")
