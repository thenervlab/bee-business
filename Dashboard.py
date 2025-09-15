import streamlit as st
import streamlit as st
import streamlit as st
import pandas as pd
import plotly.express as px

# ---- App Config ----
st.set_page_config(
    page_title="Bee Boxes",
    page_icon="🐝",
    layout="wide"
)

# ---- Landing Page ----
st.title("🐝 Welcome to the Bee Box!")
st.write("""

This app helps our citizen scientists record observations, explore the data collected so far, and learn more about the project. Find out more by visiting the data entry portal, resources, and contact pages!
""")
st.markdown("---")

# ---- Dashboard ----

st.subheader("📊 DataViz Dashboard")

st.write("""
Keen to get the buzz on bee business? Check out a summary of our observations below! 
""")

# Placeholder demo data
data = pd.DataFrame({
    "Species": ["Bee", "Wasp", "Ant", "Beetle"],
    "Observations": [15, 8, 22, 5]
})

fig = px.bar(data, x="Species", y="Observations", title="Species Observed")
st.plotly_chart(fig, use_container_width=True)

st.info("This dashboard will update as more citizen scientists enter data. Check back soon!")

st.markdown("---")

# ---- Footer ----
st.write("❔Questions? Visit the Contact page in the sidebar to get in touch!")
