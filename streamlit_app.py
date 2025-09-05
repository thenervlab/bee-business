import streamlit as st

# ---- App Config ----
st.set_page_config(
    page_title="Nesting Box Citizen Science",
    page_icon="🪲",
    layout="wide"
)

# ---- Landing Page ----
st.title("🪲 Nesting Box Citizen Science Project")
st.write("""
Welcome to the citizen science project!  
This app helps you record observations, explore data, and learn more about the project.
""")

st.markdown("---")
st.subheader("Navigation")
st.write("👉 Use the sidebar to access the dashboard, data entry form, resources, or contact page.")
