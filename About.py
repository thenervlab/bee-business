import streamlit as st

# ---- App Config ----
st.set_page_config(
    page_title="Bee Boxes",
    page_icon="🐝",
    layout="wide"
)

# ---- Landing Page ----
st.title("🐝 Welcome to the Bee Box!")
st.write("""

This app helps you record observations, explore data, and learn more about the project.
""")

st.markdown("---")
st.subheader("Navigation")
st.write("👉 Use the sidebar to access the dashboard, data entry form, resources, or contact page.")
