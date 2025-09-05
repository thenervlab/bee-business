import streamlit as st
import pandas as pd
from datetime import date

st.title("📝 Data Entry Portal")

st.write("Please record your nesting box observations below.")

# ---- Form ----
with st.form("observation_form", clear_on_submit=True):
    observer = st.text_input("Your Name (or ID)")
    location = st.text_input("Location (nesting box ID or description)")
    species = st.selectbox("Species observed", ["Bee", "Wasp", "Ant", "Beetle", "Other"])
    count = st.number_input("Number observed", min_value=0, step=1)
    notes = st.text_area("Additional Notes")
    obs_date = st.date_input("Date", value=date.today())

    submitted = st.form_submit_button("Submit Observation")

if submitted:
    st.success("✅ Thank you! Your observation has been recorded.")
    # Later: save to database or CSV
