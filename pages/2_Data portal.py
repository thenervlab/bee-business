import streamlit as st
import pandas as pd
from datetime import date
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
import io

st.title("📝 Data Entry Portal")

st.write("Please record your nesting box observations below.")

# ---- Google Drive setup (for local dev) ----
# NOTE: You'll need credentials.json in your project root
@st.cache_resource
def init_drive():
    gauth = GoogleAuth()
    gauth.LocalWebserverAuth()  # opens browser for auth on first run
    return GoogleDrive(gauth)

drive = init_drive()

# ---- Form ----
with st.form("observation_form", clear_on_submit=True):
    observer = st.text_input("Your Name (or ID)")
    location = st.text_input("Location (nesting box ID or description)")
    species = st.selectbox("Species observed", ["Bee", "Wasp", "Ant", "Beetle", "Other"])
    count = st.number_input("Number observed", min_value=0, step=1)
    notes = st.text_area("Additional Notes")
    obs_date = st.date_input("Date", value=date.today())
    photo = st.file_uploader("Upload a photo", type=["jpg", "jpeg", "png"])

    submitted = st.form_submit_button("Submit Observation")

if submitted:
    # ---- Upload file to Google Drive ----
    if photo is not None:
        file_drive = drive.CreateFile({'title': f"{observer}_{obs_date}_{photo.name}"})
        file_drive.SetContentString(photo.getvalue().decode("latin1"))  # binary to str workaround
        file_drive.Upload()
        file_link = f"https://drive.google.com/uc?id={file_drive['id']}"
    else:
        file_link = None

    # ---- Save metadata locally for now ----
    st.success("✅ Thank you! Your observation has been recorded.")
    st.write({
        "observer": observer,
        "location": location,
        "species": species,
        "count": count,
        "notes": notes,
        "date": str(obs_date),
        "photo_link": file_link
    })
