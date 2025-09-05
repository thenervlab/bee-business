import streamlit as st
import pandas as pd
from datetime import date
import dropbox
import json
import os
import uuid



# ---- Load Dropbox access token from secrets.json ----
with open("secrets.json") as f:
    secrets = json.load(f)

DROPBOX_ACCESS_TOKEN = secrets["DROPBOX_ACCESS_TOKEN"]
dbx = dropbox.Dropbox(DROPBOX_ACCESS_TOKEN)

st.title("📝 Data Entry Portal")

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
    obs_id = str(uuid.uuid4())  # generate unique ID for this observation
    file_link = None

    if photo:
        photo_path = f"/{observer}_{obs_date}_{obs_id}_{photo.name}"
        dbx.files_upload(bytes(photo.getbuffer()), photo_path, mode=dropbox.files.WriteMode.overwrite)
        # create shared link
        file_link = dbx.sharing_create_shared_link_with_settings(photo_path).url.replace("?dl=0", "?raw=1")


    obs_data = {
        "obs_id": obs_id,
        "observer": observer,
        "location": location,
        "species": species,
        "count": count,
        "notes": notes,
        "date": str(obs_date),
        "photo_link": file_link
    }

    obs_df = pd.DataFrame([obs_data])
    csv_local = f"{obs_id}.csv"
    obs_df.to_csv(csv_local, index=False)

    # upload CSV to Dropbox
    with open(csv_local, "rb") as f:
        dbx.files_upload(f.read(), f"/{csv_local}", mode=dropbox.files.WriteMode.overwrite)
    os.remove(csv_local)


    st.success("✅ Thank you! Your observation has been recorded.")
    st.json(obs_data)
