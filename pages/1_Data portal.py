import streamlit as st
import pandas as pd
from datetime import date, datetime
import uuid
import dropbox
import json
import os

# --- Load Dropbox token ---
with open("secrets.json") as f:
    secrets = json.load(f)

APP_KEY = secrets["DROPBOX_APP_KEY"]
APP_SECRET = secrets["DROPBOX_APP_SECRET"]
REFRESH_TOKEN = secrets["DROPBOX_REFRESH_TOKEN"]

dbx = dropbox.Dropbox(
    app_key=APP_KEY,
    app_secret=APP_SECRET,
    oauth2_refresh_token=REFRESH_TOKEN
)

# --- Observer → Hotel mapping ---
OBSERVER_HOTELS = {
    "Alice": ["H001", "H002"],
    "Bob": ["H003"],
    "Charlie": ["H004", "H005"]
}
HOTEL_HOLES = {
    "H001": ["1", "2", "3"],
    "H002": ["A", "B"],
    "H003": ["Left", "Right"],
    "H004": ["X", "Y"],
    "H005": ["Alpha", "Beta"]
}

DATA_FILE = "observations.csv"

# Load existing local data
if os.path.exists(DATA_FILE):
    df = pd.read_csv(DATA_FILE)
else:
    df = pd.DataFrame()

st.title("📝 Bee Hotel Observation Portal")

# --- Observer outside form ---
observer = st.selectbox("Recorded by*", list(OBSERVER_HOTELS.keys()), key="observer")
available_hotels = OBSERVER_HOTELS.get(observer, [])

with st.form("observation_form", clear_on_submit=True):
    col1, col2 = st.columns(2)
    with col1:
        hotel_code = st.selectbox("Hotel code*", available_hotels, key="hotel_code")
    with col2:
        nest_hole = st.selectbox("Nest hole*", HOTEL_HOLES.get(hotel_code, []), key="nest_hole")

    # --- Prepopulate defaults ---
    defaults = {"scientific_name": "", "num_males": 0, "num_females": 0, "social_behaviour": []}
    if not df.empty and hotel_code and nest_hole:
        subset = df[(df["hotel_code"] == hotel_code) & (df["nest_hole"] == nest_hole)]
        if not subset.empty:
            last_entry = subset.iloc[-1]
            defaults = {
                "scientific_name": last_entry["scientific_name"],
                "num_males": int(last_entry["num_males"]),
                "num_females": int(last_entry["num_females"]),
                "social_behaviour": last_entry["social_behaviour"].split(", ")
            }

    scientific_name = st.text_input("Scientific name", value=defaults["scientific_name"])
    num_males = st.number_input("Number of males", min_value=0, step=1, value=defaults["num_males"])
    num_females = st.number_input("Number of females", min_value=0, step=1, value=defaults["num_females"])
    social_behaviour = st.multiselect(
        "Social behaviour", ["Solitary", "Social", "Parasitic"],
        default=defaults["social_behaviour"]
    )

    obs_date = st.date_input("Observation date*", value=date.today())
    obs_time = st.time_input("Observation time*", value=datetime.now().time())
    photo = st.file_uploader("Upload an image", type=["jpg", "jpeg", "png"])
    notes = st.text_area("Notes")

    submitted = st.form_submit_button("Submit Observation")

if submitted:
    if not observer or not hotel_code or not nest_hole:
        st.error("⚠️ Please fill in all required fields: Recorded by, Hotel code, Nest hole")
    else:
        obs_id = str(uuid.uuid4())
        submission_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # --- Upload photo to Dropbox ---
        photo_link = None
        if photo:
            photo_filename = f"{obs_id}_{photo.name}"
            dropbox_path = f"/observations/photos/{photo_filename}"
            dbx.files_upload(photo.read(), dropbox_path, mode=dropbox.files.WriteMode.overwrite)
            shared_link = dbx.sharing_create_shared_link_with_settings(dropbox_path)
            photo_link = shared_link.url.replace("?dl=0", "?raw=1")

        # --- Save observation metadata ---
        obs_data = {
            "obs_id": obs_id,
            "observer": observer,
            "obs_date": str(obs_date),
            "obs_time": str(obs_time),
            "hotel_code": hotel_code,
            "nest_hole": nest_hole,
            "scientific_name": scientific_name,
            "num_males": num_males,
            "num_females": num_females,
            "social_behaviour": ", ".join(social_behaviour),
            "notes": notes,
            "submission_time": submission_time,
            "photo_link": photo_link
        }

        # Save locally
        new_df = pd.DataFrame([obs_data])
        new_df.to_csv(DATA_FILE, mode="a", header=not os.path.exists(DATA_FILE), index=False)

        # Upload to Dropbox as single-observation CSV
        obs_csv_filename = f"{obs_id}.csv"
        csv_buffer = new_df.to_csv(index=False).encode("utf-8")
        dbx.files_upload(csv_buffer, f"/observations/csv/{obs_csv_filename}", mode=dropbox.files.WriteMode.overwrite)

        st.success("✅ Observation recorded successfully!")
        st.json(obs_data)
