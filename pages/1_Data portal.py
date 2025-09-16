import streamlit as st
import pandas as pd
from datetime import date, datetime
import shutil
import csv
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
# Default fallbacks (used if no CSV is provided or CSV is malformed)
DEFAULT_OBSERVER_HOTELS = {
    "Alice": ["H001", "H002"],
    "Bob": ["H003"],
    "Charlie": ["H004", "H005"]
}
DEFAULT_HOTEL_HOLES = {
    "H001": ["1", "2", "3"],
    "H002": ["A", "B"],
    "H003": ["Left", "Right"],
    "H004": ["X", "Y"],
    "H005": ["Alpha", "Beta"]
}

# Attempt to load a single long-form CSV with columns (observer, hotel, hole).
# Expected path: data/observer_hotel_holes.csv (case-insensitive column names accepted).
OBSERVER_HOTELS = DEFAULT_OBSERVER_HOTELS.copy()
HOTEL_HOLES = DEFAULT_HOTEL_HOLES.copy()
oh_path = os.path.join("data", "observer_hotel_holes.csv")
if os.path.exists(oh_path):
    try:
        oh_df = pd.read_csv(oh_path)
        # Normalize column names to lowercase for detection
        col_map = {c.lower(): c for c in oh_df.columns}
        # find best matches for observer, hotel, hole
        obs_col = next((col_map[k] for k in col_map if "observer" in k or "observer" == k), None)
        hotel_col = next((col_map[k] for k in col_map if "hotel" in k or "hotel_code" in k), None)
        hole_col = next((col_map[k] for k in col_map if "hole" in k or "nest" in k), None)

        if not (obs_col and hotel_col and hole_col):
            st.warning(f"{oh_path} is missing required columns (observer, hotel, hole). Using defaults.")
        else:
            OBSERVER_HOTELS = {}
            HOTEL_HOLES = {}
            for _, row in oh_df.iterrows():
                obs = str(row.get(obs_col, "")).strip()
                hotel = str(row.get(hotel_col, "")).strip()
                hole = str(row.get(hole_col, "")).strip()
                if not obs or not hotel:
                    continue
                OBSERVER_HOTELS.setdefault(obs, [])
                if hotel not in OBSERVER_HOTELS[obs]:
                    OBSERVER_HOTELS[obs].append(hotel)
                HOTEL_HOLES.setdefault(hotel, [])
                if hole and hole not in HOTEL_HOLES[hotel]:
                    HOTEL_HOLES[hotel].append(hole)
            # sort lists for deterministic ordering
            for k in OBSERVER_HOTELS:
                try:
                    OBSERVER_HOTELS[k] = sorted(OBSERVER_HOTELS[k])
                except Exception:
                    pass
            for k in HOTEL_HOLES:
                try:
                    HOTEL_HOLES[k] = sorted(HOTEL_HOLES[k], key=lambda x: (len(x), x))
                except Exception:
                    pass
    except Exception as e:
        st.warning(f"Failed to load {oh_path}: {e}. Using defaults.")

DATA_FILE = "observations.csv"

# Load existing local data
def safe_read_csv(path):
    """Read a CSV path safely. If a ParserError occurs, move the broken file to a backup and return empty DataFrame."""
    if not os.path.exists(path):
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except pd.errors.ParserError as e:
        # Backup the malformed file and continue with empty DataFrame
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup = f"{path}.broken_{ts}.bak"
        try:
            shutil.move(path, backup)
            st.warning(f"Existing {path} was malformed and moved to {backup}. Starting fresh.")
        except Exception as mv_err:
            st.error(f"Failed to move malformed {path}: {mv_err}")
        return pd.DataFrame()
    except Exception as e:
        st.warning(f"Failed to read {path}: {e}")
        return pd.DataFrame()

df = safe_read_csv(DATA_FILE)

# Build species list from data/species_names.csv if present, otherwise fall back to historical data
species_file = os.path.join("data", "species_names.csv")
species_list = []
if os.path.exists(species_file):
    try:
        sp_df = pd.read_csv(species_file)
        if "scientific_name" in sp_df.columns:
            species_list = sorted(sp_df["scientific_name"].dropna().astype(str).str.strip().unique().tolist())
    except Exception as e:
        st.warning(f"Failed to read {species_file}: {e}")

if not species_list and not df.empty and "scientific_name" in df.columns:
    try:
        species_list = sorted(df["scientific_name"].dropna().astype(str).str.strip().unique().tolist())
    except Exception:
        species_list = []

st.title("📝 Bee Hotel Observation Portal")

# --- Top-level observer selection and passphrase gate ---
observer = st.selectbox("Recorded by*", list(OBSERVER_HOTELS.keys()), key="observer")

# Load observer passphrases (prefer environment variable, fall back to CSV)
passphrases = {}
env_val = os.environ.get("OBSERVER_PASSPHRASES")
if env_val:
    try:
        import json as _json
        parsed = _json.loads(env_val)
        if isinstance(parsed, dict):
            # normalize keys to strings
            passphrases = {str(k): str(v) for k, v in parsed.items()}
        else:
            st.warning("Environment variable OBSERVER_PASSPHRASES is not a JSON object. Falling back to CSV.")
            env_val = None
    except Exception as e:
        st.warning(f"Failed to parse OBSERVER_PASSPHRASES env var: {e}. Falling back to CSV.")
        env_val = None

if not env_val:
    pass_file = os.path.join("data", "observer_passphrases.csv")
    if os.path.exists(pass_file):
        try:
            pf = pd.read_csv(pass_file)
            col_map = {c.lower(): c for c in pf.columns}
            pass_col = next((col_map[k] for k in col_map if "pass" in k or "phrase" in k), None)
            obs_col = next((col_map[k] for k in col_map if "observer" in k or "observer" == k), None)
            if pass_col and obs_col:
                for _, row in pf.iterrows():
                    obs = str(row.get(obs_col, "")).strip()
                    pw = str(row.get(pass_col, "")).strip()
                    if obs:
                        passphrases[obs] = pw
            else:
                st.info(f"{pass_file} found but missing expected columns (observer, passphrase). Passphrase gating disabled.")
        except Exception as e:
            st.warning(f"Failed to read {pass_file}: {e}. Passphrase gating disabled.")

# Prepare gating state
hotel_code = None
submitted = False

# Ask for passphrase (only if there is a selected observer)
if observer:
    pass_key = f"pass_input_{observer}"
    unlock_key = f"unlock_{observer}"
    verified_key = f"pass_ok_{observer}"
    # show pass input and unlock button
    st.write("Enter your unique passphrase to unlock the data portal for this observer.")
    pw = st.text_input("Passphrase", type="password", key=pass_key)
    if st.button("Unlock", key=unlock_key):
        expected = passphrases.get(observer)
        if expected is None:
            st.warning("No passphrase configured for this observer. Contact an admin to enable passphrase protection.")
            st.session_state[verified_key] = False
        elif pw == expected:
            st.session_state[verified_key] = True
            st.success("Passphrase accepted — proceed to select your hotel.")
        else:
            st.session_state[verified_key] = False
            st.error("Incorrect passphrase. Try again.")

# If verified, show hotel selector
if observer and st.session_state.get(f"pass_ok_{observer}", False):
    available_hotels = OBSERVER_HOTELS.get(observer, [])
    hotel_code = st.selectbox("Hotel code*", available_hotels, key="hotel_code_top")

# Only when hotel_code is selected do we show the observation form
if hotel_code:
    with st.form("observation_form", clear_on_submit=True):

        # --- Section 2: observation date/time/image ---
        st.header("Observation details")
        # Show observer and hotel inside form as read-only
        # col1, col2 = st.columns([1, 2])
        # with col1:
        #     st.text_input("Recorded by", value=observer, disabled=True, key="observer_in_form")
        # with col2:
        #     st.text_input("Hotel code", value=hotel_code, disabled=True, key="hotel_in_form")
        # Stack date and time vertically in left column, image in right column with ratio 1:2
        dcol_left, dcol_right = st.columns([1, 2])
        with dcol_left:
            obs_date = st.date_input("Obs. date*", value=date.today(), key="obs_date")
            obs_time = st.time_input("Obs. time*", value=datetime.now().time(), key="obs_time")
        with dcol_right:
            photo = st.file_uploader("Image", type=["jpg", "jpeg", "png"], key="photo")

        # --- Section 3: grid for nest holes (rows A-K or from HOTEL_HOLES) ---
        st.header("Nest holes")
        hole_values = {}

        # Column headers for grid (ratios: hole, sci, males, females, social_behaviour, notes)
        rcols = st.columns([1, 4, 2, 2, 4, 4])
        rcols[0].markdown("**Hole**")
        rcols[1].markdown("**Scientific name**")
        rcols[2].markdown("**# Males**")
        rcols[3].markdown("**# Females**")
        rcols[4].markdown("**Social behaviours**")
        rcols[5].markdown("**Notes**")

        # Determine holes for selected hotel (fallback to A-K)
        holes_for_hotel = HOTEL_HOLES.get(hotel_code) if hotel_code else None
        if not holes_for_hotel:
            holes_for_hotel = [chr(i) for i in range(ord('A'), ord('K')+1)]

        for hole_label in holes_for_hotel:
            c0, c1, c2, c3, c4, c5 = st.columns([1, 4, 2, 2, 4, 4])

            # Prepopulate defaults if possible
            defaults = {"scientific_name": "", "num_males": 0, "num_females": 0, "social_behaviour": []}
            if not df.empty and hotel_code:
                subset = df[(df["hotel_code"] == hotel_code) & (df["nest_hole"] == hole_label)]
                if not subset.empty:
                    last_entry = subset.iloc[-1]
                    defaults = {
                        "scientific_name": last_entry.get("scientific_name", ""),
                        "num_males": int(last_entry.get("num_males", 0)),
                        "num_females": int(last_entry.get("num_females", 0)),
                        "social_behaviour": last_entry.get("social_behaviour", "").split(", ") if last_entry.get("social_behaviour") else []
                    }

            with c0:
                st.markdown(f"**{hole_label}**")
            with c1:
                # Use species dropdown sourced from data/species_names.csv (fallback to historical species)
                local_species = species_list.copy() if species_list else []
                if "" not in local_species:
                    local_species.insert(0, "")
                label = f"Scientific name for hole {hole_label}"
                try:
                    default_index = local_species.index(defaults["scientific_name"]) if defaults["scientific_name"] in local_species else 0
                except Exception:
                    default_index = 0
                sci = st.selectbox(label, local_species, index=default_index, key=f"sci_{hole_label}", label_visibility='collapsed')
            with c2:
                nm = st.number_input(f"males for {hole_label}", min_value=0, step=1, value=defaults["num_males"], key=f"males_{hole_label}", label_visibility='collapsed')
            with c3:
                nf = st.number_input(f"females for {hole_label}", min_value=0, step=1, value=defaults["num_females"], key=f"fem_{hole_label}", label_visibility='collapsed')
            with c4:
                sb = st.multiselect(f"social_behaviour for {hole_label}", ["Solitary", "Social", "Parasitic"], default=defaults["social_behaviour"], key=f"sb_{hole_label}", label_visibility='collapsed')
            with c5:
                notes = st.text_input(f"notes for {hole_label}", key=f"notes_{hole_label}", label_visibility='collapsed')

            hole_values[hole_label] = {
                "scientific_name": sci,
                "num_males": nm,
                "num_females": nf,
                "social_behaviour": sb,
                "notes": notes
            }

        # Right-align the submit button using a narrow right column and a right-aligned div
        btn_col_left, btn_col_spacer, btn_col_right = st.columns([6, 1, 1])
        with btn_col_right:
            st.markdown("<div style='text-align: right;'>", unsafe_allow_html=True)
            submitted = st.form_submit_button("Submit")
            st.markdown("</div>", unsafe_allow_html=True)

    

if submitted:
    # Validate required top-level fields
    if not observer or not hotel_code:
        st.error("⚠️ Please fill in all required fields: Recorded by and Hotel code")
    else:
        submission_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # --- Upload photo to Dropbox once (if provided) ---
        photo_link = None
        photo_bytes = None
        if photo:
            photo_bytes = photo.read()

        rows_to_save = []
        uploaded_csvs = []

        # Read fresh values from st.session_state to avoid using potentially stale captured dict
        for hole_label in list(hole_values.keys()):
            sci = str(st.session_state.get(f"sci_{hole_label}", "")).strip()
            nm = st.session_state.get(f"males_{hole_label}", 0)
            nf = st.session_state.get(f"fem_{hole_label}", 0)
            sb = st.session_state.get(f"sb_{hole_label}", []) or []
            notes_text = str(st.session_state.get(f"notes_{hole_label}", "")).strip()

            # Consider a hole 'filled' if it has a scientific name, counts, social behaviour, or notes
            if sci or nm > 0 or nf > 0 or sb or notes_text:
                    # Create a single submission_id for this form submit (used below)
                    # We'll create submission_id outside the loop once; if not present, create it now
                    if "submission_id" not in locals():
                        submission_id = str(uuid.uuid4())

                        # Upload photo once and reuse photo_link
                        if photo_bytes:
                            try:
                                photo_filename = f"{submission_id}_{photo.name}"
                                dropbox_path = f"/observations/photos/{photo_filename}"
                                dbx.files_upload(photo_bytes, dropbox_path, mode=dropbox.files.WriteMode.overwrite)
                                shared_link = dbx.sharing_create_shared_link_with_settings(dropbox_path)
                                photo_link = shared_link.url.replace("?dl=0", "?raw=1")
                            except Exception as e:
                                st.warning(f"Photo upload failed: {e}")
                                photo_link = None
                        else:
                            photo_link = None

                    obs_id = str(uuid.uuid4())

                    obs_data = {
                        "obs_id": obs_id,
                        "submission_id": submission_id,
                        "observer": observer,
                        "hotel_code": hotel_code,
                        "obs_date": str(obs_date),
                        "obs_time": str(obs_time),
                        "nest_hole": hole_label,
                        "scientific_name": sci,
                        "num_males": nm,
                        "num_females": nf,
                        "social_behaviour": ", ".join(sb),
                        "notes": notes_text,
                        "photo_link": photo_link,
                        "submission_time": submission_time
                    }

                    rows_to_save.append(obs_data)

                    # Upload per-observation CSV (optional)
                    try:
                        new_df = pd.DataFrame([obs_data])
                        obs_csv_filename = f"{obs_id}.csv"
                        csv_buffer = new_df.to_csv(index=False, quoting=csv.QUOTE_MINIMAL).encode("utf-8")
                        dbx.files_upload(csv_buffer, f"/observations/csv/{obs_csv_filename}", mode=dropbox.files.WriteMode.overwrite)
                        uploaded_csvs.append(obs_csv_filename)
                    except Exception as e:
                        st.warning(f"CSV upload failed for hole {hole_label}: {e}")

        # Save all rows locally at once
        if rows_to_save:
            # Build long-form DataFrame with requested columns and linkage fields
            cols = [
                "obs_id",
                "observer",
                "hotel_code",
                "obs_date",
                "obs_time",
                "nest_hole",
                "scientific_name",
                "num_males",
                "num_females",
                "social_behaviour",
                "notes",
                "submission_id",
                "photo_link",
                "submission_time"
            ]
            all_df = pd.DataFrame(rows_to_save)
            # Ensure all columns exist in DataFrame
            for c in cols:
                if c not in all_df.columns:
                    all_df[c] = ""
            all_df = all_df[cols]

            # Preparing to save rows

            # Read existing file (if any) and overwrite with combined data for atomicity
            try:
                if os.path.exists(DATA_FILE):
                    existing_df = safe_read_csv(DATA_FILE)
                    # If safe_read_csv returned empty (malformed file was backed up), just write new data
                    if existing_df.empty:
                        all_df.to_csv(DATA_FILE, index=False, quoting=csv.QUOTE_MINIMAL)
                    else:
                        combined = pd.concat([existing_df, all_df], ignore_index=True)
                        combined.to_csv(DATA_FILE, index=False, quoting=csv.QUOTE_MINIMAL)
                else:
                    all_df.to_csv(DATA_FILE, index=False, quoting=csv.QUOTE_MINIMAL)
                # Also upload the combined master CSV to Dropbox under /observations/observations.csv
                try:
                    try:
                        master_df = existing_df if (os.path.exists(DATA_FILE) and not existing_df.empty) else all_df
                    except Exception:
                        master_df = all_df
                    csv_bytes = master_df.to_csv(index=False, quoting=csv.QUOTE_MINIMAL).encode('utf-8')
                    dbx.files_upload(csv_bytes, '/observations/observations.csv', mode=dropbox.files.WriteMode.overwrite)
                except Exception as e:
                    st.warning(f"Failed to upload master observations.csv to Dropbox: {e}")
                st.success(f"✅ Recorded {len(rows_to_save)} observation(s) for hotel {hotel_code}")
                st.json(all_df.to_dict(orient="records")[0] if len(all_df) == 1 else all_df.to_dict(orient="records"))
            except Exception as e:
                st.error(f"Failed to write observations to {DATA_FILE}: {e}")
        else:
            st.info("No hole rows had data to submit. Please fill at least one hole row.")
