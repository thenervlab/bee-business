import streamlit as st
import pandas as pd
from datetime import date, datetime
import shutil
import csv
import uuid
import dropbox
import json
import os
import requests
from io import StringIO
from streamlit_javascript import st_javascript
import pytz



# --- Load secrets: prefer Streamlit secrets, then environment, then local secrets.json ---
def load_secrets():
    # Try Streamlit secrets (works on Streamlit Community Cloud)
    try:
        ss = {}
        # st.secrets behaves like a dict; use .get to avoid KeyError
        ss_keys = ["DROPBOX_APP_KEY", "DROPBOX_APP_SECRET", "DROPBOX_REFRESH_TOKEN", "OBSERVER_PASSPHRASES"]
        for k in ss_keys:
            try:
                ss[k] = st.secrets.get(k)
            except Exception:
                ss[k] = None
        # If at least the Dropbox keys are present, return
        if ss.get("DROPBOX_APP_KEY") and ss.get("DROPBOX_APP_SECRET") and ss.get("DROPBOX_REFRESH_TOKEN"):
            return ss
    except Exception:
        pass

    # Next, try environment variables
    env_keys = ["DROPBOX_APP_KEY", "DROPBOX_APP_SECRET", "DROPBOX_REFRESH_TOKEN", "OBSERVER_PASSPHRASES"]
    env = {k: os.environ.get(k) for k in env_keys}
    if env.get("DROPBOX_APP_KEY") and env.get("DROPBOX_APP_SECRET") and env.get("DROPBOX_REFRESH_TOKEN"):
        return env

    # Fallback: local secrets.json (for local dev only)
    if os.path.exists("secrets.json"):
        try:
            with open("secrets.json") as f:
                return json.load(f)
        except Exception:
            pass

    # Nothing found
    return {}


# Define the save and upload function
def save_observation(rows_to_save, hotel_code, DATA_FILE, dbx):
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
                # Reconcile local and remote pieces and upload authoritative master
                try:
                    # Use a lighter-weight incremental update on submit to avoid listing/downloading many files
                    authoritative = incremental_master_update(dbx, all_df, local_path=DATA_FILE)
                    if isinstance(authoritative, pd.DataFrame) and not authoritative.empty:
                        df = authoritative.copy()
                    else:
                        try:
                            df = safe_read_csv(DATA_FILE)
                        except Exception:
                            df = df
                except Exception as e:
                    st.warning(f"Incremental upload failed: {e}")

                st.success(f"✅ Recorded {len(rows_to_save)} observation(s) for hotel {hotel_code}")
                st.json(all_df.to_dict(orient="records")[0] if len(all_df) == 1 else all_df.to_dict(orient="records"))
            except Exception as e:
                st.error(f"Failed to write observations to {DATA_FILE}: {e}")

secrets = load_secrets()

APP_KEY = secrets.get("DROPBOX_APP_KEY")
APP_SECRET = secrets.get("DROPBOX_APP_SECRET")
REFRESH_TOKEN = secrets.get("DROPBOX_REFRESH_TOKEN")

timezone = st_javascript("""await (async () => {
            const userTimezone = Intl.DateTimeFormat().resolvedOptions().timeZone;
            console.log(userTimezone)
            return userTimezone
            })().then(returnValue => returnValue)""")


# Initialize Dropbox client only if credentials are available
dbx = None
if APP_KEY and APP_SECRET and REFRESH_TOKEN:
    try:
        dbx = dropbox.Dropbox(
            app_key=APP_KEY,
            app_secret=APP_SECRET,
            oauth2_refresh_token=REFRESH_TOKEN
        )
    except Exception as e:
        st.warning(f"Failed to initialize Dropbox client: {e}")
        dbx = None
else:
    st.warning("Dropbox credentials not found in Streamlit secrets or environment; photo uploads will be disabled.")

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

# Try remote URL first (st.secrets or env), then local file, then defaults
@st.cache_data(show_spinner=False)
def fetch_csv_from_url(url: str, token: str = None):
    if not url:
        return None
    try:
        headers = {}
        if token:
            headers["Authorization"] = f"token {token}"
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        return pd.read_csv(StringIO(resp.text))
    except Exception:
        return None

# Look for a configured remote URL in st.secrets or environment
oh_url = None
try:
    oh_url = st.secrets.get("OBSERVER_HOTEL_CSV_URL") or st.secrets.get("OBSERVER_HOTELS_URL")
except Exception:
    oh_url = None
if not oh_url:
    oh_url = os.environ.get("OBSERVER_HOTEL_CSV_URL") or os.environ.get("OBSERVER_HOTELS_URL")

# Optional GitHub token for private raw URLs
GITHUB_TOKEN = None
try:
    GITHUB_TOKEN = st.secrets.get("GITHUB_TOKEN")
except Exception:
    GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")

oh_df = None
if oh_url:
    oh_df = fetch_csv_from_url(oh_url, token=GITHUB_TOKEN)
if oh_df is None and os.path.exists(oh_path):
    try:
        oh_df = pd.read_csv(oh_path)
    except Exception as e:
        st.warning(f"Failed to read {oh_path}: {e}. Using defaults.")

if oh_df is not None:
    try:
        # Normalize column names to lowercase for detection
        col_map = {c.lower(): c for c in oh_df.columns}
        # find best matches for observer, hotel, hole
        obs_col = next((col_map[k] for k in col_map if "observer" in k or "observer" == k), None)
        hotel_col = next((col_map[k] for k in col_map if "hotel" in k or "hotel_code" in k), None)
        hole_col = next((col_map[k] for k in col_map if "hole" in k or "nest" in k), None)

        if not (obs_col and hotel_col and hole_col):
            st.warning("Observer/hotel CSV is missing required columns (observer, hotel, hole). Using defaults.")
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
        st.warning(f"Failed to process observer/hotel CSV: {e}. Using defaults.")

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

def reconcile_and_upload_master(dbx_client, local_path=DATA_FILE):
    """Reconcile local observations file with per-observation CSVs stored in Dropbox.
    This function:
    - Reads the local `local_path` safely
    - Attempts to list and download all CSVs under `/observations/csv/` on Dropbox
    - Optionally reads existing remote master `/observations/observations.csv`
    - Concatenates all available rows, deduplicates by `obs_id` preferring the latest by `submission_time`,
      writes the authoritative local file, and uploads it to Dropbox as `/observations/observations.csv`.
    Returns the authoritative DataFrame (may be empty DataFrame if nothing available).
    """
    # Start with local data
    local_df = safe_read_csv(local_path)

    remote_rows = []
    if dbx_client is not None:
        try:
            # Gather per-observation CSVs
            folder_path = '/observations/csv'
            has_more = True
            cursor = None
            entries = []
            try:
                res = dbx_client.files_list_folder(folder_path)
                entries.extend(res.entries)
                cursor = getattr(res, 'cursor', None)
                has_more = getattr(res, 'has_more', False)
                while has_more:
                    res = dbx_client.files_list_folder_continue(cursor)
                    entries.extend(res.entries)
                    cursor = getattr(res, 'cursor', None)
                    has_more = getattr(res, 'has_more', False)
            except dropbox.exceptions.ApiError:
                # Folder may not exist; that's fine
                entries = []

            for e in entries:
                try:
                    name = getattr(e, 'name', '')
                    if name.lower().endswith('.csv'):
                        md, resp = dbx_client.files_download(f"{folder_path}/{name}")
                        txt = resp.content.decode('utf-8')
                        try:
                            df_piece = pd.read_csv(StringIO(txt))
                            remote_rows.append(df_piece)
                        except Exception:
                            # skip malformed remote pieces
                            continue
                except Exception:
                    continue

            # Also try to read existing remote master (if present) to be extra-safe
            try:
                md_meta, md_res = dbx_client.files_download('/observations/observations.csv')
                try:
                    master_remote = pd.read_csv(StringIO(md_res.content.decode('utf-8')))
                    remote_rows.append(master_remote)
                except Exception:
                    pass
            except Exception:
                pass
        except Exception:
            # Any Dropbox error should not crash reconciliation — continue with what we have
            pass

    # Combine available frames
    pieces = [df for df in ([local_df] + remote_rows) if isinstance(df, pd.DataFrame) and not df.empty]
    if pieces:
        try:
            combined = pd.concat(pieces, ignore_index=True, sort=False)
        except Exception:
            # Fallback: use local only
            combined = local_df.copy() if isinstance(local_df, pd.DataFrame) else pd.DataFrame()
    else:
        combined = local_df.copy() if isinstance(local_df, pd.DataFrame) else pd.DataFrame()

    # Normalize columns and dedupe by obs_id, preferring latest submission_time
    if not combined.empty and 'obs_id' in combined.columns:
        # parse submission_time where possible
        if 'submission_time' in combined.columns:
            try:
                combined['__st'] = pd.to_datetime(combined['submission_time'], errors='coerce')
            except Exception:
                combined['__st'] = pd.NaT
            # sort ascending so drop_duplicates(keep='last') keeps the most recent
            try:
                combined = combined.sort_values('__st', na_position='first')
            except Exception:
                pass
        try:
            combined = combined.drop_duplicates(subset=['obs_id'], keep='last').reset_index(drop=True)
        except Exception:
            combined = combined.drop_duplicates(subset=['obs_id']).reset_index(drop=True)
        # clean up temporary column
        if '__st' in combined.columns:
            try:
                combined = combined.drop(columns=['__st'])
            except Exception:
                pass

    # Ensure we have a local file written as authoritative
    try:
        combined.to_csv(local_path, index=False, quoting=csv.QUOTE_MINIMAL)
    except Exception:
        try:
            if isinstance(local_df, pd.DataFrame) and not local_df.empty:
                local_df.to_csv(local_path, index=False, quoting=csv.QUOTE_MINIMAL)
        except Exception:
            pass

    # Upload the authoritative master to Dropbox
    if dbx_client is not None and not combined.empty:
        try:
            csv_bytes = combined.to_csv(index=False, quoting=csv.QUOTE_MINIMAL).encode('utf-8')
            dbx_client.files_upload(csv_bytes, '/observations/observations.csv', mode=dropbox.files.WriteMode.overwrite)
        except Exception:
            # If upload fails, do not raise — UI should already have saved local file
            pass

    return combined


def incremental_master_update(dbx_client, new_rows_df, local_path=DATA_FILE):
    """A lighter-weight update for submit-time:
    - Downloads the remote master `/observations/observations.csv` if present (single file only)
    - Concatenates `new_rows_df` with the remote master (or local file if remote missing)
    - Deduplicates by `obs_id`, preferring latest `submission_time`
    - Writes local authoritative file and uploads it to Dropbox
    Returns the authoritative DataFrame
    """
    # Start with existing local
    local_df = safe_read_csv(local_path)

    # Attempt to download remote master (single file) — cheaper than listing folder
    remote_master = None
    if dbx_client is not None:
        try:
            md_meta, md_res = dbx_client.files_download('/observations/observations.csv')
            try:
                remote_master = pd.read_csv(StringIO(md_res.content.decode('utf-8')))
            except Exception:
                remote_master = None
        except Exception:
            remote_master = None

    # Determine base to combine with: prefer remote_master, then local_df, else empty
    base = remote_master if (isinstance(remote_master, pd.DataFrame) and not remote_master.empty) else (local_df if not local_df.empty else pd.DataFrame())

    pieces = [p for p in [base, new_rows_df] if isinstance(p, pd.DataFrame) and not p.empty]
    if pieces:
        try:
            combined = pd.concat(pieces, ignore_index=True, sort=False)
        except Exception:
            combined = new_rows_df.copy()
    else:
        combined = new_rows_df.copy() if isinstance(new_rows_df, pd.DataFrame) else pd.DataFrame()

    # Dedupe by obs_id preferring latest submission_time
    if not combined.empty and 'obs_id' in combined.columns:
        if 'submission_time' in combined.columns:
            try:
                combined['__st'] = pd.to_datetime(combined['submission_time'], errors='coerce')
                combined = combined.sort_values('__st', na_position='first')
            except Exception:
                pass
        try:
            combined = combined.drop_duplicates(subset=['obs_id'], keep='last').reset_index(drop=True)
        except Exception:
            combined = combined.drop_duplicates(subset=['obs_id']).reset_index(drop=True)
        if '__st' in combined.columns:
            try:
                combined = combined.drop(columns=['__st'])
            except Exception:
                pass

    # Write local authoritative file
    try:
        combined.to_csv(local_path, index=False, quoting=csv.QUOTE_MINIMAL)
    except Exception:
        pass

    # Upload authoritative master
    if dbx_client is not None and not combined.empty:
        try:
            csv_bytes = combined.to_csv(index=False, quoting=csv.QUOTE_MINIMAL).encode('utf-8')
            dbx_client.files_upload(csv_bytes, '/observations/observations.csv', mode=dropbox.files.WriteMode.overwrite)
        except Exception:
            pass

    return combined
# If Dropbox is configured in this environment, prefer the master CSV stored in Dropbox
if dbx is not None:
    try:
        # Attempt to download the master observations.csv from Dropbox
        md_path = "/observations/observations.csv"
        remote_master_found = False
        try:
            md_meta, md_res = dbx.files_download(md_path)
            content = md_res.content.decode("utf-8")
            try:
                remote_df = pd.read_csv(StringIO(content))
                if not remote_df.empty:
                    df = remote_df
                    remote_master_found = True
            except Exception as e:
                st.warning(f"Failed to parse remote master CSV from Dropbox: {e}")
        except Exception:
            # No remote master CSV found or download failed; keep local df
            pass
    except Exception:
        # Any error with Dropbox should not break the app — keep local df
        pass

    # Run a full reconciliation on startup only if no remote master exists
    try:
        if not globals().get('remote_master_found', False):
            reconciled = reconcile_and_upload_master(dbx, local_path=DATA_FILE)
            if isinstance(reconciled, pd.DataFrame) and not reconciled.empty:
                df = reconciled
    except Exception:
        # If reconciliation fails, keep whatever df we already loaded
        pass

# Build species list from data/species_names.csv if present, otherwise fall back to historical data
species_file = os.path.join("data", "species_names.csv")
species_list = []
# Try remote URL first (st.secrets or env), then local file, then fallback to historical data
species_url = None
try:
    species_url = st.secrets.get("SPECIES_CSV_URL") or st.secrets.get("SPECIES_LIST_URL")
except Exception:
    species_url = None
if not species_url:
    species_url = os.environ.get("SPECIES_CSV_URL") or os.environ.get("SPECIES_LIST_URL")

sp_df = None
if species_url:
    sp_df = fetch_csv_from_url(species_url, token=GITHUB_TOKEN)
if sp_df is None and os.path.exists(species_file):
    try:
        sp_df = pd.read_csv(species_file)
    except Exception as e:
        st.warning(f"Failed to read {species_file}: {e}")

if sp_df is not None:
    try:
        if "scientific_name" in sp_df.columns:
            species_list = sorted(sp_df["scientific_name"].dropna().astype(str).str.strip().unique().tolist())
    except Exception as e:
        st.warning(f"Failed to parse species CSV: {e}")

if not species_list and not df.empty and "scientific_name" in df.columns:
    try:
        species_list = sorted(df["scientific_name"].dropna().astype(str).str.strip().unique().tolist())
    except Exception:
        species_list = []

st.title("📝 Bee Hotel Observation Portal")

# --- Top-level observer selection and passphrase gate ---
observer = st.selectbox("Recorded by*", list(OBSERVER_HOTELS.keys()), key="observer")

# Load observer passphrases (prefer Streamlit secrets, then environment variable, fall back to CSV)
passphrases = {}

# 1) Try Streamlit secrets
try:
    raw = None
    try:
        raw = st.secrets.get("OBSERVER_PASSPHRASES")
    except Exception:
        raw = None

    if raw:
        if isinstance(raw, dict):
            passphrases = {str(k): str(v) for k, v in raw.items()}
        else:
            try:
                import json as _json
                parsed = _json.loads(str(raw))
                if isinstance(parsed, dict):
                    passphrases = {str(k): str(v) for k, v in parsed.items()}
            except Exception:
                pass
except Exception:
    pass

# 2) Environment variable (JSON string)
if not passphrases:
    env_val = os.environ.get("OBSERVER_PASSPHRASES")
    if env_val:
        try:
            import json as _json
            parsed = _json.loads(env_val)
            if isinstance(parsed, dict):
                passphrases = {str(k): str(v) for k, v in parsed.items()}
        except Exception:
            st.warning("Environment variable OBSERVER_PASSPHRASES is not valid JSON. Falling back to CSV.")

# 3) CSV fallback
if not passphrases:
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
    with st.form("observation_form", clear_on_submit=False, enter_to_submit  = False):

        # --- Section 2: observation date/time/image ---
        st.header("Observation details")
        # Show observer and hotel inside form as read-only
        # col1, col2 = st.columns([1, 2])
        # with col1:
        #     st.text_input("Recorded by", value=observer, disabled=True, key="observer_in_form")
        # with col2:
        #     st.text_input("Hotel code", value=hotel_code, disabled=True, key="hotel_in_form")
        # Arrange observation details into three equal columns:
        # (1) Obs date/time + image uploader, (2) Overall Notes, (3) Logo/image display
        col_left, col_mid, col_right = st.columns([1, 1, 1])
        with col_left:
            
            obs_date = st.date_input("Obs. date*", value=datetime.now(pytz.timezone(timezone)), key="obs_date")
            obs_time = st.time_input("Obs. time (24-hour)*", value=datetime.now(pytz.timezone(timezone)), key="obs_time")
            # Image uploader now sits under date/time in the left column
            photo = st.file_uploader("Image*", type=["jpg", "jpeg", "png"], key="photo")
        with col_mid:
            # Submission-level notes (standalone, before nest holes)
            notes_submission = st.text_area("Overall notes", value="", key="notes_submission")
        with col_right:
            # Show a small logo or hotel image. Try multiple candidate paths
            # Render images at a fixed display width to avoid overly tall renders
            IMAGE_DISPLAY_WIDTH = 220
            try:
                candidates = [
                    os.path.join('data', 'logo.png'),
                    os.path.join('data', 'logo.jpg'),
                    os.path.join('data', 'logo.jpeg'),
                    os.path.join('assets', 'logo.png'),
                    os.path.join('assets', 'logo.jpg'),
                    os.path.join('assets', 'logo.jpeg'),
                    os.path.join('assets', 'logo.svg'),
                ]
                found = next((p for p in candidates if os.path.exists(p)), None)
                # Prefer hotel-specific images first (data/{hotel_code}.jpg/png/jpeg)
                hotel_candidates = [
                    os.path.join('assets', f"{hotel_code}.png"),
                    os.path.join('assets', f"{hotel_code}.jpg"),
                    os.path.join('assets', f"{hotel_code}.jpeg"),
                ]
                hotel_found = next((p for p in hotel_candidates if os.path.exists(p)), None)
                if hotel_found:
                    found = hotel_found

                if found:
                    # Render SVG inline if present, otherwise use st.image with fixed width
                    if found.lower().endswith('.svg'):
                        try:
                            with open(found, 'r', encoding='utf-8') as f:
                                svg = f.read()
                            # Constrain SVG display size with a wrapping div
                            st.markdown(f"<div style='max-width:{IMAGE_DISPLAY_WIDTH}px'>{svg}</div>", unsafe_allow_html=True)
                        except Exception:
                            st.image(found, caption=os.path.basename(found), width=IMAGE_DISPLAY_WIDTH)
                    else:
                        # Caption 'Logo' for generic logo, otherwise show filename/hotel
                        caption = 'Logo' if 'logo' in os.path.basename(found).lower() else f'Hotel {hotel_code}'
                        st.image(found, caption=caption, width=IMAGE_DISPLAY_WIDTH)
                else:
                    st.info("No logo found in data/ or assets/ (checked data/logo.png, assets/logo.png, and hotel images).")
            except Exception:
                pass



        # --- Section 3: grid for nest holes (rows A-K or from HOTEL_HOLES) ---
        st.header("Nest holes")
        hole_values = {}

    # Column headers for grid (ratios: hole, sci, counts-group, social_behaviour, notes)
    # counts-group will be subdivided into #cells | male | female | unknown with relative ratios

        # --- Consolidated one-time header row for the Nest Holes table ---
        # Render a single header row with one label per data column:
        # Hole | Scientific name | Cells | ♂️ | ♀️ | ❔ | Social behaviours | Notes
        hdr_c0, hdr_c1, hdr_c_counts, hdr_c_sb_notes = st.columns([1, 4, 4, 4])
        # First two columns: Hole and Scientific name
        try:
            hdr_c0.markdown("<div style='font-weight:700;'>Hole</div>", unsafe_allow_html=True)
            hdr_c1.markdown("<div style='font-weight:700;'>Scientific name</div>", unsafe_allow_html=True)
        except Exception:
            hdr_c0.markdown("**Hole**")
            hdr_c1.markdown("**Scientific name**")
        # inside the counts header, create 4 small header columns for Cells, male, female, unknown
        h_cells, h_male, h_female, h_unknown = hdr_c_counts.columns([1, 1, 1, 1])
        try:
            h_cells.markdown("<div style='text-align:center;font-weight:600;' title='Number of occupied nest cells in this hole'>Cells</div>", unsafe_allow_html=True)
            h_male.markdown("<div style='text-align:center;' title='Number of male individuals observed'>♂️</div>", unsafe_allow_html=True)
            h_female.markdown("<div style='text-align:center;' title='Number of female individuals observed'>♀️</div>", unsafe_allow_html=True)
            h_unknown.markdown("<div style='text-align:center;' title='Number of individuals of unknown sex'>❔</div>", unsafe_allow_html=True)
        except Exception:
            h_cells.markdown("**Cells**")
            h_male.markdown("**♂️**")
            h_female.markdown("**♀️**")
            h_unknown.markdown("**❔**")
        # split the rightmost header column into Social behaviours and Notes labels
        try:
            hdr_sb, hdr_notes = hdr_c_sb_notes.columns([2, 2])
            hdr_sb.markdown("<div style='font-weight:600;' title='Behaviour observed at this hole (Solitary, Social, Parasitic, Trophallaxis) — ask us if you need more added.'>Socialilty</div>", unsafe_allow_html=True)
            hdr_notes.markdown("<div style='font-weight:600;'>Notes</div>", unsafe_allow_html=True)
        except Exception:
            hdr_c_sb_notes.markdown("**Social behaviours / Notes**")

        # Determine holes for selected hotel (fallback to A-K)
        holes_for_hotel = HOTEL_HOLES.get(hotel_code) if hotel_code else None
        if not holes_for_hotel:
            holes_for_hotel = [chr(i) for i in range(ord('A'), ord('K')+1)]

        # Precompute latest observation per (hotel_code, nest_hole) to avoid repeated filtering and datetime parsing
        latest_by_hotel_hole = {}
        try:
            if not df.empty:
                tmp = df.copy()
                if 'submission_time' in tmp.columns:
                    try:
                        tmp['__st'] = pd.to_datetime(tmp['submission_time'], errors='coerce')
                    except Exception:
                        tmp['__st'] = pd.NaT
                    tmp = tmp.sort_values('__st', na_position='first')
                # Keep last occurrence per obs_id after sorting ensures latest by submission_time wins when deduped
                for _, r in tmp.iterrows():
                    key = (str(r.get('hotel_code', '')), str(r.get('nest_hole', '')))
                    latest_by_hotel_hole[key] = r
        except Exception:
            latest_by_hotel_hole = {}

        for hole_label in holes_for_hotel:
            # split counts into four small columns: cells, males, females, unknowns
            c0, c1, c_counts, c_sb_notes = st.columns([1, 4, 4, 4])
            # inside the counts column, create sub-columns
            cnt_cells_col, cnt_m_col, cnt_f_col, cnt_u_col = c_counts.columns([1, 1, 1, 1])
            # (Per-row headers removed - consolidated header is rendered once above the grid)
            # inside the social/notes column, create two sub-columns for social_behaviour and notes
            sb_col, hole_notes_col = c_sb_notes.columns([2, 2])

            # Prepopulate defaults if possible (use authoritative df and pick most recent by submission_time)
            defaults = {"scientific_name": "", "num_males": 0, "num_females": 0, "social_behaviour": []}
            try:
                if hotel_code:
                    key = (str(hotel_code), str(hole_label))
                    last_entry = latest_by_hotel_hole.get(key)
                    if last_entry is not None:
                        defaults = {
                            "scientific_name": last_entry.get("scientific_name", ""),
                            "num_males": int(last_entry.get("num_males", 0) or 0),
                            "num_females": int(last_entry.get("num_females", 0) or 0),
                            "social_behaviour": last_entry.get("social_behaviour", "").split(", ") if last_entry.get("social_behaviour") else []
                        }
            except Exception:
                pass

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
            with cnt_cells_col:
                ncells = st.number_input(f"cells for {hole_label}", min_value=0, step=1, value=int(defaults.get("num_cells", 0) or 0), key=f"cells_{hole_label}", label_visibility='collapsed')
            with cnt_m_col:
                nm = st.number_input(f"males for {hole_label}", min_value=0, step=1, value=defaults["num_males"], key=f"males_{hole_label}", label_visibility='collapsed')
            with cnt_f_col:
                nf = st.number_input(f"females for {hole_label}", min_value=0, step=1, value=defaults["num_females"], key=f"fem_{hole_label}", label_visibility='collapsed')
            with cnt_u_col:
                nu = st.number_input(f"unknowns for {hole_label}", min_value=0, step=1, value=int(defaults.get("num_unknowns", 0) or 0), key=f"unk_{hole_label}", label_visibility='collapsed')
            with sb_col:
                sb = st.multiselect(f"social_behaviour for {hole_label}", ["Solitary", "Social", "Parasitic", "Trophallaxis"], default=defaults["social_behaviour"], key=f"sb_{hole_label}", label_visibility='collapsed')
            with hole_notes_col:
                notes = st.text_input(f"notes for {hole_label}", key=f"notes_{hole_label}", label_visibility='collapsed')

            hole_values[hole_label] = {
                "scientific_name": sci,
                "num_cells": ncells,
                "num_males": nm,
                "num_females": nf,
                "num_unknowns": nu,
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
        if not photo:
            st.error("⚠️ No photo uploaded. This is required, please go up and upload one!")
            st.stop()
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
                nc = st.session_state.get(f"cells_{hole_label}", 0)
                nm = st.session_state.get(f"males_{hole_label}", 0)
                nf = st.session_state.get(f"fem_{hole_label}", 0)
                nu = st.session_state.get(f"unk_{hole_label}", 0)
                sb = st.session_state.get(f"sb_{hole_label}", []) or []
                notes_text = str(st.session_state.get(f"notes_{hole_label}", "")).strip()

                # Consider a hole 'filled' if it has a scientific name, counts, social behaviour, or notes
                if sci or nm > 0 or nf > 0 or sb or notes_text or nc > 0 or nu > 0:
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
                            "num_cells": nc,
                            "num_males": nm,
                            "num_females": nf,
                            "num_unknowns": nu,
                            "social_behaviour": ", ".join(sb),
                            "notes": notes_text,
                            "submission_notes": notes_submission,
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
                # Save all rows locally at once
                save_observation(rows_to_save, hotel_code, DATA_FILE, dbx)
            else:

                # If NO DATA ARE PROVIDED, CHECK WITH THE USER
                st.info("No hole rows had data to submit. Please fill at least one hole row.")
                    # Ask for user input using a confirmation button
                if st.info("❌ No data are provided, that's okay but please go back up and, for a single hole, select 'Empty' for the Scientific name", key = "emptyQuery"):
                    st.success("Proceeding without any hole row data...")
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
                        "nest_hole": "all_empty",
                        "scientific_name": sci,
                        "num_cells": nc,
                        "num_males": nm,
                        "num_females": nf,
                        "num_unknowns": nu,
                        "social_behaviour": ", ".join(sb),
                        "notes": notes_text,
                        "submission_notes": notes_submission,
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
                    save_observation(rows_to_save, hotel_code, DATA_FILE, dbx)
