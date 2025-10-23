import streamlit as st
import pandas as pd
import plotly.express as px
import os
from datetime import datetime, timedelta
import shutil
import json
import dropbox

def safe_read_csv(path):
    if not os.path.exists(path):
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        # if malformed, move aside and return empty
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        try:
            shutil.move(path, f"{path}.broken_{ts}.bak")
        except Exception:
            pass
        return pd.DataFrame()


def init_dropbox():
    # Load Dropbox credentials: prefer Streamlit secrets, then environment, then local secrets.json
    try:
        # Try Streamlit secrets first
        try:
            app_key = st.secrets.get("DROPBOX_APP_KEY")
            app_secret = st.secrets.get("DROPBOX_APP_SECRET")
            refresh = st.secrets.get("DROPBOX_REFRESH_TOKEN")
        except Exception:
            app_key = app_secret = refresh = None

        # Next, environment variables
        if not (app_key and app_secret and refresh):
            app_key = app_key or os.environ.get("DROPBOX_APP_KEY")
            app_secret = app_secret or os.environ.get("DROPBOX_APP_SECRET")
            refresh = refresh or os.environ.get("DROPBOX_REFRESH_TOKEN")

        # Fallback to local secrets.json
        if not (app_key and app_secret and refresh) and os.path.exists("secrets.json"):
            try:
                with open("secrets.json") as f:
                    s = json.load(f)
                app_key = app_key or s.get("DROPBOX_APP_KEY")
                app_secret = app_secret or s.get("DROPBOX_APP_SECRET")
                refresh = refresh or s.get("DROPBOX_REFRESH_TOKEN")
            except Exception:
                pass

        if app_key and app_secret and refresh:
            return dropbox.Dropbox(app_key=app_key, app_secret=app_secret, oauth2_refresh_token=refresh)
    except Exception:
        pass
    return None


def load_observations_from_dropbox(dbx):
    # Try common paths for the master observations file in the Dropbox root
    candidate_paths = ["/observations/observations.csv", "/observations.csv", "/observations/observations_master.csv"]
    for p in candidate_paths:
        try:
            md = dbx.files_get_metadata(p)
            _, res = dbx.files_download(p)
            content = res.content.decode("utf-8")
            from io import StringIO
            return pd.read_csv(StringIO(content))
        except Exception:
            continue
    return pd.DataFrame()


def ensure_photo_links(dbx, df):
    # If photo_link is missing, try to locate files in /observations/photos/ and create shared links
    if dbx is None or df.empty:
        return df
    # Normalize existing Dropbox sharing links (replace dl=0 / dl=1 with raw=1)
    try:
        if 'photo_link' in df.columns:
            def _normalize_link(url):
                if not isinstance(url, str) or not url:
                    return url
                if 'dropbox.com' in url and 'raw=1' not in url:
                    return url.replace('?dl=0', '?raw=1').replace('?dl=1', '?raw=1').replace('&dl=0', '&raw=1').replace('&dl=1', '&raw=1')
                return url
            df['photo_link'] = df['photo_link'].apply(_normalize_link)
    except Exception:
        pass
    try:
        # list files in photos folder
        photos = {}
        try:
            res = dbx.files_list_folder('/observations/photos')
            entries = res.entries
            while res.has_more:
                res = dbx.files_list_folder_continue(res.cursor)
                entries.extend(res.entries)
        except Exception:
            entries = []

        for e in entries:
            if hasattr(e, 'name'):
                photos[e.name] = e.path_lower

        # Map missing or non-raw Dropbox photo_link rows by looking for files that start with submission_id or obs_id
        for idx, row in df.iterrows():
            current_link = row.get('photo_link')
            needs_resolution = False
            if not current_link or (isinstance(current_link, str) and 'dropbox.com' in current_link and 'raw=1' not in current_link):
                needs_resolution = True
            if not needs_resolution:
                continue

            # try obs_id then submission_id to find a file in photos
            found = None
            for key in ('submission_id', 'obs_id'):
                val = row.get(key)
                if not val:
                    continue
                prefix = f"{val}_"
                matches = [name for name in photos.keys() if name.startswith(prefix)]
                if matches:
                    found = photos[matches[0]]
                    break

            if found:
                # Prefer a temporary direct link (files_get_temporary_link) which Streamlit can load reliably
                try:
                    tmp = dbx.files_get_temporary_link(found)
                    if hasattr(tmp, 'link'):
                        df.at[idx, 'photo_link'] = tmp.link
                        continue
                    # older SDK might return the url attribute
                    if hasattr(tmp, 'url'):
                        df.at[idx, 'photo_link'] = tmp.url
                        continue
                except Exception:
                    pass
                # Fallback to shared links if temporary link not available
                try:
                    sl = dbx.sharing_create_shared_link_with_settings(found)
                    link = sl.url.replace('?dl=0', '?raw=1').replace('?dl=1', '?raw=1').replace('&dl=0', '&raw=1').replace('&dl=1', '&raw=1')
                    df.at[idx, 'photo_link'] = link
                    continue
                except Exception:
                    pass
                try:
                    lst = dbx.sharing_list_shared_links(found, direct_only=True)
                    if lst.links:
                        link = lst.links[0].url.replace('?dl=0', '?raw=1').replace('?dl=1', '?raw=1').replace('&dl=0', '&raw=1').replace('&dl=1', '&raw=1')
                        df.at[idx, 'photo_link'] = link
                        continue
                except Exception:
                    pass

            # If we couldn't find a file by submission_id, but there's an existing Dropbox share link in the CSV,
            # try to normalize it (best-effort) so Streamlit can fetch it.
            if isinstance(current_link, str) and 'dropbox.com' in current_link:
                try:
                    normalized = current_link.replace('?dl=0', '?raw=1').replace('?dl=1', '?raw=1').replace('&dl=0', '&raw=1').replace('&dl=1', '&raw=1')
                    df.at[idx, 'photo_link'] = normalized
                except Exception:
                    pass
    except Exception:
        pass
    return df


def load_authoritative_observations(dbx_client):
    """Return authoritative observations DataFrame:
    - If a remote master exists (preferred), download and return it.
    - Otherwise, attempt to list and concatenate CSVs under `/observations/csv/`.
    - Falls back to local `observations.csv` if Dropbox not available.
    """
    # If no Dropbox, fall back to local file
    if dbx_client is None:
        return safe_read_csv('observations.csv')

    # Try master locations first (single file download is cheap)
    candidate_paths = ['/observations/observations.csv', '/observations.csv', '/observations/observations_master.csv']
    for p in candidate_paths:
        try:
            md = dbx_client.files_get_metadata(p)
            _, res = dbx_client.files_download(p)
            content = res.content.decode('utf-8')
            from io import StringIO
            df = pd.read_csv(StringIO(content))
            if not df.empty:
                return df
        except Exception:
            continue

    # If no master found, try to reconstruct by concatenating per-observation CSVs
    pieces = []
    try:
        try:
            res = dbx_client.files_list_folder('/observations/csv')
            entries = res.entries
            while getattr(res, 'has_more', False):
                res = dbx_client.files_list_folder_continue(res.cursor)
                entries.extend(res.entries)
        except Exception:
            entries = []

        for e in entries:
            try:
                name = getattr(e, 'name', '')
                if name.lower().endswith('.csv'):
                    _, r = dbx_client.files_download(f"/observations/csv/{name}")
                    txt = r.content.decode('utf-8')
                    try:
                        pieces.append(pd.read_csv(StringIO(txt)))
                    except Exception:
                        continue
            except Exception:
                continue
    except Exception:
        pass

    if pieces:
        try:
            combined = pd.concat(pieces, ignore_index=True, sort=False)
            # dedupe by obs_id preferring latest by submission_time
            if 'obs_id' in combined.columns:
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
            return combined
        except Exception:
            pass

    # Fallback to local file
    return safe_read_csv('observations.csv')

# ---- App Config ----
st.set_page_config(
    page_title="Bee Box",
    page_icon="🐝",
    layout="wide"
)

# ---- Load Data at Startup ----
# Initialize Dropbox and load authoritative observations once at startup
dbx = init_dropbox()
obs_df = load_authoritative_observations(dbx)

# ---- Landing Page ----
st.title("🐝 Welcome to the bee hotel project!")
st.write("""
This is the landing page for our wonderful contributors and collaborators to enter their data and to seek some helpful documentation for what to do.
""")
st.write(""" 
The bee hotel project is a collaboration involving both citizen scientists and professional (or retired) scientists and entomologists looking to better understand what our lovely native bees do at home. We are starting simple and looking at things like sociality, activity periods, parasitism and so forth. We expect that the site will grow slightly as questions get asked and needs arise… certainly we will fill these pages with more information and statistics.
""")
st.write(""" 
Keep in mind that this is very much in development... But, for now, happy observing, Bee Nerds! 
""")


# ---- Dashboard ----

# Placeholder demo data

# Species visualization from observations (dropbox master preferred)
try:
    if not obs_df.empty and 'scientific_name' in obs_df.columns:
        sp = obs_df['scientific_name'].fillna('Unknown')
        sp_counts = sp.value_counts().reset_index()
        sp_counts.columns = ['Species', 'Observations']
        sp_counts = sp_counts[sp_counts['scientific_name'] != "Empty"]
        # Bee-inspired palette (yellows and black)
        bee_colors = ['#F6C85F', '#F4A460', '#E07A3C', '#B5651D', '#3A3A3A']
        fig = px.bar(sp_counts, x='Observations', y='Species', orientation='h', color='Observations', color_continuous_scale=['#FFF1C9', '#F6C85F', '#E07A3C', '#B5651D', '#3A3A3A'])
        fig.update_layout(yaxis={'categoryorder':'total ascending'}, coloraxis_showscale=False, plot_bgcolor='white', margin=dict(l=10, r=10, t=40, b=20))
        # store species figure to render after KPI in a two-column layout
        species_fig = fig
    else:
        species_fig = None
        st.info('No species data available yet to build species visualization.')
except Exception as e:
    st.warning(f'Failed to build species visualization: {e}')

st.markdown("---")

# KPI row to make the dashboard more engaging
try:
    total_submissions = 0
    unique_observers = 0
    total_bees = 0
    if not obs_df.empty:
        if 'submission_id' in obs_df.columns:
            total_submissions = obs_df['submission_id'].dropna().unique().size
        else:
            total_submissions = obs_df['obs_id'].dropna().unique().size if 'obs_id' in obs_df.columns else 0
        unique_observers = obs_df['observer'].nunique()
        # compute total bees observed as sum of num_males + num_females (defensive numeric conversion)
        try:
            males = pd.to_numeric(obs_df.get('num_males', pd.Series(dtype=float)), errors='coerce').fillna(0).sum()
            females = pd.to_numeric(obs_df.get('num_females', pd.Series(dtype=float)), errors='coerce').fillna(0).sum()
            total_bees = int(males + females)
        except Exception:
            total_bees = 0

    # Render KPI cards with simple styling
    c1, c2, c3 = st.columns([1,1,1])
    c1.markdown(f"<div style='background:#FFF7E6;padding:16px;border-radius:8px;text-align:center;'><div style='font-size:20px;font-weight:700'>{total_submissions}</div><div style='color:#666'>Total submissions</div></div>", unsafe_allow_html=True)
    c2.markdown(f"<div style='background:#FFF7E6;padding:16px;border-radius:8px;text-align:center;'><div style='font-size:20px;font-weight:700'>{unique_observers}</div><div style='color:#666'>Unique observers</div></div>", unsafe_allow_html=True)
    c3.markdown(f"<div style='background:#FFF7E6;padding:16px;border-radius:8px;text-align:center;'><div style='font-size:20px;font-weight:700'>{total_bees}</div><div style='color:#666'>Bees observed</div></div>", unsafe_allow_html=True)
except Exception:
    pass
# --- Leaderboard & Species two-column layout ---
# Build leaderboard for the last 7 days and render alongside the species chart

# Initialize leaderboard_html to avoid undefined variable error
leaderboard_html = "<p>Leaderboard currently unavailable.</p>"

try:
    if obs_df.empty:
        st.info("No observations yet — leaderboard will populate as data arrives.")
    else:
        # last 7 days leaderboard
        N = 7
        today = datetime.now().date()
        days = [(today - timedelta(days=i)) for i in range(N-1, -1, -1)]
        if "obs_date" in obs_df.columns:
            try:
                obs_df["obs_date_parsed"] = pd.to_datetime(obs_df["obs_date"]).dt.date
            except Exception:
                obs_df["obs_date_parsed"] = None
        else:
            obs_df["obs_date_parsed"] = None

        # compute counts per observer per day
        observers = sorted(obs_df["observer"].dropna().unique().tolist())
        leaderboard_rows = []
        for obs in observers:
            row = {"Observer": obs}
            total = 0
            for d in days:
                try:
                    subset = obs_df[(obs_df["observer"] == obs) & (obs_df["obs_date_parsed"] == d)]
                    # count unique submissions per day (use submission_id, fallback to obs_id)
                    if 'submission_id' in subset.columns:
                        cnt = subset['submission_id'].dropna().unique().size
                        # if submission_id missing, fallback to obs_id
                        if cnt == 0 and 'obs_id' in subset.columns:
                            cnt = subset['obs_id'].dropna().unique().size
                    else:
                        cnt = subset['obs_id'].dropna().unique().size if 'obs_id' in subset.columns else 0
                except Exception:
                    cnt = 0
                # day columns show a star if there was at least one unique submission that day
                row[d.strftime("%Y-%m-%d")] = "★" if cnt > 0 else ""
                total += cnt
            row["Total"] = total
            leaderboard_rows.append(row)

        lb_df = pd.DataFrame(leaderboard_rows).set_index('Observer')
        # sort by Total descending
        if 'Total' in lb_df.columns:
            lb_df = lb_df.sort_values(by='Total', ascending=False)

        # Reorder columns: Total first, then dates oldest->newest
        date_cols = [d.strftime("%Y-%m-%d") for d in days]
        cols = ['Total'] + date_cols
        # ensure cols exist and render an HTML table so day stars can be styled
        existing_cols = [c for c in cols if c in lb_df.columns]
        # Build HTML table with column widths following ratio Observer:Total:days = 5:2:1..1
        table_html = []
        table_html.append("<table style='width:100%;border-collapse:collapse;'>")
        # compute dynamic widths based on how many day columns we actually have
        days_count = len([c for c in existing_cols if c != 'Total'])
        total_ratio = 5 + 2 + max(0, days_count)
        unit = 100.0 / total_ratio if total_ratio > 0 else 0
        obs_w = round(5 * unit, 2)
        total_w = round(2 * unit, 2)
        day_w = round(1 * unit, 2)
        # header
        table_html.append("<thead><tr>")
        table_html.append(f"<th style='text-align:left;padding:8px;border-bottom:2px solid #ddd;width:{obs_w}%;'>Observer</th>")
        for c in existing_cols:
            if c == 'Total':
                table_html.append(f"<th style='text-align:center;padding:8px;border-bottom:2px solid #ddd;width:{total_w}%;'>{c}</th>")
            else:
                # blank header, show full date on hover via title; width based on ratio
                table_html.append(f"<th title='{c}' style='text-align:center;padding:4px;border-bottom:2px solid #ddd;width:{day_w}%;'>&nbsp;</th>")
        table_html.append("</tr></thead>")
        table_html.append("<tbody>")
        # rows
        for idx, row in lb_df[existing_cols].iterrows():
            table_html.append("<tr>")
            # Observer name cell
            table_html.append(f"<td style='text-align:left;padding:8px;border-bottom:1px solid #eee;font-weight:600;width:{obs_w}%;'>{idx}</td>")
            for c in existing_cols:
                val = row.get(c, "")
                if c == 'Total':
                    # numeric total with width
                    table_html.append(f"<td style='text-align:center;padding:8px;border-bottom:1px solid #eee;width:{total_w}%;'>{int(val) if pd.notna(val) and str(val)!="" else 0}</td>")
                else:
                    # day columns: show a large gold star if non-empty; keep narrow width
                    if pd.notna(val) and str(val).strip() != "":
                        star = "<span style='color:#FFD700;font-size:20px;line-height:1;'>★</span>"
                        table_html.append(f"<td style='text-align:center;padding:6px;border-bottom:1px solid #eee;width:{day_w}%;'>{star}</td>")
                    else:
                        table_html.append(f"<td style='text-align:center;padding:6px;border-bottom:1px solid #eee;width:{day_w}%;'></td>")
            table_html.append("</tr>")
        table_html.append("</tbody></table>")
        leaderboard_html = ''.join(table_html)

        # Now render species figure (left) and leaderboard HTML (right) evenly
        left, right = st.columns([1, 1])
        with left:
            st.subheader("🧬 Species")
            if 'species_fig' in locals() and species_fig is not None:
                st.plotly_chart(species_fig, use_container_width=True)
            else:
                st.info('No species data available yet to build species visualization.')
        with right:
            # Social-behaviour section title (emoji to match others)
            st.subheader("🤝 Social behaviour")
            # Small social-behaviour pie chart above the leaderboard using bee palette
            try:
                if not obs_df.empty and 'social_behaviour' in obs_df.columns:
                    sb = obs_df['social_behaviour'].fillna('Unknown')
                    sb_counts = sb.value_counts().reset_index()
                    sb_counts.columns = ['social_behaviour', 'count']
                    sb_counts = sb_counts[sb_counts['social_behaviour'] != "Unknown"]
                    if not sb_counts.empty:
                        bee_palette = ['#FFF1C9', '#F6C85F', '#E07A3C', '#B5651D', '#3A3A3A']
                        pie = px.pie(sb_counts, names='social_behaviour', values='count', hole=0.35, color='social_behaviour', color_discrete_sequence=bee_palette)
                        pie.update_traces(textposition='inside', textinfo='percent+label', hoverinfo='label+value')
                        pie.update_layout(margin=dict(l=10, r=10, t=10, b=10), showlegend=False)
                        st.plotly_chart(pie, use_container_width=True)
                else:
                    st.info('No social behaviour data yet.')
            except Exception:
                st.info('Social behaviour chart unavailable.')
            # Leaderboard will be shown full-width below (above gallery)
            pass
except Exception:
    # If leaderboard fails, fall back to showing a placeholder and continue
    st.info('Leaderboard currently unavailable.')
    leaderboard_html = "<p>Leaderboard currently unavailable.</p>"

# --- Leaderboard (full-width) ---
st.subheader("🏆 Leaderboard")
st.markdown(leaderboard_html, unsafe_allow_html=True)

# --- Recent Images Gallery ---
st.subheader("📸 Recent Images")
# Use the data already loaded at startup
if obs_df.empty or "photo_link" not in obs_df.columns:
    st.info("No images found yet.")
else:
    # show latest 12 images with caption
    # Ensure photo links are present by resolving from Dropbox if necessary
    obs_df = ensure_photo_links(dbx, obs_df)
    # Deduplicate by submission_id so one image per submission (fallback to obs_id)
    img_df = obs_df.dropna(subset=["photo_link"]).copy()
    # use submission_id if present, else obs_id
    img_df['submission_match_id'] = img_df['submission_id'].fillna(img_df.get('obs_id', ''))
    # keep latest row per submission_match_id
    img_df = img_df.sort_values(by='submission_time', ascending=False).drop_duplicates(subset=['submission_match_id'], keep='first')
    recent = img_df.head(12)
    cols = st.columns(4)
    for i, (_, row) in enumerate(recent.iterrows()):
        c = cols[i % 4]
        try:
            c.image(row["photo_link"], width='stretch', caption=f"{row.get('observer','')} — {row.get('hotel_code','')} / {row.get('nest_hole','')}")
        except Exception:
            c.write("[Image unavailable]")

st.markdown("---")

# ---- Footer ----
st.markdown("<div style='text-align:center'><em>Questions? Visit the Contact page in the sidebar to get in touch!</em></div>", unsafe_allow_html=True)

