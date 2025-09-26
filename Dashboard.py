import streamlit as st
import pandas as pd
import plotly.express as px
import os
from datetime import datetime, timedelta
import shutil
import json
import dropbox



    # Set up the navigation bar
        # icons at https://fonts.google.com/icons?icon.set=Material+Symbols&icon.style=Rounded&icon.size=24&icon.color=%23e3e3e3
dashPage = st.Page("pages/0_landingPage.py", title="Dashboard", icon = ":material/home:", default=True)
portalPage = st.Page("pages/1_Data portal.py", title="Data portal", icon = ":material/database_upload:")
installPage = st.Page("pages/5_Hotel installation.py", title="Install your hotel", icon = ":material/handyman:")
CheckPage = st.Page("pages/4_Checking your hotel.py", title="Check your hotel", icon = ":material/mystery:")
IDPage = st.Page("pages/7_Bee identification resources.py", title="Identification resources", icon = ":material/frame_bug:")
SpecimenPage = st.Page("pages/6_Collecting specimens.py", title="Collecting specimens", icon = ":material/labs:")
contactPage =  st.Page("pages/3_Contact.py", title="Contact us", icon = ":material/mail:")

pg = st.navigation(
    {
            "": [dashPage, portalPage],
            "Resources": [installPage, CheckPage, IDPage, SpecimenPage],
            " ": [contactPage],
        },
)
pg.run()
