import streamlit as st

st.title("📩 Contact Us")

st.write("""
Interested in joining the project or have questions?  
We’d love to hear from you!
""")

with st.form("contact_form"):
    name = st.text_input("Your Name")
    email = st.text_input("Your Email")
    message = st.text_area("Your Message")

    submitted = st.form_submit_button("Send Message")

if submitted:
    st.success("✅ Thank you for reaching out! We’ll get back to you soon.")
    # Later: hook into email service or store messages
