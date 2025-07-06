# login_manager.py – optional authentication module for WhatsApp Agent Interface
# -----------------------------------------------------------------------------
import streamlit as st

def simple_auth() -> bool:
    """
    Minimal auth. Reads users/passwords from st.secrets["auth_users"].
    Returns True if already authenticated, else shows login form and returns False.
    """

    # Pull the dict of allowed users out of secrets.toml
    USERS = st.secrets["auth_users"]

    # Initialize session‐state flags
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
        st.session_state.username      = None

    # If not yet authenticated, show the login form
    if not st.session_state.authenticated:
        st.title("🔐 WhatsApp Agent Login")

        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            with st.form("login_form"):
                user = st.text_input("Username")
                pwd  = st.text_input("Password", type="password")
                if st.form_submit_button("Login", use_container_width=True):
                    if USERS.get(user) == pwd:
                        st.session_state.authenticated = True
                        st.session_state.username      = user
                        st.success("✅ Login successful! Redirecting…")
                        st.balloons()
                        st.rerun()
                    else:
                        st.error("❌ Invalid username or password")

        st.info("Ask your admin for credentials")
        return False

    # If already authenticated, render a “logout” button in the sidebar
    with st.sidebar:
        st.write(f"👋 Welcome **{st.session_state.username}**")
        if st.button("🚪 Logout", use_container_width=True):
            st.session_state.authenticated = False
            st.session_state.username      = None
            st.rerun()

    return True
