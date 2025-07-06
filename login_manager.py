# login_manager.py ─ minimal auth, now reading from secrets.toml
import streamlit as st

# --------------------------------------------------------------------------- #
# Load users from secrets.  Fallback to demo users if none defined.
# --------------------------------------------------------------------------- #
USERS = st.secrets["auth_users"]        # <-- must exist in secrets.toml


def simple_auth() -> bool:
    """Return True when the user is authenticated, else False."""
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
        st.session_state.username = None

    # ------------------------------ LOGIN PAGE
    if not st.session_state.authenticated:
        st.title("🔐 WhatsApp Agent Login")
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            with st.form("login"):
                user = st.text_input("Username")
                pwd  = st.text_input("Password", type="password")
                if st.form_submit_button("Login", use_container_width=True):
                    if USERS.get(user) == pwd:
                        st.session_state.authenticated = True
                        st.session_state.username = user
                        st.success("✅ Login successful!  Redirecionando…")
                        st.rerun()
                    else:
                        st.error("❌ Invalid username or password")

        if USERS is DEMO_USERS:
            st.info("**Demo:** agent1 / agent123")
        return False

    # ------------------------------ SIDEBAR WHEN LOGGED IN
    with st.sidebar:
        st.write(f"👋 Welcome **{st.session_state.username}**")
        if st.button("🚪 Logout", use_container_width=True):
            st.session_state.authenticated = False
            st.session_state.username = None
            st.rerun()
    return True
