# login_manager.py – optional authentication module for WhatsApp Agent Interface
# -----------------------------------------------------------------------------
# Import this module from your main `app.py`. Toggle login protection on/off by
# setting the LOGIN_ENABLED flag in `app.py`.

import streamlit as st

# --- simple username/password dict ------------------------------------------------
USERS = {
    "agent1": "agent123",
    "manager": "manager456",
    "admin": "admin789",
}


def simple_auth() -> bool:
    """Minimal, self-contained auth. Returns True if authenticated, else False.

    Usage in app.py:
        from login_manager import simple_auth

        LOGIN_ENABLED = True  # turn off while developing
        if LOGIN_ENABLED and not simple_auth():
            st.stop()
    """

    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
        st.session_state.username = None

    # ------------------------------------------------------------------ LOGIN VIEW
    if not st.session_state.authenticated:
        st.title("🔐 WhatsApp Agent Login")

        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            with st.form("login_form"):
                user = st.text_input("Username", value="agent1")
                pwd = st.text_input("Password", type="password")
                if st.form_submit_button("Login", use_container_width=True):
                    if USERS.get(user) == pwd:
                        st.session_state.authenticated = True
                        st.session_state.username = user
                        st.success("✅ Login successful!  Redirecionando…")
                        st.balloons()
                        st.rerun()
                    else:
                        st.error("❌ Invalid username or password")

        st.info("**Demo:** agent1 / agent123")
        return False  # not yet authenticated

    # -------------------------------------------------------------- SIDEBAR LAYOUT
    with st.sidebar:
        st.write(f"👋 Welcome **{st.session_state.username}**")
        if st.button("🚪 Logout", use_container_width=True):
            st.session_state.authenticated = False
            st.session_state.username = None
            st.rerun()

    return True  # authenticated
