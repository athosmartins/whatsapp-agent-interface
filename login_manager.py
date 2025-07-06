# login_manager.py – authentication with cookie persistence
# -----------------------------------------------------------------------------
import streamlit as st
import extra_streamlit_components as stx
import time


def simple_auth() -> bool:
    """
    Authentication with cookie persistence.
    Returns True if authenticated, False otherwise.
    """

    # Initialize cookie manager
    cookie_manager = stx.CookieManager(key="cm_auth")

    # Pull the dict of allowed users out of secrets.toml
    USERS = st.secrets["auth_users"]

    # Initialize session state
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
        st.session_state.username = None

    # IMPORTANT: on the very first run we must mount the cookie components
    # so that get() actually returns the real browser values on the *next* run.
    if "cookies_checked" not in st.session_state:
        st.session_state.cookies_checked = True
        # prime those components
        cookie_manager.get("urb_link_authenticated")
        cookie_manager.get("urb_link_username")
        # now rerun so those values show up below
        st.experimental_rerun()

    # Check cookie authentication FIRST (before session state)
    auth_cookie = cookie_manager.get("urb_link_authenticated")
    username_cookie = cookie_manager.get("urb_link_username")

    # Restore session from cookies if valid
    if auth_cookie == "1" and username_cookie and not st.session_state.authenticated:
        # Validate the username from cookie is still valid
        if username_cookie in USERS:
            st.session_state.authenticated = True
            st.session_state.username = username_cookie
            # Don't show login form, go straight to the app
            return True

    # If already authenticated via session, return True
    if st.session_state.authenticated:
        return True

    # Show login form only if not authenticated
    st.title("🔐 WhatsApp Agent Login")

    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        with st.form("login_form", clear_on_submit=False):
            user = st.text_input("Username")
            pwd = st.text_input("Password", type="password")

            submitted = st.form_submit_button("Login", use_container_width=True)

            if submitted:
                if user and USERS.get(user) == pwd:
                    # Set session state
                    st.session_state.authenticated = True
                    st.session_state.username = user

                    # CRITICAL: Set cookies BEFORE rerun
                    # unique key per component call avoids collisions
                    cookie_manager.set(
                        "urb_link_authenticated", "1",
                        max_age=3600 * 24 * 30,
                        key="set_auth_cookie",
                        path="/",
                        same_site="Lax",
                        secure=False,
                    )
                    cookie_manager.set(
                        "urb_link_username", user,
                        max_age=3600 * 24 * 30,
                        key="set_user_cookie",
                        path="/",
                        same_site="Lax",
                        secure=False,
                    )


                    st.success("✅ Login successful!")
                    time.sleep(0.5)  # Give time for cookies to be set
                    st.rerun()
                else:
                    st.error("❌ Invalid username or password")

        st.info("Ask your admin for credentials")

    return False


def handle_logout():
    """
    Handle logout and clear all auth data.
    Call this from your main app when logout is needed.
    """
    cookie_manager = stx.CookieManager(key="cm_auth")

    # Clear session state
    st.session_state.authenticated = False
    st.session_state.username = None

    # Clear cookies
    cookie_manager.delete("urb_link_authenticated", key="del_auth_cookie")
    cookie_manager.delete("urb_link_username",    key="del_user_cookie")

    # Force a clean rerun
    st.rerun()
