# db_loader.py  ─ one-path loader
import os, sqlite3, pandas as pd

GDRIVE_FILE_ID   = "1xvleAGsC8qJnM8Kim5MEAG96-2nhcAxw"   # snapshot folder/file ID
LOCAL_DB_PATH    = "whatsapp_conversations.db"           # always the same name
TABLE            = "deepseek_results"                    # what the UI expects

def _download_from_drive(dest: str):
    import gdown, pathlib, shutil, os, tempfile

    tmp_dir = tempfile.mkdtemp()
    gdown.download_folder(
        id="1xvleAGsC8qJnM8Kim5MEAG96-2nhcAxw",        # folder id
        output=tmp_dir,
        quiet=False,
        use_cookies=False
    )
    # pick newest *.db
    newest = max(pathlib.Path(tmp_dir).glob("*.db"), key=os.path.getmtime)
    shutil.move(newest, dest)
    shutil.rmtree(tmp_dir)


def _ensure_db() -> str:
    """
    Return a local path to an up-to-date DB file.
    • If file exists ⇒ use it
    • If not ⇒ fetch from Drive (works locally & on Streamlit Cloud)
    """
    # On Streamlit Cloud write to /tmp; locally write beside the script
    path = "/tmp/" + LOCAL_DB_PATH if os.getenv("STREMLIT_SERVER_HEADLESS") else LOCAL_DB_PATH
    if not os.path.isfile(path):
        _download_from_drive(path)
    return path

def get_dataframe() -> pd.DataFrame:
    """Load <TABLE> into a DataFrame (guarantees column names untouched)."""
    db_path = _ensure_db()
    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql_query(f"SELECT * FROM {TABLE}", conn)

    # Normalise that one rogue column
    if "OBITO PROVAVEL" in df.columns and "OBITO_PROVAVEL" not in df.columns:
        df = df.rename(columns={"OBITO PROVAVEL": "OBITO_PROVAVEL"})
    return df
