def update_sheet(changes: list[dict]) -> list[int]:
    """
    changes: [{id: int, field: str, new_value: Any}, …]
    Returns list of row-IDs successfully updated.
    """
    # TODO: call gspread / Google Drive API
    return [c["id"] for c in changes]
