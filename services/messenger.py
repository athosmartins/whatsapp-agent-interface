def send_whatsapp(messages: list[dict]) -> list[int]:
    """
    messages: [{id: int, text: str, to: str}, …]
    Returns list of row-IDs successfully sent.
    """
    # TODO: call your VoxUI WhatsApp API
    return [m["id"] for m in messages]
