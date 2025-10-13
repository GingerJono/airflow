from datetime import datetime

DATETIME_FORMATS = [
    "%d/%m/%Y",
    "%Y-%m-%d",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%fZ",
]


def get_field_value(output: dict, key: str):
    """Safely extract output["fields"][key]["value"], or return None."""
    try:
        node = output.get("fields", {}).get(key, {})
        return node.get("value")
    except Exception:
        return None


def safe_stringify(v):
    """Return str(value) if value is not None, else None."""
    return None if v is None else str(v)


def safe_parse_date(value):
    if not value:
        return None
    if isinstance(value, (list, dict)):
        return None
    s = str(value).strip()
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    # fallback: keep the original text so it isn't lost
    return s


def safe_parse_datetime(value):
    if not value:
        return None
    s = str(value).strip()
    for fmt in ("%d/%m/%Y %H:%M", "%d/%m/%Y %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%dT%H:%M:%S")
        except ValueError:
            continue
    return s

