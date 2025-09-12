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


def safe_parse_date(v):
    """Try to parse a string using known datetime formats; return date or None."""
    if not v:
        return None

    s = str(v).strip()
    from datetime import datetime as dt

    for fmt in DATETIME_FORMATS:
        try:
            return dt.strptime(s[: len(fmt)], fmt).date()
        except Exception:
            continue
    return None
