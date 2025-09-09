DATETIME_FORMATS = ["%d/%m/%Y", "%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%fZ"]

def get_field_value(output: dict, key: str):
    try:
        node = output.get("fields", {}).get(key, {})
        return node.get("value")
    except Exception:
        return None

def to_str_or_none(v):
    return None if v is None else str(v)

def parse_date_or_none(v):
    if not v:
        return None
    s = str(v).strip()
    from datetime import datetime as dt
    for fmt in DATETIME_FORMATS:
        try:
            return dt.strptime(s[:len(fmt)], fmt).date()
        except Exception:
            continue
    return None