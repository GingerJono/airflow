from helpers.utils import get_field_value


def parse_programme_ref_and_year_of_account(
    renewed_from: str,
) -> tuple[str | None, int | None]:
    if not renewed_from:
        return None, None
    token = renewed_from.split(";", 1)[0].strip()
    if len(token) < 9:
        return None, None
    programme_ref = token[:7]
    yy = token[7:9]
    if not yy.isdigit():
        return programme_ref, None
    y = int(yy)
    year = 2000 + y
    return programme_ref, year


def check_is_renewal(output: dict) -> bool:
    val = get_field_value(output, "renewed_from")
    if not val:
        return False
    val_str = str(val).strip()
    if val_str.lower() == "notrenewal":
        return False
    return ";" in val_str
