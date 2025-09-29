def bytes_to_megabytes(size_bytes: int | None, precision: int = 3) -> float | None:
    """
    Convert a file size in bytes to megabytes.
    """
    if size_bytes is None:
        return None
    return round(size_bytes / (1024 * 1024), precision)