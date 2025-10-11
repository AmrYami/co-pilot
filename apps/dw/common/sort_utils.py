def normalize_sort(sort_by: str | None, default_col: str = "REQUEST_DATE"):
    if not sort_by:
        return default_col, True
    s = str(sort_by).upper()
    if s.endswith("_DESC"):
        return s[:-5], True
    if s.endswith("_ASC"):
        return s[:-4], False
    return s, True
