import hashlib

from .settings import Settings


def choose_canary(inquiry_id: int) -> bool:
    settings = Settings()
    enabled = settings.get_bool("DW_RULES_CANARY_ENABLED", default=False, scope="namespace")
    if not enabled:
        return False
    pct = settings.get_int("DW_RULES_CANARY_PERCENT", default=10, scope="namespace")
    if pct is None:
        pct = 10
    pct = max(0, min(100, int(pct)))
    digest = hashlib.sha1(str(inquiry_id).encode()).hexdigest()
    h_val = int(digest, 16)
    return (h_val % 100) < pct


__all__ = ["choose_canary"]
