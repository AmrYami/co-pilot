import os
from sqlalchemy import create_engine

_mem_engine = None


def get_mem_engine():
    global _mem_engine
    if _mem_engine is not None:
        return _mem_engine

    # 1) env overrides
    url = os.getenv("MEMORY_DB_URL")

    # 2) fallback to mem_settings (if you have a settings reader, use it)
    if not url:
        try:
            # if you have a settings accessor, use it; otherwise hardcode for now
            # from apps.common.settings import get_setting
            # url = get_setting("MEMORY_DB_URL", scope="global")
            url = "postgresql+psycopg2://postgres:123456789@localhost/copilot_mem_dev"
        except Exception:
            pass

    if not url:
        raise RuntimeError("MEMORY_DB_URL is not configured")

    _mem_engine = create_engine(url, pool_pre_ping=True, future=True)
    return _mem_engine
