from apps.core.memdb import get_memory_engine


def get_mem_engine():
    """Return the shared Postgres-backed memory engine."""

    return get_memory_engine()
