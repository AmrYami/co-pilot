from __future__ import annotations

from typing import Any

from core.settings import Settings
from core.sql_exec import get_mem_engine
from core.datasources import DatasourceRegistry


class Pipeline:
    """Lightweight pipeline wrapper for DocuWare flows."""

    def __init__(self, settings: Settings | None = None, namespace: str = "dw::common") -> None:
        self.settings = settings or Settings(namespace=namespace)
        self.namespace = namespace

        # Attach namespace to settings so DB reads resolve correctly.
        try:
            self.settings.set_namespace(namespace)
        except AttributeError:
            pass

        # Memory engine (Postgres) used for metadata + settings overrides.
        self.mem = get_mem_engine(self.settings)
        self.mem_engine = self.mem
        try:
            self.settings.attach_mem_engine(self.mem)
        except AttributeError:
            pass

        # Datasource registry (Oracle, etc.).
        self.ds = DatasourceRegistry(self.settings, namespace=self.namespace)

        # Resolve default application engine eagerly for request handlers.
        try:
            self.app_engine = self.ds.engine(None)
        except Exception:
            self.app_engine = None

        # Active app flag retained for compatibility; defaults to DocuWare.
        self.active_app = (self.settings.get("ACTIVE_APP", scope="namespace") or "dw").strip() or "dw"

    # ------------------------------------------------------------------
    def engine(self, name: str | None = None):
        return self.ds.engine(name)

    # Compatibility shims ------------------------------------------------
    def ensure_ingested(self, *args: Any, **kwargs: Any):  # pragma: no cover - legacy stub
        raise NotImplementedError("Ingestion is not supported in the simplified pipeline.")

    def answer(self, *args: Any, **kwargs: Any):  # pragma: no cover - legacy stub
        raise NotImplementedError("LLM answering is not available in the simplified pipeline.")

    def reprocess_inquiry(self, *args: Any, **kwargs: Any):  # pragma: no cover - legacy stub
        raise NotImplementedError("Inquiry reprocessing is not available in the simplified pipeline.")

    def apply_admin_and_retry(self, *args: Any, **kwargs: Any):  # pragma: no cover - legacy stub
        raise NotImplementedError("Admin retry is not available in the simplified pipeline.")
