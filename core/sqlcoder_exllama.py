import importlib
import logging
import os
from typing import List, Optional

logger = logging.getLogger(__name__)

try:  # pragma: no cover - depends on exllamav2 install
    from exllamav2.generator import ExLlamaV2BaseGenerator  # type: ignore[attr-defined]
except Exception:  # pragma: no cover - fallback for older builds
    ExLlamaV2BaseGenerator = None  # type: ignore[assignment]

try:  # pragma: no cover - prefer modern sampler location
    from exllamav2.generator import ExLlamaV2Sampler  # type: ignore[attr-defined]
except Exception:  # pragma: no cover - fallback to legacy path or absence
    try:
        from exllamav2.generator.sampler import ExLlamaV2Sampler  # type: ignore[attr-defined]
    except Exception:  # pragma: no cover - sampler missing entirely
        ExLlamaV2Sampler = None  # type: ignore[assignment]

try:  # pragma: no cover - legacy generator alias
    from exllamav2.generator import ExLlamaV2Generator as _LegacyGenerator  # type: ignore[attr-defined]
except Exception:  # pragma: no cover - not available on newer builds
    _LegacyGenerator = None  # type: ignore[assignment]


class ExLlamaSqlGenerator:
    """Version-tolerant SQL generator wrapper for ExLlamaV2."""

    def __init__(self, generator: "ExLlamaV2BaseGenerator") -> None:
        self._generator = generator
        self.temperature = float(os.getenv("GENERATION_TEMPERATURE", "0.2"))
        self.top_p = float(os.getenv("GENERATION_TOP_P", "0.9"))
        self.max_seq_env = int(os.getenv("EXL2_CACHE_MAX_SEQ_LEN", "2048"))
        self.reserve_env = int(os.getenv("EXL2_INPUT_RESERVE_TOKENS", "64"))

    # ------------------------ internal helpers ------------------------

    def _truncate_prompt(self, prompt: str, max_new_tokens: int) -> str:
        """Approximate prompt truncation to avoid cache overflows."""
        max_in_tokens = max(64, self.max_seq_env - self.reserve_env - max_new_tokens)
        max_chars = max(1024, max_in_tokens * 4)
        if len(prompt) > max_chars:
            return prompt[-max_chars:]
        return prompt

    def _make_settings(self):
        if ExLlamaV2Sampler is None:
            return None
        try:
            settings = ExLlamaV2Sampler.Settings()
        except Exception:  # pragma: no cover - sampler lacks Settings
            return None
        if hasattr(settings, "temperature"):
            settings.temperature = self.temperature
        if hasattr(settings, "top_p"):
            settings.top_p = self.top_p
        return settings

    @staticmethod
    def _apply_stops(text: str, stops: Optional[List[str]]) -> str:
        if not stops:
            return text
        cut = len(text)
        for stop in stops:
            if not stop:
                continue
            idx = text.find(stop)
            if idx != -1:
                cut = min(cut, idx)
        return text[:cut]

    def _call_generate(self, prompt: str, settings, max_new_tokens: int) -> str:
        # Prefer modern (prompt, settings, num_tokens) signature
        if settings is not None:
            try:
                return self._generator.generate_simple(prompt, settings, max_new_tokens)
            except TypeError:
                pass
        # Try two-argument signature (prompt, num_tokens)
        try:
            return self._generator.generate_simple(prompt, max_new_tokens)
        except TypeError:
            # Final fallback for legacy (prompt, None, num_tokens)
            return self._generator.generate_simple(prompt, None, max_new_tokens)

    # ----------------------------- public -----------------------------

    def generate(
        self,
        prompt: str,
        max_new_tokens: int = 256,
        stop: Optional[List[str]] = None,
    ) -> str:
        prompt = self._truncate_prompt(prompt, max_new_tokens)
        settings = self._make_settings()

        if os.getenv("DW_DEBUG", "0") == "1":
            logger.info(
                "[sql] exllamav2.generate start max_new=%s prompt_len=%s",
                max_new_tokens,
                len(prompt),
            )

        out = self._call_generate(prompt, settings, max_new_tokens)
        text = out if isinstance(out, str) else str(out)
        text = self._apply_stops(text, stop)

        if os.getenv("DW_DEBUG", "0") == "1":
            logger.info("[sql] exllamav2.generate end out_len=%s", len(text))

        return text


# ------------------------------- loader -------------------------------


def _resolve_tokenizer(cfg):
    tok_mod = importlib.import_module("exllamav2.tokenizer")
    candidates = [
        "ExLlamaV2Tokenizer",
        "ExLlamaV2TokenizerHF",
        "Tokenizer",
    ]
    last_exc: Exception | None = None
    for name in candidates:
        tok_cls = getattr(tok_mod, name, None)
        if tok_cls is None:
            continue
        try:
            return tok_cls(cfg)
        except Exception as exc:  # pragma: no cover - signature mismatch
            last_exc = exc
            continue
    if last_exc is not None:
        raise RuntimeError("No compatible tokenizer class available") from last_exc
    raise RuntimeError("No tokenizer class found in exllamav2.tokenizer")


def _build_generator(model, tokenizer, cache_len: int):
    if ExLlamaV2BaseGenerator is not None:
        try:
            gen = ExLlamaV2BaseGenerator(model, tokenizer, max_seq_len=cache_len)
        except TypeError:  # pragma: no cover - signature differences
            gen = ExLlamaV2BaseGenerator(model, tokenizer)
            if hasattr(gen, "set_max_seq_len"):
                try:
                    gen.set_max_seq_len(cache_len)
                except Exception:
                    pass
            elif hasattr(gen, "max_seq_len") and not getattr(gen, "max_seq_len", None):
                try:
                    setattr(gen, "max_seq_len", cache_len)
                except Exception:
                    pass
        return gen
    if _LegacyGenerator is not None:
        try:
            return _LegacyGenerator(model, tokenizer, max_seq_len=cache_len)
        except TypeError:  # pragma: no cover - old signature without kwarg
            return _LegacyGenerator(model, tokenizer, cache_len)
    raise RuntimeError("No compatible ExLlama generator class available")


def load_exllama_generator(model_path: str) -> ExLlamaSqlGenerator:
    """Create a base generator and wrap it in :class:`ExLlamaSqlGenerator`."""
    logger.info("Loading ExLlamaV2 model: %s", model_path)

    from exllamav2 import ExLlamaV2, ExLlamaV2Config  # type: ignore import

    cfg = ExLlamaV2Config()
    cfg.model_path = model_path

    cache_len = int(os.getenv("EXL2_CACHE_MAX_SEQ_LEN", "2048"))
    if hasattr(cfg, "max_input_len"):
        cfg.max_input_len = cache_len
    cfg.prepare()

    model = ExLlamaV2(cfg)
    tokenizer = _resolve_tokenizer(cfg)
    generator = _build_generator(model, tokenizer, cache_len)

    if hasattr(generator, "warmup"):
        try:
            generator.warmup()
        except Exception:  # pragma: no cover - warmup optional
            pass

    logger.info("ExLlamaV2 generator ready (cache_len=%s)", cache_len)
    return ExLlamaSqlGenerator(generator)
