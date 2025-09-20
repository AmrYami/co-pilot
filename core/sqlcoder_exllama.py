import os
import logging
from typing import List, Optional

from exllamav2 import ExLlamaV2, ExLlamaV2Config
from exllamav2.tokenizer import ExLlamaV2Tokenizer
from exllamav2.generator import ExLlamaV2Generator

try:  # pragma: no cover - depends on exllamav2 version
    from exllamav2.generator.sampler import ExLlamaV2Sampler  # type: ignore[attr-defined]
except Exception:  # pragma: no cover - fallback for older builds
    try:
        from exllamav2.generator import ExLlamaV2Sampler  # type: ignore[attr-defined]
    except Exception:  # pragma: no cover - no sampler available
        ExLlamaV2Sampler = None  # type: ignore[assignment]

LOG = logging.getLogger("main")

class SQLCoderExL2:
    """
    Thin, robust wrapper around ExLlamaV2 to generate SQL with safe prompt truncation
    and consistent sampler settings across exllamav2 versions.
    """

    def __init__(self, model: ExLlamaV2, tokenizer: ExLlamaV2Tokenizer, generator: ExLlamaV2Generator):
        self._model = model
        self._tokenizer = tokenizer
        self._generator = generator

        # Env knobs
        self._max_new_default = int(os.getenv("GENERATION_MAX_NEW_TOKENS", os.getenv("LLM_MAX_NEW", "192")))
        self._reserve_tokens = int(os.getenv("EXL2_INPUT_RESERVE_TOKENS", "64"))

        # Detect cache length
        cache_len = None
        try:
            cache_len = getattr(generator, "max_seq_len", None)
            if cache_len is None and hasattr(generator, "cache"):
                cache_len = getattr(generator.cache, "max_seq_len", None)
        except Exception:
            cache_len = None
        if cache_len is None:
            cache_len = int(os.getenv("EXL2_CACHE_MAX_SEQ_LEN", "2048"))
        self._cache_max_len = int(cache_len)

        if os.getenv("DW_DEBUG", "0") == "1":
            LOG.info("[sql] exllamav2: cache_max_len=%s, reserve=%s", self._cache_max_len, self._reserve_tokens)

    # ---------- internals ----------

    def _build_settings(self) -> Optional["ExLlamaV2Sampler.Settings"]:
        if ExLlamaV2Sampler is None:
            return None
        try:
            settings = ExLlamaV2Sampler.Settings()
        except Exception:
            return None

        def _set(attr: str, env: str, default: str, cast):
            if not hasattr(settings, attr):
                return
            try:
                value = cast(os.getenv(env, default))
            except Exception:
                value = cast(default)
            setattr(settings, attr, value)

        _set("temperature", "GENERATION_TEMPERATURE", "0.2", float)
        _set("top_p", "GENERATION_TOP_P", "0.9", float)
        _set("top_k", "GENERATION_TOP_K", "0", int)
        _set("min_p", "GENERATION_MIN_P", "0.05", float)
        _set("token_repetition_penalty", "GENERATION_REPEAT_PENALTY", "1.08", float)
        return settings

    def _truncate_prompt(self, prompt: str, max_new: int) -> str:
        """Token-level truncate so prompt + new tokens fit inside cache window."""
        ids = self._tokenizer.encode(prompt)
        max_input = max(8, self._cache_max_len - max_new - self._reserve_tokens)
        if len(ids) > max_input:
            ids = ids[-max_input:]
            truncated = self._tokenizer.decode(ids)
            if os.getenv("DW_DEBUG", "0") == "1":
                LOG.info("[sql] prompt truncated: tokens=%s -> %s (max_input=%s)", len(self._tokenizer.encode(prompt)), len(ids), max_input)
            return truncated
        return prompt

    def _manual_stop(self, text: str, stop: Optional[List[str]] = None) -> str:
        if not stop:
            return text
        cut = len(text)
        for s in stop:
            i = text.find(s)
            if i != -1:
                cut = min(cut, i)
        return text[:cut]

    def _call_generate_simple(self, prompt: str, settings: Optional["ExLlamaV2Sampler.Settings"], max_new: int) -> str:
        """
        Handle both exllamav2 signatures:
          - new: generate_simple(prompt, settings, num_tokens)
          - old: generate_simple(prompt, num_tokens)
        """
        if settings is not None:
            try:
                return self._generator.generate_simple(prompt, settings, max_new)
            except TypeError:
                # fall back to 2-arg signature below
                pass

        try:
            return self._generator.generate_simple(prompt, max_new)
        except TypeError:
            if settings is None and ExLlamaV2Sampler is not None:
                try:
                    fallback = ExLlamaV2Sampler.Settings()
                    return self._generator.generate_simple(prompt, fallback, max_new)
                except Exception:
                    pass
            raise

    # ---------- public ----------

    def generate(self, prompt: str, max_new_tokens: Optional[int] = None, stop: Optional[List[str]] = None) -> str:
        max_new = int(max_new_tokens or self._max_new_default)
        prompt2 = self._truncate_prompt(prompt, max_new)
        settings = self._build_settings()

        if os.getenv("DW_DEBUG", "0") == "1":
            LOG.info("[sql] gen.begin max_new=%s prompt_len=%s", max_new, len(prompt2))

        out = self._call_generate_simple(prompt2, settings, max_new)
        text = out if isinstance(out, str) else str(out)
        text = self._manual_stop(text, stop)

        if os.getenv("DW_DEBUG", "0") == "1":
            LOG.info("[sql] gen.end out_size=%s", len(text))

        return text


# -------- loader --------

def load_exllama_generator(path: str) -> SQLCoderExL2:
    """
    Create model/tokenizer/generator and wrap them in SQLCoderExL2
    """
    LOG.info("Loading model: %s", path)
    cfg = ExLlamaV2Config()
    cfg.model_path = path
    # Lower VRAM / safer defaults if env suggests
    if os.getenv("EXL2_FORCE_BASE", "0") == "1":
        cfg.max_input_len = int(os.getenv("EXL2_CACHE_MAX_SEQ_LEN", "2048"))
    cfg.prepare()

    model = ExLlamaV2(cfg)
    tok = ExLlamaV2Tokenizer(cfg)

    cache_len = int(os.getenv("EXL2_CACHE_MAX_SEQ_LEN", "2048"))
    gen = ExLlamaV2Generator(model, tok, max_seq_len=cache_len)
    gen.warmup()

    LOG.info("ExLlamaV2 ready (cache=%s)", cache_len)
    return SQLCoderExL2(model, tok, gen)
