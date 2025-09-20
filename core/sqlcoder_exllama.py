import inspect
import logging
import os
from typing import List, Optional

logger = logging.getLogger("core.sqlcoder_exllama")

try:
    from exllamav2 import ExLlamaV2, ExLlamaV2Cache, ExLlamaV2Config
except Exception as exc:  # pragma: no cover - propagate import failure
    raise

try:
    from exllamav2 import ExLlamaV2Tokenizer
except Exception:  # pragma: no cover - fallback for older builds
    from exllamav2.tokenizer import ExLlamaV2Tokenizer  # type: ignore[attr-defined]

try:
    from exllamav2.generator.base import ExLlamaV2BaseGenerator
except Exception:  # pragma: no cover - fallback for alternate layout
    from exllamav2.generator import ExLlamaV2BaseGenerator  # type: ignore[attr-defined]


def _make_sampler_settings(temperature: float, top_p: float):
    """Return a sampler/settings object compatible with installed exllamav2."""

    settings = None
    try:  # pragma: no cover - new sampler location
        from exllamav2.generator.sampler import SamplerSettings

        settings = SamplerSettings()
    except Exception:
        try:  # pragma: no cover - legacy sampler location
            from exllamav2.generator.settings import (  # type: ignore[attr-defined]
                ExLlamaV2SamplerSettings as SamplerSettings,
            )

            settings = SamplerSettings()
        except Exception:
            settings = None
    if settings is not None:
        try:
            if hasattr(settings, "temperature"):
                settings.temperature = float(temperature)
            if hasattr(settings, "top_p"):
                settings.top_p = float(top_p)
        except Exception:  # pragma: no cover - tolerate exotic sampler APIs
            pass
    return settings


class SQLCoderExLlama:
    """Wrapper ensuring stable ExLlamaV2 generation behaviour across versions."""

    def __init__(self, model, tokenizer, generator, cache, cfg) -> None:
        self._model = model
        self._tokenizer = tokenizer
        self._generator = generator
        self._cache = cache
        self._cfg = cfg

    def generate(
        self,
        prompt: str,
        max_new_tokens: int = 256,
        stop: Optional[List[str]] = None,
        temperature: Optional[float] = None,
        top_p: Optional[float] = None,
        **_ignored,
    ) -> str:
        # Build sampler/settings (if supported by installed version)
        temp_val = float(temperature) if temperature is not None else float(os.getenv("GENERATION_TEMPERATURE", "0.2"))
        top_p_val = float(top_p) if top_p is not None else float(os.getenv("GENERATION_TOP_P", "0.9"))
        settings = _make_sampler_settings(temp_val, top_p_val)

        # Respect cache length and keep an input reserve
        reserve = int(os.getenv("EXL2_INPUT_RESERVE_TOKENS", "64"))
        try:
            max_seq = getattr(self._cache, "max_seq_len", int(os.getenv("EXL2_CACHE_MAX_SEQ_LEN", "2048")))
        except Exception:
            max_seq = int(os.getenv("EXL2_CACHE_MAX_SEQ_LEN", "2048"))
        try:
            token_ids = self._tokenizer.encode(prompt)
            prompt_tokens = len(token_ids)
        except Exception:
            prompt_tokens = max(1, len(prompt) // 4)
        budget = max_seq - prompt_tokens - reserve
        if budget < 1:
            safe_new = 1
        else:
            safe_new = max(1, min(int(max_new_tokens), budget))

        # Call generate_simple with signature detection
        gen_simple = getattr(self._generator, "generate_simple")
        sig = inspect.signature(gen_simple)
        try:
            if len(sig.parameters) == 2:
                text = gen_simple(prompt, safe_new)
            else:
                if settings is None:
                    settings = _make_sampler_settings(temp_val, top_p_val)
                text = gen_simple(prompt, settings, safe_new)
        except TypeError:
            text = gen_simple(prompt, settings or None, safe_new)

        text = text or ""

        # Local stop-string enforcement independent of generator implementation
        stop_list = list(stop or [])
        stop_list.extend(["```", "</s>"])
        cut = len(text)
        for marker in stop_list:
            idx = text.find(marker)
            if idx != -1:
                cut = min(cut, idx)
        return text[:cut].strip()


def load_exllama_generator(model_path: str) -> SQLCoderExLlama:
    logger.info("[core.sqlcoder_exllama] Loading ExLlamaV2 model: %s", model_path)

    cfg = ExLlamaV2Config()
    cfg.model_dir = os.path.abspath(model_path)
    if hasattr(cfg, "model_path"):
        cfg.model_path = cfg.model_dir

    try:  # pragma: no cover - helper not present everywhere
        split = os.getenv("EXL2_GPU_SPLIT_GB")
        if split:
            cfg.set_auto_map(split)  # type: ignore[attr-defined]
    except Exception:
        pass

    cfg.prepare()

    model = ExLlamaV2(cfg)
    tokenizer = ExLlamaV2Tokenizer(cfg)
    cache = ExLlamaV2Cache(model, max_seq_len=int(os.getenv("EXL2_CACHE_MAX_SEQ_LEN", "2048")))
    generator = ExLlamaV2BaseGenerator(model, cache, tokenizer)
    return SQLCoderExLlama(model, tokenizer, generator, cache, cfg)
