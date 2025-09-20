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

from exllamav2.generator.sampler import ExLlamaV2Sampler


class SQLCoderExLlama:
    """Wrapper ensuring stable ExLlamaV2 generation behaviour across versions."""

    def __init__(self, model, tokenizer, generator, cache, cfg) -> None:
        self._model = model
        self._tokenizer = tokenizer
        self._generator = generator
        self._cache = cache
        self._cfg = cfg
        self._log = logging.getLogger("core.sqlcoder_exllama")

    def _build_settings(
        self,
        temperature: Optional[float],
        top_p: Optional[float],
    ) -> ExLlamaV2Sampler.Settings:
        settings = ExLlamaV2Sampler.Settings()
        settings.temperature = float(
            temperature if temperature is not None else os.getenv("GENERATION_TEMPERATURE", "0.2")
        )
        settings.top_p = float(top_p if top_p is not None else os.getenv("GENERATION_TOP_P", "0.9"))
        try:
            settings.token_repetition_penalty_max = float(os.getenv("GENERATION_REP_PENALTY", "1.08"))
            settings.token_repetition_penalty_sustain = 256
            settings.token_repetition_penalty_decay = 128
        except Exception:  # pragma: no cover - tolerate sampler variants lacking these attrs
            pass
        return settings

    def _truncate_to_ctx(self, text: str, max_new_tokens: int) -> str:
        try:
            max_ctx = int(os.getenv("EXL2_CACHE_MAX_SEQ_LEN", "2048"))
            reserve = int(os.getenv("EXL2_INPUT_RESERVE_TOKENS", "64"))
            limit = max(32, max_ctx - reserve - int(max_new_tokens))
            tokenizer = getattr(self._generator, "tokenizer", None) or self._tokenizer
            if tokenizer is None:
                return text
            token_ids = tokenizer.encode(text, add_bos=True)
            length = token_ids.shape[-1] if hasattr(token_ids, "shape") else len(token_ids)
            if length > limit:
                if hasattr(token_ids, "shape"):
                    token_ids = token_ids[:, -limit:]
                    text = tokenizer.decode(token_ids[0])
                else:
                    token_ids = token_ids[-limit:]
                    text = tokenizer.decode(token_ids)
                self._log.info("[exl2] truncated prompt tokens: %s -> %s", length, limit)
            return text
        except Exception as exc:  # pragma: no cover - trimming best-effort only
            self._log.warning("[exl2] prompt trim skipped: %s", exc)
            return text

    def generate(
        self,
        prompt: str,
        max_new_tokens: int = 256,
        stop: Optional[List[str]] = None,
        temperature: Optional[float] = None,
        top_p: Optional[float] = None,
        **_ignored,
    ) -> str:
        prompt = self._truncate_to_ctx(prompt, int(max_new_tokens))
        settings = self._build_settings(temperature, top_p)

        text = self._generator.generate_simple(prompt, settings, int(max_new_tokens))
        text = text or ""

        if stop:
            cut = len(text)
            for marker in stop:
                idx = text.find(marker)
                if idx != -1:
                    cut = min(cut, idx)
            text = text[:cut]

        return text.strip()


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
