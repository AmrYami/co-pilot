"""Utility for loading SQLCoder (ExLlamaV2) as a simple text generator."""

from __future__ import annotations

import logging
import os
import time
from typing import Any, Dict, Iterable, Optional

import torch

try:  # exllamav2 >= 0.1.5
    from exllamav2.generator.sampler import ExLlamaV2Sampler  # type: ignore
except Exception:  # pragma: no cover - fallback for older versions
    try:
        from exllamav2.generator import ExLlamaV2Sampler  # type: ignore
    except Exception:  # pragma: no cover - sampler unavailable
        ExLlamaV2Sampler = None  # type: ignore


logger = logging.getLogger("dw")


def _parse_gpu_split(env_value: str | None) -> Optional[list[float]]:
    if not env_value:
        return None
    parts = [p.strip() for p in env_value.split(",") if p.strip()]
    if not parts:
        return None
    try:
        numbers = [float(p) for p in parts]
    except Exception:
        return None
    total = sum(numbers)
    if total <= 0:
        return None
    if max(numbers) > 1.5:
        return [n / total for n in numbers]
    return [n / total for n in numbers]


class SQLCoderExLlama:
    def __init__(self, generator, tokenizer, cache_max_seq_len: int, input_reserve_tokens: int = 64):
        self._generator = generator
        self._tokenizer = tokenizer
        self._cache_max_seq_len = cache_max_seq_len
        self._input_reserve_tokens = input_reserve_tokens

    def _truncate_prompt(self, prompt: str, max_new_tokens: int) -> str:
        """Ensure the prompt fits within the KV cache budget."""

        reserve_env = os.getenv("EXL2_INPUT_RESERVE_TOKENS")
        try:
            reserve_tokens = int(reserve_env) if reserve_env is not None else self._input_reserve_tokens
        except Exception:
            reserve_tokens = self._input_reserve_tokens
        reserve_tokens = max(0, reserve_tokens)

        try:
            ids = self._tokenizer.encode(prompt, add_bos=True)
        except Exception:
            if len(prompt) > 8000:
                tail = prompt[-8000:]
                logger.debug("[dw] prompt truncated to last 8000 characters (encode fallback)")
                return tail
            return prompt

        if hasattr(ids, "tolist"):
            tokens = ids.tolist()
            if tokens and isinstance(tokens[0], list):
                tokens = tokens[0]
        else:
            tokens = list(ids)

        token_count = len(tokens)

        max_new = max(0, int(max_new_tokens or 0))
        max_input_tokens = max(1, self._cache_max_seq_len - max_new - reserve_tokens)
        if token_count > max_input_tokens:
            tokens = tokens[-max_input_tokens:]
            prompt = self._tokenizer.decode(tokens)
            logger.debug(
                "[dw] prompt truncated to %d tokens (max_input_tokens=%d)",
                len(tokens),
                max_input_tokens,
            )
        return prompt

    @staticmethod
    def _apply_stops(text: str, stops: Optional[Iterable[str]]) -> str:
        if not text:
            return text

        cut = len(text)
        for fence in ("```sql", "```"):
            idx = text.find(fence)
            if idx != -1:
                cut = min(cut, idx)

        if stops:
            for stop in stops:
                if not stop:
                    continue
                idx = text.find(stop)
                if idx != -1:
                    cut = min(cut, idx)

        return text[:cut]

    def _call_generate_simple(
        self,
        prompt: str,
        max_new_tokens: int,
        temperature: Optional[float],
        top_p: Optional[float],
    ):
        settings = None
        if ExLlamaV2Sampler is not None:
            try:
                settings = ExLlamaV2Sampler.Settings()
                if temperature is not None:
                    try:
                        settings.temperature = float(temperature)
                    except Exception:
                        settings.temperature = 0.2
                if top_p is not None:
                    try:
                        settings.top_p = float(top_p)
                    except Exception:
                        settings.top_p = 0.9
            except Exception:  # pragma: no cover - fallback for unexpected API changes
                settings = None

        if settings is not None:
            try:
                return self._generator.generate_simple(prompt, settings, max_new_tokens)
            except TypeError:
                pass

        try:
            return self._generator.generate_simple(prompt, max_new_tokens)
        except TypeError:
            return self._generator.generate_simple(prompt, None, max_new_tokens)

    def generate(
        self,
        prompt: str,
        max_new_tokens: int = 256,
        stop: Optional[Iterable[str]] = None,
        temperature: float | None = None,
        top_p: float | None = None,
    ) -> str:
        """Generate text with safe defaults and robust stopping."""

        try:
            max_new = int(os.getenv("GENERATION_MAX_NEW_TOKENS", max_new_tokens))
        except Exception:
            max_new = int(max_new_tokens or 256)
        max_new = max(1, max_new)

        if temperature is None:
            try:
                temperature = float(os.getenv("GENERATION_TEMPERATURE", "0.2"))
            except Exception:
                temperature = 0.2
        else:
            try:
                temperature = float(temperature)
            except Exception:
                temperature = 0.2

        if top_p is None:
            try:
                top_p = float(os.getenv("GENERATION_TOP_P", "0.9"))
            except Exception:
                top_p = 0.9
        else:
            try:
                top_p = float(top_p)
            except Exception:
                top_p = 0.9

        prompt = self._truncate_prompt(prompt, max_new)

        try:
            output = self._call_generate_simple(prompt, max_new, temperature, top_p)
        except AssertionError as err:
            logger.warning("[dw] exllama overflow; retrying with truncated context: %s", err)
            prompt = self._truncate_prompt(prompt, max_new)
            output = self._call_generate_simple(prompt, min(max_new, 128), temperature, top_p)

        text = output if isinstance(output, str) else output[0] if isinstance(output, (list, tuple)) else str(output)
        if not isinstance(text, str):
            text = str(text)

        text = self._apply_stops(text, stop)
        return text.strip()


def load_exllama_generator(model_path: str, config: Dict[str, Any]) -> SQLCoderExLlama:
    """Load ExLlamaV2 for SQLCoder and return a lightweight generator wrapper."""

    from exllamav2 import ExLlamaV2, ExLlamaV2Cache, ExLlamaV2Config, ExLlamaV2Tokenizer

    force_base = str(os.getenv("EXL2_FORCE_BASE", "0")).lower() in {"1", "true", "yes", "on"}
    if force_base:
        try:
            import exllamav2.attn as _attn

            _attn.has_flash_attn = False
        except Exception:
            pass

    dyn_available = False
    if not force_base:
        try:
            from exllamav2.generator.dynamic import ExLlamaV2DynamicGenerator  # noqa: F401

            dyn_available = True
        except Exception:
            dyn_available = False

    cfg = ExLlamaV2Config(model_path)
    cfg.max_seq_len = int(config.get("max_seq_len", 4096))
    if torch.cuda.is_available():
        cfg.set_low_mem()
        cfg.gpu_peer_fix = True
    cfg.prepare()

    model = ExLlamaV2(cfg)
    tokenizer = ExLlamaV2Tokenizer(cfg)

    cache_len = int(os.getenv("EXL2_CACHE_MAX_SEQ_LEN", cfg.max_seq_len))
    cache = ExLlamaV2Cache(model, lazy=True, max_seq_len=cache_len)

    split = _parse_gpu_split(os.getenv("EXL2_GPU_SPLIT_GB") or os.getenv("GPU_SPLIT"))

    reserve: Optional[list[int]] = None
    try:
        reserve_gb = float(os.getenv("RESERVE_VRAM_GB", "0") or 0)
    except Exception:
        reserve_gb = 0.0
    if torch.cuda.device_count() > 1 and reserve_gb > 0:
        reserve = [0, int(reserve_gb * (1 << 30))]

    def _load_weights(split_hint: Optional[list[float]], cache_obj: ExLlamaV2Cache) -> None:
        t0 = time.time()
        if split_hint:
            model.load(split_hint, cache_obj)
        else:
            model.load_autosplit(cache_obj, progress=True, reserve_vram=reserve)
        print(f"ExLlamaV2 weights ready in {time.time() - t0:.2f}s")

    current_cache = cache

    try:
        _load_weights(split, current_cache)
    except Exception as exc:
        msg = str(exc).lower()
        flash_fail = "flashatt" in msg or "flash_attn" in msg
        if flash_fail:
            try:
                import exllamav2.attn as _attn

                _attn.has_flash_attn = False
            except Exception:
                pass
        smaller = max(1024, cache_len // 2)
        if smaller < cache_len:
            current_cache = ExLlamaV2Cache(model, lazy=True, max_seq_len=smaller)
        else:
            current_cache = ExLlamaV2Cache(model, lazy=True, max_seq_len=cache_len)
        if split:
            _load_weights(None, current_cache)
        else:
            _load_weights(split, current_cache)

    cache = current_cache

    try:
        reserve_tokens = int(os.getenv("EXL2_INPUT_RESERVE_TOKENS", "64") or 64)
    except Exception:
        reserve_tokens = 64

    gen = None
    if dyn_available and not force_base:
        try:
            from exllamav2.generator.dynamic import ExLlamaV2DynamicGenerator

            gen = ExLlamaV2DynamicGenerator(model=model, tokenizer=tokenizer, cache=cache)
        except Exception:
            gen = None

    if gen is None:
        from exllamav2.generator.base import ExLlamaV2BaseGenerator

        gen = ExLlamaV2BaseGenerator(model=model, tokenizer=tokenizer, cache=cache)

    return SQLCoderExLlama(gen, tokenizer, cache.max_seq_len, reserve_tokens)
