"""Utility for loading SQLCoder (ExLlamaV2) as a simple text generator."""

from __future__ import annotations

import logging
import os
import time
from typing import Any, Dict, Iterable, Optional

import torch
from exllamav2 import ExLlamaV2Sampler


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
    def __init__(self, generator, tokenizer):
        self._generator = generator
        self._tokenizer = tokenizer
        self._settings = ExLlamaV2Sampler.Settings()

    def _trim_prompt(self, prompt: str, reserve_tokens: int = 256) -> str:
        """Left-truncate prompt to avoid cache overflow."""

        try:
            max_len = int(os.getenv("EXL2_CACHE_MAX_SEQ_LEN", "2048"))
        except Exception:
            max_len = 2048

        try:
            ids = self._tokenizer.encode(prompt, add_bos=True)
            if len(ids) + reserve_tokens > max_len:
                keep = max(max_len - reserve_tokens, 128)
                ids = ids[-keep:]
                prompt = self._tokenizer.decode(ids)
        except Exception:
            if len(prompt) > 8000:
                prompt = prompt[-8000:]
        return prompt

    @staticmethod
    def _truncate_on_stop(text: str, stops: Optional[Iterable[str]]) -> str:
        if not stops:
            return text
        cut = len(text)
        for s in stops:
            if not s:
                continue
            idx = text.find(s)
            if idx != -1 and idx < cut:
                cut = idx
        return text[:cut]

    def generate(
        self,
        prompt: str,
        max_new_tokens: int = 256,
        stop: Optional[Iterable[str]] = None,
        temperature: float = 0.2,
        top_p: float = 0.9,
    ) -> str:
        """Generate text with safe defaults and robust stopping."""

        settings = self._settings.clone() if hasattr(self._settings, "clone") else ExLlamaV2Sampler.Settings()
        settings.temperature = float(temperature) if temperature is not None else 0.2
        settings.top_p = float(top_p) if top_p is not None else 0.9

        max_new = int(max_new_tokens or 256)
        if max_new < 1:
            max_new = 1
        if max_new > 512:
            max_new = 512

        try:
            reserve = int(os.getenv("EXL2_INPUT_RESERVE_TOKENS", "64") or 64)
        except Exception:
            reserve = 64
        prompt = self._trim_prompt(prompt, reserve_tokens=reserve)

        try:
            output = self._generator.generate_simple(prompt, settings, max_new)
        except AssertionError as err:
            logger.warning("[dw] exllama overflow; retrying with reduced context/new tokens: %s", err)
            prompt_tail = self._trim_prompt(prompt, reserve_tokens=reserve + 128)
            output = self._generator.generate_simple(prompt_tail, settings, min(128, max_new))
        text = output if isinstance(output, str) else output[0] if isinstance(output, (list, tuple)) else str(output)
        if not isinstance(text, str):
            text = str(text)

        if stop:
            cut = len(text)
            for s in stop:
                if not s:
                    continue
                idx = text.find(s)
                if idx != -1:
                    cut = min(cut, idx)
            text = text[:cut]

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

    return SQLCoderExLlama(gen, tokenizer)
