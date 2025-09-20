"""Utility for loading SQLCoder (ExLlamaV2) as a simple text generator."""

from __future__ import annotations

import os
import time
from typing import Any, Dict, Iterable, Optional

import torch
from exllamav2.generator import ExLlamaV2Sampler


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


class ExllamaSqlCoder:
    def __init__(
        self,
        model_generator,
        tokenizer,
        stop_tokens: Iterable[str],
        defaults: Dict[str, Any],
        dynamic: bool,
        cache,
    ) -> None:
        self._generator = model_generator
        self._tokenizer = tokenizer
        self._defaults = defaults
        self._stop_tokens = [tok for tok in stop_tokens if tok]
        self._dynamic = dynamic
        self._cache = cache

    def _truncate_to_fit(self, prompt: str, max_new: int) -> str:
        try:
            max_seq = getattr(self._cache, "max_seq_len", 2048)
            ids = self._tokenizer.encode(prompt, add_bos=True)
            if hasattr(ids, "tolist"):
                ids = ids.tolist()
            max_input = max_seq - max_new - 32
            if max_input <= 0 or not isinstance(ids, (list, tuple)):
                return prompt
            if len(ids) <= max_input:
                return prompt
            keep_ids = ids[-max_input:]
            return self._tokenizer.decode(keep_ids)
        except Exception:
            return prompt[-8000:]

    def generate(
        self,
        prompt: str,
        max_new_tokens: int = 192,
        temperature: float = 0.05,
        top_p: float = 0.9,
        stop: Optional[Iterable[str]] = None,
    ) -> str:
        stop_list = list(self._stop_tokens)
        if stop:
            for token in stop:
                if token and token not in stop_list:
                    stop_list.append(token)

        max_new = int(
            max_new_tokens if max_new_tokens is not None else self._defaults.get("max_new_tokens", 192)
        )
        prompt = self._truncate_to_fit(prompt, max_new)
        settings = ExLlamaV2Sampler.Settings()
        settings.temperature = float(
            temperature if temperature is not None else self._defaults.get("temperature", 0.0)
        )
        settings.top_p = float(top_p if top_p is not None else self._defaults.get("top_p", 1.0))
        settings.token_repetition_penalty = 1.05

        self._generator.set_stop_strings(stop_list)
        output = self._generator.generate_simple(prompt, settings, max_new)
        text = output[0] if isinstance(output, (list, tuple)) else output
        for token in stop_list:
            if token and text.endswith(token):
                text = text[: -len(token)]
        return text


def load_exllama_generator(model_path: str, config: Dict[str, Any]) -> ExllamaSqlCoder:
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
    gen_is_dynamic = False
    if dyn_available and not force_base:
        try:
            from exllamav2.generator.dynamic import ExLlamaV2DynamicGenerator

            gen = ExLlamaV2DynamicGenerator(model=model, tokenizer=tokenizer, cache=cache)
            gen_is_dynamic = True
        except Exception:
            gen = None
            gen_is_dynamic = False

    if gen is None:
        from exllamav2.generator.base import ExLlamaV2BaseGenerator

        gen = ExLlamaV2BaseGenerator(model=model, tokenizer=tokenizer, cache=cache)
        gen_is_dynamic = False

    defaults = {
        "max_new_tokens": int(config.get("max_new_tokens", 256)),
        "temperature": float(config.get("temperature", 0.2)),
        "top_p": float(config.get("top_p", 0.9)),
    }
    stop_tokens = list(config.get("stop") or [])
    return ExllamaSqlCoder(
        gen,
        tokenizer,
        stop_tokens,
        defaults,
        gen_is_dynamic,
        cache,
    )
