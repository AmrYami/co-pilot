"""Utility for loading SQLCoder (ExLlamaV2) as a simple text generator."""

from __future__ import annotations

import os
import time
from typing import Any, Dict, Iterable, Optional

import torch


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


class ExLlamaGenerator:
    def __init__(
        self,
        generator,
        tokenizer,
        stop_tokens: Iterable[str],
        defaults: Dict[str, Any],
        dynamic: bool,
        cache_max_seq_len: Optional[int] = None,
    ) -> None:
        self._generator = generator
        self._tokenizer = tokenizer
        self._stop_tokens = [tok for tok in stop_tokens if tok]
        self._defaults = defaults
        self._dynamic = dynamic
        self._cache_max_seq_len = cache_max_seq_len or int(
            os.getenv("EXL2_CACHE_MAX_SEQ_LEN", "2048")
        )
        self._input_reserve = int(os.getenv("EXL2_INPUT_RESERVE_TOKENS", "64"))

    def _truncate_tokens_left(self, text: str, keep_tokens: int) -> str:
        if keep_tokens <= 0 or not text:
            return text
        try:
            ids = self._tokenizer.encode(text)
        except Exception:
            return text
        if hasattr(ids, "tolist"):
            ids = ids.tolist()
        if not isinstance(ids, (list, tuple)):
            return text
        if len(ids) <= keep_tokens:
            return text
        trimmed = ids[-keep_tokens:]
        try:
            return self._tokenizer.decode(trimmed)
        except Exception:
            return text

    def generate(
        self,
        prompt: str,
        max_new_tokens: Optional[int] = None,
        temperature: Optional[float] = None,
        top_p: Optional[float] = None,
        stop: Optional[Iterable[str]] = None,
    ) -> str:
        from exllamav2.generator import ExLlamaV2Sampler

        args = dict(self._defaults)
        if max_new_tokens is not None:
            args["max_new_tokens"] = int(max_new_tokens)
        if temperature is not None:
            args["temperature"] = float(temperature)
        if top_p is not None:
            args["top_p"] = float(top_p)

        stop_tokens = list(stop or self._stop_tokens)
        for token in ("```", "</s>"):
            if token and token not in stop_tokens:
                stop_tokens.append(token)
        max_new = int(args["max_new_tokens"])
        temp = float(args["temperature"])
        nucleus = float(args["top_p"])

        allow_in = max(self._cache_max_seq_len - max_new - self._input_reserve, 256)
        prompt_text = self._truncate_tokens_left(prompt, allow_in)

        if self._dynamic:
            try:
                text = self._generator.generate_simple(
                    prompt_text,
                    max_new_tokens=max_new,
                    temperature=temp,
                    top_p=nucleus,
                )
            except TypeError:
                text = None
            else:
                if isinstance(text, (list, tuple)):
                    text = text[0]
                if text is not None:
                    for token in stop_tokens:
                        if token and token in text:
                            text = text.split(token, 1)[0]
                    return text

        settings = ExLlamaV2Sampler.Settings()
        settings.temperature = temp
        settings.top_p = nucleus
        try:
            output = self._generator.generate_simple(prompt_text, settings, max_new)
        except TypeError:
            try:
                output = self._generator.generate_simple(prompt_text, max_new, settings)
            except TypeError:
                output = self._generator.generate_simple(
                    prompt_text,
                    settings=settings,
                    max_new_tokens=max_new,
                )
        text = output[0] if isinstance(output, (list, tuple)) else output
        for token in stop_tokens:
            if token and token in text:
                text = text.split(token, 1)[0]
        return text


def load_exllama_generator(model_path: str, config: Dict[str, Any]) -> ExLlamaGenerator:
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
    stop_tokens = config.get("stop") or []
    cache_max_seq_len = getattr(cache, "max_seq_len", None)
    return ExLlamaGenerator(
        gen,
        tokenizer,
        stop_tokens,
        defaults,
        gen_is_dynamic,
        cache_max_seq_len=cache_max_seq_len,
    )
