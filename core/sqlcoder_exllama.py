# core/sqlcoder_exllama.py
# Adapter for SQLCoder (EXL2 4-bit) on ExLlamaV2 – API-compatible with recent exllamav2

from __future__ import annotations
import os
import logging
from typing import List, Optional

import torch

from exllamav2 import (
    ExLlamaV2,
    ExLlamaV2Cache,
    ExLlamaV2Config,
    ExLlamaV2Tokenizer,
)
from exllamav2.generator import (
    ExLlamaV2BaseGenerator,
    ExLlamaV2SamplingSettings,
)

log = logging.getLogger("core.sqlcoder_exllama")


# ---------- Utilities ----------

def _parse_gpu_split(env_val: Optional[str]) -> Optional[List[float]]:
    """
    Parse EXL2_GPU_SPLIT_GB like "29,2" into [29.0, 2.0].
    Return None if empty/unset.
    """
    if not env_val:
        return None
    parts = [p.strip() for p in env_val.split(",") if p.strip()]
    if not parts:
        return None
    try:
        return [float(p) for p in parts]
    except Exception:
        log.warning("[exl2] Could not parse EXL2_GPU_SPLIT_GB=%r; ignoring.", env_val)
        return None


def _cut_at_first(text: str, stops: List[str]) -> str:
    if not text:
        return text
    first = len(text)
    for s in stops:
        i = text.find(s)
        if i != -1 and i < first:
            first = i
    return text[:first]


def _extract_sql_fenced(text: str) -> str:
    """
    Try to extract the first ```sql ... ``` or ``` ... ``` block.
    If none, return raw text (caller may decide it’s invalid).
    """
    if not text:
        return ""

    # Prefer ```sql fenced block
    start = text.find("```sql")
    fence_len = 6
    if start == -1:
        # fallback: any ```
        start = text.find("```")
        fence_len = 3

    if start == -1:
        return text.strip()

    start += fence_len
    end = text.find("```", start)
    if end == -1:
        return text[start:].strip()
    return text[start:end].strip()


# ---------- Main loader & wrapper ----------

class SQLCoderExL2:
    """
    Thin wrapper exposing .generate(prompt, max_new_tokens=..., stop=...), as expected by model_loader.get_model("sql").
    """

    def __init__(
        self,
        generator: ExLlamaV2BaseGenerator,
        tokenizer: ExLlamaV2Tokenizer,
        cache: ExLlamaV2Cache,
        input_reserve_tokens: int = 64,
        model_name: str = "sqlcoder-exl2",
    ):
        self._generator = generator
        self._tokenizer = tokenizer
        self._cache = cache
        self._input_reserve = max(0, int(input_reserve_tokens))
        self.name = f"{model_name} (exllama)"

        # Default sampling settings – deterministic but not pathological
        self._temperature = float(os.getenv("GENERATION_TEMPERATURE", "0.2"))
        self._top_p = float(os.getenv("GENERATION_TOP_P", "0.9"))

    # --- internal helpers ---

    def _truncate_prompt_to_fit(self, prompt: str, max_new: int) -> str:
        """
        Ensure total tokens <= cache.max_seq_len - small_safety.
        We keep the last part of the prompt if it is too long (most important instructions are usually near the end).
        """
        try:
            # encode returns a tensor of token ids
            ids = self._tokenizer.encode(prompt)
            max_allowed = max(16, self._cache.max_seq_len - max_new - self._input_reserve)
            if ids.numel() > max_allowed:
                ids = ids[-max_allowed:]
                # decode back to text
                prompt = self._tokenizer.decode(ids)
        except Exception as e:
            # If anything goes wrong, don't kill the run – just return original text.
            log.warning("[exl2] prompt truncation skipped: %s", e)
        return prompt

    # --- public API expected by the rest of the app ---

    def generate(
        self,
        prompt: str,
        max_new_tokens: int = 256,
        stop: Optional[List[str]] = None,
        temperature: Optional[float] = None,
        top_p: Optional[float] = None,
    ) -> str:
        """
        Generate raw model text. We *do not* call removed/unstable generator APIs like set_stop_strings.
        We post-trim in Python using `stop` markers (e.g., "```").
        """
        max_new = int(max_new_tokens)
        prompt = self._truncate_prompt_to_fit(prompt, max_new)

        # Build sampling settings every call (cheap & side-effect free)
        s = ExLlamaV2SamplingSettings()
        s.temperature = float(temperature if temperature is not None else self._temperature)
        s.top_p = float(top_p if top_p is not None else self._top_p)
        # A couple of sensible defaults for SQL
        s.token_repetition_penalty = 1.05
        s.disallow_tokens = None  # keep simple

        # exllamav2 >= 0.2 expects (text, settings, num_tokens)
        text = self._generator.generate_simple(prompt, s, max_new)

        # Apply simple stop trimming client-side
        if stop:
            text = _cut_at_first(text, stop)

        return text


def load_exllama_generator(model_dir: str) -> SQLCoderExL2:
    """
    Create and return a SQLCoderExL2 (generator+tokenizer+cache bundle).
    Environment knobs:
      - EXL2_GPU_SPLIT_GB="29,2"     # optional autosplit across GPUs
      - EXL2_CACHE_MAX_SEQ_LEN=2048  # cache length
      - EXL2_INPUT_RESERVE_TOKENS=64 # budget for system suffixes/stops
    """
    log.info("Loading ExLlamaV2 model: %s", model_dir)

    # 1) Config
    cfg = ExLlamaV2Config()
    cfg.model_dir = model_dir
    cfg.prepare()

    # 2) Model
    model = ExLlamaV2(cfg)

    split = _parse_gpu_split(os.getenv("EXL2_GPU_SPLIT_GB", "").strip())
    if split:
        model.load_autosplit(split)
        log.info("ExLlamaV2 weights loaded (autosplit): %s", split)
    else:
        model.load()
        log.info("ExLlamaV2 weights loaded (single device)")

    # 3) Tokenizer
    tokenizer = ExLlamaV2Tokenizer(cfg)

    # 4) Cache + Generator
    cache_len = int(os.getenv("EXL2_CACHE_MAX_SEQ_LEN", "2048"))
    cache = ExLlamaV2Cache(model, cache_len)

    generator = ExLlamaV2BaseGenerator(model, cache, tokenizer)

    # 5) Wrap
    reserve = int(os.getenv("EXL2_INPUT_RESERVE_TOKENS", "64"))
    wrapper = SQLCoderExL2(
        generator=generator,
        tokenizer=tokenizer,
        cache=cache,
        input_reserve_tokens=reserve,
        model_name=os.path.basename(model_dir.rstrip("/")),
    )
    log.info("ExLlamaV2 model ready (cache_len=%d, reserve=%d)", cache_len, reserve)
    return wrapper
