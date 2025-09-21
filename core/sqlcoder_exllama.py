import os
from typing import List, Optional

from exllamav2 import ExLlamaV2, ExLlamaV2Config, ExLlamaV2Cache
from exllamav2.tokenizer import Tokenizer
from exllamav2.generator import (
    ExLlamaV2BaseGenerator,
    ExLlamaV2SamplingSettings,
)


# -------------------------
# Utilities
# -------------------------

def _env_float(name: str, dflt: float) -> float:
    try:
        return float(os.getenv(name, str(dflt)))
    except Exception:
        return dflt


def _env_int(name: str, dflt: int) -> int:
    try:
        return int(os.getenv(name, str(dflt)))
    except Exception:
        return dflt


def _stop_truncate(text: str, stops: Optional[List[str]]) -> str:
    if not text or not stops:
        return text
    cut = len(text)
    for s in stops:
        if not s:
            continue
        i = text.find(s)
        if i != -1 and i < cut:
            cut = i
    return text[:cut]


# -------------------------
# Public loader
# -------------------------


class SQLCoderModel:
    """
    Lightweight wrapper around ExLlamaV2 0.3.x for text generation.
    """

    def __init__(self, generator: ExLlamaV2BaseGenerator, tokenizer: Tokenizer) -> None:
        self.generator = generator
        self.tokenizer = tokenizer

    def generate(
        self,
        prompt: str,
        max_new_tokens: int = 256,
        temperature: Optional[float] = None,
        top_p: Optional[float] = None,
        stop: Optional[List[str]] = None,
    ) -> str:
        # Build sampling settings (must not be None for 0.3.x)
        settings = ExLlamaV2SamplingSettings()
        settings.temperature = _env_float("GENERATION_TEMPERATURE", 0.2) if temperature is None else float(temperature)
        settings.top_p = _env_float("GENERATION_TOP_P", 0.9) if top_p is None else float(top_p)
        # Some sensible defaults to keep outputs deterministic enough for SQL:
        settings.token_repetition_penalty = 1.05
        settings.disallow_tokens = []  # leave empty; we’ll fence via prompt

        # Generate
        num_new = int(_env_int("GENERATION_MAX_NEW_TOKENS", max_new_tokens))
        # ExLlamaV2 0.3.x returns a plain string here
        text = self.generator.generate_simple(prompt, settings, num_new)

        # Trim on user stop strings if provided
        text = _stop_truncate(text, stop)

        return text


def build_sql_model(model_dir: Optional[str] = None) -> SQLCoderModel:
    """
    Build and return the SQLCoderModel wrapper.
    Reads:
      - MODEL_PATH (required): path to /sqlcoder70b-exl2-4bit
      - EXL2_CACHE_MAX_SEQ_LEN (optional, default 2048)
      - EXL2_GPU_SPLIT_GB (optional, e.g. "29,2")
    """
    env_model_dir = os.getenv("MODEL_PATH")
    if env_model_dir:
        model_dir = env_model_dir

    if not model_dir or not os.path.isdir(model_dir):
        raise RuntimeError(f"[exllama] MODEL_PATH not found or not a directory: {model_dir!r}")

    # Config
    cfg = ExLlamaV2Config()
    cfg.model_dir = model_dir
    cfg.prepare()

    # Load model
    model = ExLlamaV2(cfg)

    # Optional: split across GPUs if EXL2_GPU_SPLIT_GB is provided (simple heuristic)
    split = os.getenv("EXL2_GPU_SPLIT_GB")
    if split:
        _ = [s.strip() for s in split.split(",") if s.strip()]
        # We won’t overcomplicate the placement; 0.3.x loads fine on a single big GPU.
        # If you need precise per-layer split, we’d add exllamav2.autosplit here.

    # Tokenizer & cache
    tokenizer = Tokenizer(cfg)
    max_seq = _env_int("EXL2_CACHE_MAX_SEQ_LEN", 2048)
    cache = ExLlamaV2Cache(model, max_seq_len=max_seq)

    # Generator
    generator = ExLlamaV2BaseGenerator(model, tokenizer, cache)

    return SQLCoderModel(generator, tokenizer)
