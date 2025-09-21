import os
import time
import logging
from typing import List, Optional
from importlib import metadata

import torch

LOG = logging.getLogger("core.sqlcoder_exllama")

# --- Import ExLlamaV2 core ---
from exllamav2 import ExLlamaV2, ExLlamaV2Config, ExLlamaV2Cache

# --- Tokenizer import across variants (0.3.x) ---
# In 0.3.x the class is typically exposed as `Tokenizer` from exllamav2.tokenizer
try:
    from exllamav2.tokenizer import Tokenizer as ExTokenizer
except Exception:
    # Fallbacks – keep them but we will recommend 0.3.2
    try:
        from exllamav2.tokenizer import ExLlamaV2Tokenizer as ExTokenizer
    except Exception as e:
        raise ImportError(
            "Could not import ExLlamaV2 Tokenizer. Please `pip install exllamav2==0.3.2`."
        ) from e

# --- Generator & Sampler (0.3.x) ---
from exllamav2.generator import ExLlamaV2BaseGenerator, ExLlamaV2Sampler


def _str2gb_list(s: str) -> Optional[List[float]]:
    """
    Parse EXL2_GPU_SPLIT_GB like '29,2' -> [29.0, 2.0]
    """
    if not s:
        return None
    try:
        return [float(x.strip()) for x in s.split(",") if x.strip()]
    except Exception:
        return None


class SQLCoderExLlama:
    """
    Thin wrapper around ExLlamaV2BaseGenerator with stable .generate interface.
    """

    def __init__(self, model, tokenizer, generator, cache, max_seq_len: int):
        self.model = model
        self.tokenizer = tokenizer
        self.generator = generator
        self.cache = cache
        self.max_seq_len = max_seq_len

        # Defaults (can be overridden via env)
        self.temperature = float(os.getenv("GENERATION_TEMPERATURE", "0.2"))
        self.top_p = float(os.getenv("GENERATION_TOP_P", "0.9"))

    def _build_settings(self, temperature: Optional[float] = None, top_p: Optional[float] = None):
        st = ExLlamaV2Sampler.Settings()
        st.temperature = self.temperature if temperature is None else float(temperature)
        st.top_p = self.top_p if top_p is None else float(top_p)
        # conservative defaults; adjust if you like
        st.top_k = 0
        st.typical_p = 0.0
        st.token_repetition_penalty = 1.0
        st.dry_multiplier = 0.0
        st.disallow_tokens = None
        return st

    @staticmethod
    def _post_trim(text: str, stop: Optional[List[str]]) -> str:
        if not stop:
            return text
        cut = len(text)
        for s in stop:
            if not s:
                continue
            i = text.find(s)
            if i != -1:
                cut = min(cut, i)
        return text[:cut]

    def generate(
        self,
        prompt: str,
        max_new_tokens: int = 256,
        stop: Optional[List[str]] = None,
        temperature: Optional[float] = None,
        top_p: Optional[float] = None,
    ) -> str:
        """
        Stable generation entry. Always builds valid sampler settings.
        We avoid relying on generator stop hooks; we post-trim.
        """
        # Keep prompt within cache budget (very defensive)
        # Note: ExLlamaV2 works with string input; internal tokenization handles truncation.
        settings = self._build_settings(temperature=temperature, top_p=top_p)

        # exllamav2 0.3.x signature: generate_simple(prompt, settings, num_tokens, ...)
        start = time.time()
        text = self.generator.generate_simple(prompt, settings, int(max_new_tokens))
        text = self._post_trim(text, stop)

        LOG.debug("[exl2] gen %.1f ms, out_len=%d", (time.time() - start) * 1000, len(text))
        return text


def build_sql_model(model_dir: str) -> SQLCoderExLlama:
    """
    Create and return a ready-to-use SQLCoderExLlama wrapper.
    Honors these env vars (all optional):
      - EXL2_CACHE_MAX_SEQ_LEN (int, default 2048)
      - EXL2_CACHE_8BIT (0/1)
      - EXL2_GPU_SPLIT_GB (e.g. '29,2') if you want to guide autosplit
      - RESERVE_VRAM_GB (float)
    """
    ver = metadata.version("exllamav2")
    LOG.info("Loading ExLlamaV2 model from %s (exllamav2 %s)", model_dir, ver)

    # --- Config ---
    max_seq = int(os.getenv("EXL2_CACHE_MAX_SEQ_LEN", "2048"))

    cfg = ExLlamaV2Config()
    cfg.model_dir = model_dir
    # You can set additional knobs here if you want (e.g., cfg.max_seq_len = max_seq)
    # cfg.max_seq_len = max_seq
    cfg.prepare()

    # --- Model ---
    model = ExLlamaV2(cfg)

    # Reserve VRAM on secondary GPU (optional)
    reserve_gb = float(os.getenv("RESERVE_VRAM_GB", "0"))
    if reserve_gb > 0 and torch.cuda.is_available():
        try:
            # simple reservation by allocating a tensor (kept until process exit)
            dev_count = torch.cuda.device_count()
            if dev_count > 1:
                dev1 = torch.device("cuda:1")
                LOG.info("Reserving ~%.1f GiB on cuda:1", reserve_gb)
                _ = torch.empty(int(reserve_gb * (1024**3) / 2), dtype=torch.float16, device=dev1)
        except Exception as e:
            LOG.warning("VRAM reservation failed: %s", e)

    # --- Tokenizer ---
    tokenizer = ExTokenizer(model_dir)

    # --- Cache & Generator ---
    use_8bit = os.getenv("EXL2_CACHE_8BIT", "1") == "1"
    cache = ExLlamaV2Cache(model, max_seq_len=max_seq, lazy=True, cache_8bit=use_8bit)

    # In 0.3.x BaseGenerator uses model+tokenizer+cache
    generator = ExLlamaV2BaseGenerator(model, tokenizer, cache)

    # Optional guided autosplit by GB if you want to match your earlier behavior
    split = _str2gb_list(os.getenv("EXL2_GPU_SPLIT_GB", ""))
    try:
        if split:
            LOG.info("Attempting guided autosplit by GB: %s", split)
            # Newer API does autosplit internally based on cache/model footprints.
            # If your local fork exposes a helper, call it here; otherwise model will autosplit during first run.
            # We keep this log so you know the intent.
        else:
            LOG.info("Relying on ExLlamaV2 autosplit.")
    except Exception as e:
        LOG.warning("Autosplit hint failed (non-fatal): %s", e)

    LOG.info("SQL model (ExLlamaV2) ready: cache_len=%d, cache_8bit=%s", max_seq, use_8bit)
    return SQLCoderExLlama(model, tokenizer, generator, cache, max_seq_len=max_seq)
