import os
import logging
from typing import Dict, Any, List
import torch

# ExLlamaV2 imports – keep them broad to tolerate minor version changes
from exllamav2 import ExLlamaV2Config, ExLlamaV2, ExLlamaV2Cache, ExLlamaV2Tokenizer
try:
    from exllamav2.generator import ExLlamaV2BaseGenerator, ExLlamaV2Sampler
    _HAS_SAMPLER = True
except Exception:
    # Some older wheels expose sampler under a different path or not at all
    from exllamav2.generator.base import ExLlamaV2BaseGenerator  # type: ignore
    ExLlamaV2Sampler = None  # type: ignore
    _HAS_SAMPLER = False

# Loader API moved around across versions; try a couple of options
_LOADER_CLS = None
for cand in (
    "exllamav2.loader.ModelLoader",
    "exllamav2.loader.LazyModelLoader",
    "exllamav2.loader.SelectiveGPUModelLoader"
):
    try:
        mod_name, cls_name = cand.rsplit(".", 1)
        mod = __import__(mod_name, fromlist=[cls_name])
        _LOADER_CLS = getattr(mod, cls_name)
        break
    except Exception:
        continue

log = logging.getLogger("core.sqlcoder_exllama")


def _parse_gpu_split() -> List[float]:
    """Parse EXL2_GPU_SPLIT_GB=29,2 -> [29.0, 2.0]"""
    env = os.getenv("EXL2_GPU_SPLIT_GB", "").strip()
    if not env:
        return []
    parts = []
    for p in env.split(","):
        p = p.strip()
        if not p:
            continue
        try:
            parts.append(float(p))
        except Exception:
            pass
    return parts


def _autosplit(model: ExLlamaV2, model_dir: str, gpu_split: List[float]) -> None:
    """
    Load weights and set device map. Tries the loader class if available,
    otherwise falls back to model-level helpers.
    """
    if _LOADER_CLS is not None:
        try:
            loader = _LOADER_CLS(model, model_dir=model_dir)
        except TypeError:
            # Some versions expect (model,) only and take model_dir from config
            loader = _LOADER_CLS(model)
        # Not all loaders expose the same method names; try autosplit first
        for meth in ("load_autosplit", "load_multi_gpu_autosplit", "load"):
            if hasattr(loader, meth):
                fn = getattr(loader, meth)
                try:
                    if meth == "load_autosplit":
                        fn(gpu_split if gpu_split else None)
                    else:
                        # Best-effort: call without split if the signature is unknown
                        fn()
                    log.info("[exllama] weights loaded via %s", meth)
                    return
                except Exception as e:
                    log.warning("[exllama] %s failed: %s", meth, e)
                    continue
    # Fallback: single device best effort
    dev = 0
    try:
        if torch.cuda.is_available():
            dev = 0
        model.to( torch.device(f"cuda:{dev}") if torch.cuda.is_available() else torch.device("cpu") )
        log.info("[exllama] weights loaded to single device %s", f"cuda:{dev}" if torch.cuda.is_available() else "cpu")
    except Exception as e:
        log.error("[exllama] fallback to single-device failed: %s", e)
        raise


class SQLCoderExLlama:
    """
    Thin wrapper around ExLlamaV2BaseGenerator to normalize generate() across
    minor API differences and provide simple stop-string truncation.
    """

    def __init__(self, generator: Any, tokenizer: Any):
        self._generator = generator
        self._tokenizer = tokenizer
        # Default sampler settings if available
        if _HAS_SAMPLER:
            self._settings = ExLlamaV2Sampler.Settings()
            try:
                # Stable defaults; can be overridden in generate()
                self._settings.temperature = float(os.getenv("GENERATION_TEMPERATURE", "0.2"))
                self._settings.top_p = float(os.getenv("GENERATION_TOP_P", "0.9"))
                # make sure CFG unset unless explicitly provided
                self._settings.cfg_scale = None
            except Exception:
                pass
        else:
            self._settings = None

    def generate(self, prompt: str, max_new_tokens: int = 256, stop: List[str] | None = None,
                 temperature: float | None = None, top_p: float | None = None) -> str:
        # Update settings if we have a sampler
        settings = self._settings
        if settings is not None:
            if temperature is not None:
                settings.temperature = float(temperature)
            if top_p is not None:
                settings.top_p = float(top_p)

        # ExLlamaV2BaseGenerator.generate_simple signature differs across versions.
        # Try (prompt, settings, num_tokens) first, then (prompt, num_tokens).
        text = None
        try:
            text = self._generator.generate_simple(prompt, settings, int(max_new_tokens))  # type: ignore[arg-type]
        except TypeError:
            # Older/newer signature without settings
            text = self._generator.generate_simple(prompt, int(max_new_tokens))  # type: ignore[call-arg]

        # Manual stop-string truncation (don’t rely on set_stop_strings API)
        if text and stop:
            cut = len(text)
            for s in stop:
                i = text.find(s)
                if i != -1:
                    cut = min(cut, i)
            text = text[:cut]
        return text or ""


def load_exllama_generator(model_dir: str) -> Dict[str, Any]:
    """
    Build ExLlamaV2 model/tokenizer/generator with a safe autosplit and cache.
    Returns a dictionary the model_loader expects.
    """
    log.info("Loading ExLlamaV2 model: %s", model_dir)

    cfg = ExLlamaV2Config()
    # Newer versions expect model_dir on the config object
    cfg.model_dir = model_dir
    # Max seq len & cache len from env
    cfg.max_seq_len = int(os.getenv("MODEL_MAX_SEQ_LEN", "4096"))
    try:
        # Keep some headroom for long prompts
        input_reserve = int(os.getenv("EXL2_INPUT_RESERVE_TOKENS", "64"))
        cfg.max_input_len = max(256, min(cfg.max_seq_len - 128, cfg.max_seq_len - input_reserve))
    except Exception:
        pass

    # Enforce base if requested (helps compat)
    try:
        if os.getenv("EXL2_FORCE_BASE", "0") in ("1", "true", "True"):
            cfg.no_flash_attn = True
    except Exception:
        pass

    cfg.prepare()

    model = ExLlamaV2(cfg)
    tokenizer = ExLlamaV2Tokenizer(cfg)

    # Load weights + device map
    gpu_split = _parse_gpu_split()
    _autosplit(model, model_dir, gpu_split)

    # Build cache and generator
    cache_len = int(os.getenv("EXL2_CACHE_MAX_SEQ_LEN", "2048"))
    cache = ExLlamaV2Cache(model, batch_size=1, max_seq_len=cache_len)
    generator = ExLlamaV2BaseGenerator(model, tokenizer, cache)

    # Always decode to string using the model’s tokenizer
    wrapper = SQLCoderExLlama(generator, tokenizer)

    log.info("ExLlamaV2 ready (seq_len=%s, cache_len=%s, gpus=%s, split=%s)",
             cfg.max_seq_len, cache_len,
             torch.cuda.device_count() if torch.cuda.is_available() else 0,
             gpu_split if gpu_split else "auto/single")

    return {
        "model": model,
        "tokenizer": tokenizer,
        "generator": wrapper,   # expose wrapper with .generate()
        "cache": cache,
        "meta": {
            "backend": "exllama",
            "path": model_dir,
            "max_seq_len": cfg.max_seq_len,
            "cache_len": cache_len
        }
    }
