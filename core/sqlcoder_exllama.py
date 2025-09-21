import os
import inspect
from types import SimpleNamespace

import torch

from exllamav2 import ExLlamaV2, ExLlamaV2Config, ExLlamaV2Cache

# Tokenizer import changed across versions; try both
try:
    from exllamav2.tokenizer import ExLlamaV2Tokenizer
except Exception:
    # some wheels expose tokenizer at top-level
    from exllamav2 import ExLlamaV2Tokenizer  # type: ignore

# Generator import: prefer the higher-level class if available
try:
    from exllamav2.generator import ExLlamaV2Generator as GenClass
except Exception:
    from exllamav2.generator import ExLlamaV2BaseGenerator as GenClass  # type: ignore

# Sampler + Settings: prefer Sampler.Settings; fall back to a dummy
try:
    from exllamav2.generator import ExLlamaV2Sampler  # newer layout
except Exception:
    try:
        from exllamav2.generator.sampler import ExLlamaV2Sampler  # older layout
    except Exception:
        ExLlamaV2Sampler = None  # type: ignore


def _build_settings(temperature: float, top_p: float):
    """
    Build a sampling settings object compatible with the installed exllamav2.
    If the official Settings class is missing, return a SimpleNamespace with needed attrs.
    """
    # Try official Settings
    settings = None
    if ExLlamaV2Sampler is not None:
        try:
            SettingsClass = getattr(ExLlamaV2Sampler, "Settings", None)
            if SettingsClass is not None:
                settings = SettingsClass()
                # common knobs
                if hasattr(settings, "temperature"): settings.temperature = float(temperature)
                if hasattr(settings, "top_p"):       settings.top_p = float(top_p)
                # ensure fields accessed by sampler exist
                if hasattr(settings, "cfg_scale"):   settings.cfg_scale = None
                if hasattr(settings, "top_k"):       settings.top_k = 0
                return settings
        except Exception:
            settings = None

    # Fallback dummy with the fields the sampler touches
    settings = SimpleNamespace()
    # fields the sampler often reads:
    settings.temperature = float(temperature)
    settings.top_p = float(top_p)
    settings.top_k = 0
    settings.min_p = 0.0
    settings.typical = None
    settings.tfs = None
    settings.repetition_penalty = 1.0
    settings.penalty_range = 64
    settings.presence_penalty = 0.0
    settings.frequency_penalty = 0.0
    settings.mirostat = None
    settings.cfg_scale = None
    return settings


class SQLCoderExLlama:
    def __init__(self, model_dir: str):
        if not model_dir or not os.path.isdir(model_dir):
            raise RuntimeError(f"[exllama] model path not found: {model_dir}")

        self.model_dir = model_dir
        self.max_seq_len = int(os.getenv("EXL2_CACHE_MAX_SEQ_LEN", "2048"))

        # Build config
        self.config = ExLlamaV2Config()
        self.config.model_dir = model_dir

        # Optional speed knobs
        if os.getenv("EXL2_FORCE_BASE") == "1":
            # just a hint env; ExLlamaV2 reads config for core toggles
            pass

        # Prepare config and model
        self.config.prepare()
        self.model = ExLlamaV2(self.config)

        # Tokenizer
        self.tokenizer = ExLlamaV2Tokenizer(self.config)

        # Cache
        self.cache = ExLlamaV2Cache(self.model, max_seq_len=self.max_seq_len, batch_size=1)
        # Generator
        self.generator = GenClass(self.model, self.tokenizer, self.cache)

    def generate(self, prompt: str, max_new_tokens: int = 192, stop=None, temperature=0.2, top_p=0.9) -> str:
        """
        Version-tolerant generation:
        - Builds compatible sampling settings
        - Calls generate_simple() with correct signature
        - Applies client-side stop strings
        """
        # defensive crop: prevent overlong prompts w.r.t. cache
        # (ExLlamaV2 caches KV; we ensure prompt tokens fit)
        # Tokenize to check length
        ids = self.tokenizer.encode(prompt)
        if ids.shape[-1] > self.max_seq_len - 64:
            # trim tokens to leave headroom for generation
            ids = ids[:, -(self.max_seq_len - 64):]
            prompt = self.tokenizer.decode(ids)

        settings = _build_settings(temperature, top_p)

        # Inspect generate_simple signature
        sig = inspect.signature(self.generator.generate_simple)
        params = list(sig.parameters.keys())

        # Some versions: generate_simple(prompt, num_tokens)
        # Others:        generate_simple(prompt, settings, num_tokens)
        if len(params) == 3:
            # (self, prompt, settings, num_tokens)
            text = self.generator.generate_simple(prompt, settings, int(max_new_tokens))
        elif len(params) == 2:
            # (self, prompt, num_tokens)
            text = self.generator.generate_simple(prompt, int(max_new_tokens))
        else:
            # unknown variant: try the 3-arg form first
            try:
                text = self.generator.generate_simple(prompt, settings, int(max_new_tokens))
            except TypeError:
                text = self.generator.generate_simple(prompt, int(max_new_tokens))

        # Client-side stop handling
        if stop:
            if isinstance(stop, str):
                stop = [stop]
            for s in stop:
                if not s:
                    continue
                idx = text.find(s)
                if idx != -1:
                    text = text[:idx]
                    break

        return text


def load_exllama_generator(path: str):
    """
    Factory used by model_loader.py
    Returns a dict { 'model': SQLCoderExLlama, 'backend': 'exllama', 'path': path }
    """
    mdl = SQLCoderExLlama(path)
    return {
        "backend": "exllama",
        "path": path,
        "handle": mdl
    }
