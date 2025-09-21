import os
import inspect
from types import SimpleNamespace

# Hard requirement: PyTorch must be importable before exllamav2 loads
import torch

# --- ExLlamaV2 core imports (stable) ---
from exllamav2 import ExLlamaV2, ExLlamaV2Config, ExLlamaV2Cache

# --- Tokenizer import is version-dependent; try the common paths ---
try:
    from exllamav2.tokenizer import Tokenizer  # many 0.2.x builds
except Exception:
    try:
        from exllamav2.tokenizer.tokenizer import Tokenizer  # some builds
    except Exception:
        try:
            from exllamav2 import Tokenizer  # fallback if re-exported
        except Exception as e:
            raise ImportError(
                "Could not import ExLlamaV2 Tokenizer from any known path"
            ) from e

# --- Generator imports (stable base generator across 0.2.x/0.3.x) ---
from exllamav2.generator import ExLlamaV2BaseGenerator

# --- Sampling settings: name & location changed across versions ---
try:
    # many 0.2.x builds
    from exllamav2.generator import ExLlamaV2SamplingSettings as _SamplerSettings
except Exception:
    try:
        # some builds moved settings to sampler module
        from exllamav2.generator.sampler import SamplingSettings as _SamplerSettings
    except Exception:
        _SamplerSettings = None  # will use a duck-typed fallback


def _make_settings():
    """Return a sampling settings object acceptable by this exllamav2 build."""
    t = float(os.getenv("GENERATION_TEMPERATURE", "0.2"))
    tp = float(os.getenv("GENERATION_TOP_P", "0.9"))

    if _SamplerSettings is not None:
        s = _SamplerSettings()
        # The attribute names differ slightly across builds; set the common ones.
        for name in ("temperature", "top_p"):
            if hasattr(s, name):
                setattr(s, name, t if name == "temperature" else tp)
        # These are safe defaults; present in many builds (ignored if absent)
        for name, val in (
            ("token_repetition_penalty", 1.05),
            ("min_p", 0.0),
            ("top_k", 0),
            ("mirostat_tau", 0.0),
            ("mirostat_lr", 0.0),
            ("cfg_scale", None),
            ("seed", -1),
        ):
            if hasattr(s, name):
                setattr(s, name, val)
        return s

    # Duck-typed fallback: the sampler only accesses attributes it needs.
    return SimpleNamespace(
        temperature=t,
        top_p=tp,
        token_repetition_penalty=1.05,
        min_p=0.0,
        top_k=0,
        mirostat_tau=0.0,
        mirostat_lr=0.0,
        cfg_scale=None,
        seed=-1,
    )


def _parse_gpu_split(var: str):
    if not var:
        return None
    try:
        return [float(x.strip()) for x in var.split(",") if x.strip()]
    except Exception:
        return None


class SQLCoderExLlama:
    def __init__(self, generator: ExLlamaV2BaseGenerator, tokenizer: Tokenizer, max_seq_len: int):
        self.generator = generator
        self.tokenizer = tokenizer
        self.max_seq_len = max_seq_len

        # detect signature: (prompt, settings, num_tokens) vs (prompt, num_tokens)
        try:
            sig = inspect.signature(self.generator.generate_simple)
            self._gen_has_settings = (len(sig.parameters) == 3)
        except Exception:
            # most 0.2.x builds have 3 args
            self._gen_has_settings = True

    def _safe_num_new(self, prompt_text: str, requested_new: int) -> int:
        # Estimate tokens conservatively; most tokenizers return len(list[int])
        try:
            n_in = len(self.tokenizer.encode(prompt_text, add_bos=False, add_eos=False))
        except Exception:
            # crude heuristic if encode(...) signature differs
            n_in = max(1, len(prompt_text) // 4)

        # keep a small reserve to prevent cache overflow
        reserve = int(os.getenv("EXL2_INPUT_RESERVE_TOKENS", "64"))
        budget = max(32, self.max_seq_len - n_in - reserve)
        return max(32, min(requested_new, budget))

    def generate(self, prompt: str, max_new_tokens: int = 256, stop=None):
        settings = _make_settings()
        n_new = self._safe_num_new(prompt, int(max_new_tokens))

        # Do not call removed APIs like set_stop_strings; we’ll trim in downstream.
        if self._gen_has_settings:
            text = self.generator.generate_simple(prompt, settings, n_new)
        else:
            # some newer builds accept only (prompt, num_tokens)
            try:
                text = self.generator.generate_simple(prompt, n_new)
            except TypeError:
                # fallback to 3-arg if our guess was wrong
                text = self.generator.generate_simple(prompt, settings, n_new)

        return text


def build_sql_model(model_dir: str):
    """
    Build and return a light wrapper exposing .generate(prompt, max_new_tokens, stop).
    Respects:
      - EXL2_CACHE_MAX_SEQ_LEN
      - EXL2_GPU_SPLIT_GB (comma-separated, e.g. "29,2")
      - RESERVE_VRAM_GB
    """
    if not os.path.isdir(model_dir):
        raise RuntimeError(f"MODEL_PATH does not exist: {model_dir}")

    max_seq = int(os.getenv("EXL2_CACHE_MAX_SEQ_LEN", "2048"))
    gpu_split = _parse_gpu_split(os.getenv("EXL2_GPU_SPLIT_GB", ""))
    reserve_gb = float(os.getenv("RESERVE_VRAM_GB", "0") or 0)

    cfg = ExLlamaV2Config(model_dir)
    cfg.prepare()  # builds internal index

    model = ExLlamaV2(cfg)
    tokenizer = Tokenizer(cfg)

    # Lazy cache helps large prompts; keep it small but safe
    cache = ExLlamaV2Cache(model, max_seq_len=max_seq, lazy=True)

    # Multi-GPU autosplit; avoid model.to(...)
    try:
        model.load_autosplit(cache, gpu_split=gpu_split, reserve_vram=reserve_gb)
    except TypeError:
        # some builds use different arg name(s)
        model.load_autosplit(cache, gpu_split)

    generator = ExLlamaV2BaseGenerator(model, tokenizer, cache)
    return SQLCoderExLlama(generator, tokenizer, max_seq_len=max_seq)
