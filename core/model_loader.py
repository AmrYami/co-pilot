"""
Model loader for local (4‑bit EXL2 via ExLlamaV2) and server (FP16 via HF Transformers).

Single source of truth, driven by environment variables (or an injected `settings` shim
with a `.get(key, default=None)` API). Returns a lightweight handle you can call like:

    from core.model_loader import load_model
    llm = load_model()
    text = llm.generate(prompt, max_new_tokens=256)

Env keys (examples):
  ENVIRONMENT=local|server                # chooses sensible defaults
  MODEL_BACKEND=exllama|hf-fp16|hf-8bit|hf-4bit
  MODEL_PATH=/models/SQLCoder-70B-EXL2
  MODEL_MAX_SEQ_LEN=4096
  MODEL_TRUST_REMOTE_CODE=true
  DEVICE_MAP=auto                         # for HF; or omit to let HF decide
  MAX_MEMORY=cuda:0=22GiB,cuda:1=8GiB     # optional HF per-device memory map
  TORCH_DTYPE=float16|bfloat16            # HF dtype override (server)
  GENERATION_MAX_NEW_TOKENS=256
  GENERATION_TEMPERATURE=0.2
  GENERATION_TOP_P=0.9
  STOP=</s>,<|im_end|>                    # comma separated stop sequences
  LLM_GPU=0,1                             # GPU devices to use (comma separated)
  CUDA_VISIBLE_DEVICES=0,1                # Alternative GPU specification
"""
from __future__ import annotations
import os, time
import torch
from typing import Any, Callable, Dict, Iterable, Optional

import threading
_MODEL_CACHE: dict[tuple, "ModelHandle"] = {}
_MODEL_LOCK = threading.Lock()

class _SettingsShim:
    """Optional shim so we can read from a settings service; falls back to os.environ."""
    def __init__(self, settings: Any | None = None):
        self.settings = settings

    def get(self, key: str, default: Optional[str] = None) -> Optional[str]:
        if self.settings is not None:
            try:
                return self.settings.get(key)  # type: ignore[attr-defined]
            except Exception:
                pass
        return os.getenv(key, default)


def _parse_bool(v: Optional[str], default: bool) -> bool:
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "t", "yes", "y"}


def _parse_stop(v: Optional[str]) -> Optional[Iterable[str]]:
    if not v:
        return None
    return [s.strip() for s in v.split(",") if s.strip()]


def _parse_max_memory(v: Optional[str]) -> Optional[Dict[str, str]]:
    # e.g. "cuda:0=22GiB,cuda:1=8GiB,cpu=64GiB"
    if not v:
        return None
    mm: Dict[str, str] = {}
    for part in v.split(","):
        if "=" in part:
            k, val = part.split("=", 1)
            mm[k.strip()] = val.strip()
    return mm or None


def _setup_gpu_devices(s: _SettingsShim) -> None:
    """Setup GPU device ordering and visibility"""
    llm_gpu = s.get("LLM_GPU")
    cuda_visible = s.get("CUDA_VISIBLE_DEVICES")

    # Priority: LLM_GPU > CUDA_VISIBLE_DEVICES > default
    if llm_gpu:
        os.environ["CUDA_VISIBLE_DEVICES"] = llm_gpu
        print(f"Set CUDA_VISIBLE_DEVICES to: {llm_gpu}")
    elif cuda_visible:
        print(f"Using existing CUDA_VISIBLE_DEVICES: {cuda_visible}")
    else:
        # Default to first available GPU
        if torch.cuda.is_available():
            os.environ["CUDA_VISIBLE_DEVICES"] = "0"
            print("Set CUDA_VISIBLE_DEVICES to: 0 (default)")


def _check_cuda_compatibility() -> bool:
    """Check if CUDA is available and compatible"""
    if not torch.cuda.is_available():
        print("WARNING: CUDA not available, falling back to CPU")
        return False

    print(f"CUDA available: {torch.cuda.is_available()}")
    print(f"CUDA devices: {torch.cuda.device_count()}")
    for i in range(torch.cuda.device_count()):
        props = torch.cuda.get_device_properties(i)
        print(f"  GPU {i}: {props.name} ({props.total_memory // 1024**3} GB)")

    return True


class ModelHandle:
    def __init__(self, backend: str, model: Any, tokenizer: Any, generate_fn: Callable[..., str],
                 meta: Optional[Dict[str, Any]] = None):
        self.meta = meta or {}
        self.backend = backend
        self.model = model
        self.tokenizer = tokenizer
        self._generate = generate_fn

    def generate(
        self,
        prompt: str,
        max_new_tokens: Optional[int] = None,
        temperature: Optional[float] = None,
        top_p: Optional[float] = None,
        stop: Optional[Iterable[str]] = None,
    ) -> str:
        return self._generate(
            prompt,
            max_new_tokens=max_new_tokens,
            temperature=temperature,
            top_p=top_p,
            stop=stop,
        )


def load_model(settings: Any | None = None) -> ModelHandle:
    s = _SettingsShim(settings)

    # Setup GPU order before any CUDA import
    _setup_gpu_devices(s)
    _check_cuda_compatibility()

    env = (s.get("ENVIRONMENT", "local") or "local").lower()
    default_backend = "exllama" if env == "local" else "hf-fp16"

    backend = (s.get("MODEL_BACKEND", default_backend) or default_backend).lower()
    model_path = s.get("MODEL_PATH")
    if not model_path:
        raise ValueError("MODEL_PATH is required")
    if not os.path.exists(model_path):
        raise ValueError(f"MODEL_PATH does not exist: {model_path}")

    max_seq_len = int(s.get("MODEL_MAX_SEQ_LEN", "4096") or 4096)
    stop        = _parse_stop(s.get("STOP"))
    gen_defaults = {
        "max_new_tokens": int(s.get("GENERATION_MAX_NEW_TOKENS", "256") or 256),
        "temperature": float(s.get("GENERATION_TEMPERATURE", "0.2") or 0.2),
        "top_p": float(s.get("GENERATION_TOP_P", "0.9") or 0.9),
        "stop": stop,
    }

    # Build a cache key that captures backend-critical knobs
    # (For EXL2, include base/dynamic and cache length knobs)
    exl_force_base = (os.getenv("EXL2_FORCE_BASE", "0").strip().lower() in {"1","true","yes","y"})
    exl_cache_len  = int(os.getenv("EXL2_CACHE_MAX_SEQ_LEN", str(max_seq_len)) or max_seq_len)
    llm_gpu_vis    = os.getenv("CUDA_VISIBLE_DEVICES") or os.getenv("LLM_GPU") or ""
    gpu_split_env  = (os.getenv("GPU_SPLIT") or "").strip()

    reserve_gb_key = (os.getenv("RESERVE_VRAM_GB") or "").strip()

    key = (
        backend,
        os.path.abspath(model_path),
        max_seq_len,
        exl_force_base,
        exl_cache_len,
        llm_gpu_vis,
        gpu_split_env,
        reserve_gb_key,
    )

    with _MODEL_LOCK:
        mh = _MODEL_CACHE.get(key)
        if mh is not None:
            print("Reusing cached model handle")
            return mh

        print(f"Loading model: {model_path}")
        print(f"Backend: {backend}")
        print(f"Max sequence length: {max_seq_len}")

        if backend == "exllama":
            mh = _load_exllama(model_path, max_seq_len, gen_defaults, s)
        elif backend in {"hf-fp16", "hf-8bit", "hf-4bit"}:
            mh = _load_hf(model_path, backend, max_seq_len, gen_defaults, s)
        else:
            raise ValueError(f"Unknown MODEL_BACKEND: {backend}")

        _MODEL_CACHE[key] = mh
        return mh


# ------------------------------
# Backends
# ------------------------------

def _load_exllama(model_path: str, max_seq_len: int, gen_defaults: Dict[str, Any], s: _SettingsShim) -> ModelHandle:
    """Load ExLlamaV2 with robust autosplit/manual split, lazy cache, and Dynamic→Base fallback."""

    import os, time
    import torch

    # ---------- Imports ----------
    try:
        print("Importing ExLlamaV2...")
        from exllamav2 import ExLlamaV2Config, ExLlamaV2, ExLlamaV2Tokenizer, ExLlamaV2Cache
        from exllamav2.generator import ExLlamaV2Sampler
        print("ExLlamaV2 imported successfully")
    except Exception as e:
        raise RuntimeError(
            "ExLlamaV2 import failed (JIT/toolchain likely). "
            f"Original error: {e}"
        )

    # ---------- Flags / policy ----------
    force_base = str(os.getenv("EXL2_FORCE_BASE", "0")).lower() in {"1", "true", "yes", "y"}

    # If we force Base, proactively disable use of flash-attn inside EXL2 load path
    if force_base:
        try:
            import exllamav2.attn as _attn
            _attn.has_flash_attn = False
            print("FlashAttention disabled for load (EXL2_FORCE_BASE=1)")
        except Exception:
            pass

    dyn_available = False
    if not force_base:
        try:
            from exllamav2.generator.dynamic import ExLlamaV2DynamicGenerator  # noqa
            dyn_available = True
            print("Dynamic generator available")
        except Exception as e:
            print(f"Dynamic generator not available: {e}")

    # ---------- Config ----------
    print("Creating ExLlamaV2 config...")
    cfg = ExLlamaV2Config(model_path)
    cfg.max_seq_len = max_seq_len
    if torch.cuda.is_available():
        cfg.set_low_mem()
        cfg.gpu_peer_fix = True
        print("Enabled low memory mode and peer fix")

    print("Preparing config...")
    cfg.prepare()
    print("Config prepared successfully")

    # ---------- Model / tokenizer / cache ----------
    print("Loading model...")
    model = ExLlamaV2(cfg)
    print("Model created")

    print("Loading tokenizer...")
    tok = ExLlamaV2Tokenizer(cfg)
    print("Tokenizer loaded")

    cache_len = int(os.getenv("EXL2_CACHE_MAX_SEQ_LEN", str(max_seq_len)))
    print(f"Creating lazy cache (max_seq_len={cache_len})...")
    cache = ExLlamaV2Cache(model, lazy=True, max_seq_len=cache_len)
    print("Cache created")

    # ---------- Split parsing (settings → env), GiB or fractions ----------
    split: list[float] | None = None
    split_cfg = (s.get("GPU_SPLIT") or os.getenv("GPU_SPLIT", "")).strip()
    if split_cfg and not force_base:
        vals = [float(x) for x in split_cfg.split(",") if x.strip()]
        if vals:
            if max(vals) > 1.5:
                # GiB input (e.g., "30,10") → normalize to fractions that sum ≈ 1.0
                total = sum(vals)
                if total <= 0:
                    raise ValueError("GPU_SPLIT total must be > 0")
                split = [v / total for v in vals]
            else:
                # Already fractions; normalize defensively
                total = sum(vals)
                split = [v / total for v in vals] if total > 0 else None
        if split:
            print(f"Manual split normalized from '{split_cfg}' → {split}")

    reserve = None
    try:
        reserve_gb = float(os.getenv("RESERVE_VRAM_GB", "0") or 0)
        if torch.cuda.device_count() > 1 and reserve_gb > 0:
            reserve = [0, int(reserve_gb * (1 << 30))]
            print(f"Reserve VRAM on secondary GPU: {reserve_gb} GiB")
    except Exception:
        reserve = None

    # ---------- Load weights with fallback ----------

    def _try_load(split_f, cache_f) -> None:
        t0 = time.time()
        if split_f:
            model.load(split_f, cache_f)
            print(f"Model weights loaded with manual split={split_f} in {time.time() - t0:.2f}s")
        else:
            model.load_autosplit(cache_f, progress=True, reserve_vram=reserve)
            print(f"Model weights loaded (autosplit) in {time.time() - t0:.2f}s")

    try:
        _try_load(split, cache)
    except Exception as e:
        msg = str(e)
        print(f"Initial load failed: {msg}")

        # If FlashAttention is the culprit (or any OOM-ish issue), disable flash and retry autosplit with smaller cache
        flash_fail = ("FlashAttention" in msg) or ("flash_attn" in msg)
        oomish = any(
            k in msg
            for k in (
                "Insufficient space",
                "Insufficient VRAM",
                "Insufficient VRAM for model and cache",
                "out of memory",
                "CUDA error",
            )
        )

        if flash_fail:
            try:
                import exllamav2.attn as _attn
                _attn.has_flash_attn = False
                print("Disabled FlashAttention after failure; retrying load…")
            except Exception:
                pass

        if flash_fail or oomish:
            smaller = max(1024, cache_len // 2)
            if smaller < cache_len:
                print(f"Retrying load with smaller cache_len={smaller} and autosplit…")
                cache = ExLlamaV2Cache(model, lazy=True, max_seq_len=smaller)
                _try_load(None, cache)
            else:
                print("No smaller cache_len possible; rethrowing.")
                raise
        else:
            raise


    # ---------- Generator selection ----------
    gen_is_dynamic = False
    gen = None

    if dyn_available and not force_base:
        try:
            print("Attempting Dynamic generator…")
            from exllamav2.generator.dynamic import ExLlamaV2DynamicGenerator
            gen = ExLlamaV2DynamicGenerator(model=model, tokenizer=tok, cache=cache)
            gen_is_dynamic = True
            print("Dynamic generator created successfully")
        except Exception as e:
            print(f"Dynamic generator failed: {e}. Falling back to Base.")

    if gen is None:
        from exllamav2.generator.base import ExLlamaV2BaseGenerator
        gen = ExLlamaV2BaseGenerator(model=model, tokenizer=tok, cache=cache)
        print("Base generator created successfully")

    # ---------- Generation wrapper ----------
    def _generate(prompt: str, **kw: Any) -> str:
        args = {**gen_defaults, **{k: v for k, v in kw.items() if v is not None}}
        max_new = int(args["max_new_tokens"])
        temp = float(args["temperature"])
        top_p = float(args["top_p"])

        if gen_is_dynamic:
            try:
                text = gen.generate_simple(
                    prompt,
                    max_new_tokens=max_new,
                    temperature=temp,
                    top_p=top_p,
                )
            except TypeError:
                pass
            else:
                for stop_tok in (args.get("stop") or []):
                    if stop_tok and stop_tok in text:
                        text = text.split(stop_tok, 1)[0]
                return text

        settings = ExLlamaV2Sampler.Settings()
        settings.temperature, settings.top_p = temp, top_p
        try:
            out = gen.generate_simple(prompt, settings, max_new)
        except TypeError:
            try:
                out = gen.generate_simple(prompt, max_new, settings)
            except TypeError:
                out = gen.generate_simple(prompt, settings=settings, max_new_tokens=max_new)

        text = out[0] if isinstance(out, (tuple, list)) else out
        for stop_tok in (args.get("stop") or []):
            if stop_tok and stop_tok in text:
                text = text.split(stop_tok, 1)[0]
        return text

    print("ExLlamaV2 model loaded successfully!")
    try:
        import exllamav2 as _exl
        exl_version = getattr(_exl, "__version__", "unknown")
        from exllamav2 import attn as _exl_attn
        flash_enabled = bool(getattr(_exl_attn, "has_flash_attn", False))
    except Exception:
        exl_version, flash_enabled = "unknown", False

    visible = os.getenv("CUDA_VISIBLE_DEVICES") or os.getenv("LLM_GPU") or ""
    gpu_info = []
    if torch.cuda.is_available():
        for i in range(torch.cuda.device_count()):
            p = torch.cuda.get_device_properties(i)
            gpu_info.append({"index": i, "name": p.name, "total_gib": round(p.total_memory / (1 << 30), 2)})

    meta = {
        "backend": "exllama",
        "generator": "dynamic" if gen_is_dynamic else "base",
        "force_base": force_base,
        "flash_attn_enabled": flash_enabled,
        "model_path": model_path,
        "torch_version": torch.__version__,
        "exllama_version": exl_version,
        "model_max_seq_len": max_seq_len,
        "cache_max_seq_len": cache_len,
        "placement": "manual" if split else "autosplit",
        "split_fractions": [round(x, 4) for x in (split or [])],
        "reserve_vram_gb": float(os.getenv("RESERVE_VRAM_GB", "0") or 0),
        "visible_devices": visible,
        "gpus": gpu_info,
        "arches": os.getenv("TORCH_CUDA_ARCH_LIST", ""),
        "loaded_at": time.time(),
    }
    return ModelHandle("exllama", model, tok, _generate, meta=meta)


def _load_hf(
    model_path: str,
    backend: str,
    max_seq_len: int,
    gen_defaults: Dict[str, Any],
    s: _SettingsShim,
) -> ModelHandle:
    import torch
    from transformers import AutoModelForCausalLM, AutoTokenizer

    trust = _parse_bool(s.get("MODEL_TRUST_REMOTE_CODE", "true"), True)

    # dtype
    dtype_str = (s.get("TORCH_DTYPE") or ("bfloat16" if torch.cuda.is_available() else "float32")).lower()
    if dtype_str == "float16":
        dtype = torch.float16
    elif dtype_str == "bfloat16":
        dtype = torch.bfloat16
    else:
        dtype = torch.float32

    print(f"Loading tokenizer from: {model_path}")
    tok = AutoTokenizer.from_pretrained(model_path, trust_remote_code=trust)

    load_kwargs: Dict[str, Any] = {
        "trust_remote_code": trust,
        "torch_dtype": dtype,
    }

    # quantization / memory routing
    device_map = s.get("DEVICE_MAP", "auto") or "auto"
    if device_map == "auto":
        load_kwargs["device_map"] = "auto"
    max_memory = _parse_max_memory(s.get("MAX_MEMORY"))
    if max_memory:
        load_kwargs["device_map"] = "auto"
        load_kwargs["max_memory"] = max_memory

    if backend == "hf-8bit":
        load_kwargs["load_in_8bit"] = True
    elif backend == "hf-4bit":
        load_kwargs["load_in_4bit"] = True

    print(f"Loading HF model with backend: {backend}")
    print(f"Load kwargs: {load_kwargs}")
    model = AutoModelForCausalLM.from_pretrained(model_path, **load_kwargs)
    print("HF model loaded successfully!")

    def _generate(prompt: str, **kw: Any) -> str:
        args = {**gen_defaults, **{k: v for k, v in kw.items() if v is not None}}
        inputs = tok(prompt, return_tensors="pt")
        inputs = {k: v.to(model.device) for k, v in inputs.items()}
        with torch.inference_mode():
            out = model.generate(
                **inputs,
                max_new_tokens=args["max_new_tokens"],
                do_sample=args["temperature"] > 0,
                temperature=args["temperature"],
                top_p=args["top_p"],
                eos_token_id=_first_token_id(tok, args.get("stop")),
            )
        text = tok.decode(out[0], skip_special_tokens=True)
        # fallback stop handling
        for sseq in (args.get("stop") or []):
            if sseq and sseq in text:
                text = text.split(sseq, 1)[0]
        return text

    meta = {
        "backend": backend,
        "model_path": model_path,
        "torch_version": torch.__version__,
        "dtype": str(dtype),
        "device_map": load_kwargs.get("device_map", "auto"),
        "max_memory": max_memory,
        "loaded_at": time.time(),
    }
    return ModelHandle(backend, model, tok, _generate, meta=meta)


def _first_token_id(tok: Any, stop: Optional[Iterable[str]]) -> Optional[int]:
    if not stop:
        return None
    try:
        s0 = next(iter(stop))
        if not s0:
            return None
        return tok.convert_tokens_to_ids(s0)
    except Exception:
        return None