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
import os
import torch
from typing import Any, Callable, Dict, Iterable, Optional


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
    def __init__(self, backend: str, model: Any, tokenizer: Any, generate_fn: Callable[..., str]):
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

    # Setup GPU devices before importing any CUDA libraries
    _setup_gpu_devices(s)
    _check_cuda_compatibility()

    # Defaults by environment
    env = (s.get("ENVIRONMENT", "local") or "local").lower()
    default_backend = "exllama" if env == "local" else "hf-fp16"

    backend = (s.get("MODEL_BACKEND", default_backend) or default_backend).lower()
    model_path = s.get("MODEL_PATH")
    if not model_path:
        raise ValueError("MODEL_PATH is required")

    if not os.path.exists(model_path):
        raise ValueError(f"MODEL_PATH does not exist: {model_path}")

    max_seq_len = int(s.get("MODEL_MAX_SEQ_LEN", "4096") or 4096)
    stop = _parse_stop(s.get("STOP"))
    gen_defaults = {
        "max_new_tokens": int(s.get("GENERATION_MAX_NEW_TOKENS", "256") or 256),
        "temperature": float(s.get("GENERATION_TEMPERATURE", "0.2") or 0.2),
        "top_p": float(s.get("GENERATION_TOP_P", "0.9") or 0.9),
        "stop": stop,
    }

    print(f"Loading model: {model_path}")
    print(f"Backend: {backend}")
    print(f"Max sequence length: {max_seq_len}")

    if backend == "exllama":
        return _load_exllama(model_path, max_seq_len, gen_defaults, s)
    elif backend in {"hf-fp16", "hf-8bit", "hf-4bit"}:
        return _load_hf(model_path, backend, max_seq_len, gen_defaults, s)
    else:
        raise ValueError(f"Unknown MODEL_BACKEND: {backend}")


# ------------------------------
# Backends
# ------------------------------

def _load_exllama(model_path: str, max_seq_len: int, gen_defaults: Dict[str, Any], s: _SettingsShim) -> ModelHandle:
    """Load ExLlamaV2 model with improved error handling"""

    try:
        print("Importing ExLlamaV2...")
        from exllamav2 import ExLlamaV2Config, ExLlamaV2, ExLlamaV2Tokenizer, ExLlamaV2Cache
        print("ExLlamaV2 imported successfully")
    except Exception as e:
        print(f"Failed to import ExLlamaV2: {e}")
        raise RuntimeError(f"ExLlamaV2 import failed. This usually means:\n"
                         f"1. CUDA extension compilation failed\n"
                         f"2. Incompatible CUDA version\n"
                         f"3. Missing build dependencies\n"
                         f"Original error: {e}")

    # Prefer dynamic generator; fall back to base
    DynGen = None
    BaseGen = None
    try:
        from exllamav2.generator import ExLlamaV2Generator as DynGen  # dynamic
        print("Dynamic generator available")
    except Exception as e:
        print(f"Dynamic generator not available: {e}")
        DynGen = None

    if BaseGen is None:
        try:
            from exllamav2.generator.base import ExLlamaV2BaseGenerator as BaseGen
            print("Base generator loaded from .base module")
        except Exception:
            try:
                from exllamav2.generator import ExLlamaV2BaseGenerator as BaseGen  # older path
                print("Base generator loaded from main module")
            except Exception as e:
                raise RuntimeError(f"Could not import any ExLlamaV2 generator: {e}")

    try:
        print("Creating ExLlamaV2 config...")
        cfg = ExLlamaV2Config()
        cfg.model_dir = model_path
        cfg.max_seq_len = max_seq_len

        # Set GPU device allocation
        if torch.cuda.is_available():
            cfg.set_low_mem()  # Enable memory optimization for large models
            print(f"Enabled low memory mode for large model")

        print("Preparing config...")
        cfg.prepare()  # IMPORTANT
        print("Config prepared successfully")

        print("Loading model...")
        model = ExLlamaV2(cfg)
        print("Model loaded successfully")

        print("Loading tokenizer...")
        tok = ExLlamaV2Tokenizer(cfg)
        print("Tokenizer loaded successfully")

        print("Creating cache...")
        cache = ExLlamaV2Cache(model, batch_size=1, max_seq_len=max_seq_len)
        print("Cache created successfully")

    except Exception as e:
        raise RuntimeError(f"Failed to initialize ExLlamaV2 components: {e}")

    # Helper that builds a generator regardless of init order, then fixes attrs
    def _ensure_device_idx(obj, default=0):
        # if generator later tries to read obj.device_idx, make sure it exists
        if hasattr(obj, "set_device_idx") and not hasattr(obj, "device_idx"):
            try:
                obj.set_device_idx(default)
            except Exception:
                pass
            try:
                setattr(obj, "device_idx", default)
            except Exception:
                pass

    # Apply to common components (embedding / lm_head naming varies a bit)
    for attr in ("embedding", "embed", "input_embed", "lm_head", "output_head", "head"):
        comp = getattr(model, attr, None)
        if comp is not None:
            _ensure_device_idx(comp)

    # --- generator creation (try both arg orders, then fix swapped attrs) ---
    def _make_gen(GenClass):
        last_err = None
        for args in ((model, tok, cache), (model, cache, tok)):
            try:
                print(f"Trying to create {GenClass.__name__} with args order: {[type(a).__name__ for a in args]}")
                g = GenClass(*args)
                print(f"Successfully created {GenClass.__name__}")
                break
            except TypeError as e:
                print(f"Failed with args order {[type(a).__name__ for a in args]}: {e}")
                last_err = e
                g = None
        if g is None:
            raise RuntimeError(f"ExLlamaV2 generator ctor mismatch: {last_err}")

        # If tokenizer/cache got swapped internally, fix them
        from exllamav2 import ExLlamaV2Tokenizer, ExLlamaV2Cache
        if isinstance(getattr(g, "tokenizer", None), ExLlamaV2Cache) and isinstance(getattr(g, "cache", None), ExLlamaV2Tokenizer):
            print("Fixing swapped tokenizer/cache attributes")
            g.tokenizer, g.cache = g.cache, g.tokenizer

        # Final safety: make sure generator sees a tokenizer with eos_token_id
        if not hasattr(g.tokenizer, "eos_token_id"):
            try:
                g.tokenizer.eos_token_id = g.tokenizer.convert_tokens_to_ids("</s>")
                print("Set eos_token_id to </s>")
            except Exception:
                try:
                    g.tokenizer.eos_token_id = g.tokenizer.convert_tokens_to_ids("<|endoftext|>")
                    print("Set eos_token_id to <|endoftext|>")
                except Exception:
                    print("Warning: Could not set eos_token_id")
                    pass
        return g

    gen_is_dynamic = False
    gen = None

    if DynGen is not None:
        try:
            print("Attempting to create dynamic generator...")
            gen = _make_gen(DynGen)
            gen_is_dynamic = True
            print("Dynamic generator created successfully")
        except Exception as e:
            print(f"Dynamic generator failed: {e}")
            gen = None

    if gen is None:
        print("Creating base generator...")
        gen = _make_gen(BaseGen)
        gen_is_dynamic = False
        print("Base generator created successfully")

    # ---- compatibility wrapper around different generate_simple signatures ----
    def _generate(prompt: str, **kw: Any) -> str:
        args = {**gen_defaults, **{k: v for k, v in kw.items() if v is not None}}
        max_new = int(args["max_new_tokens"])
        temp = float(args["temperature"])
        top_p = float(args["top_p"])

        # Try dynamic API first (supports keyword args)
        if gen_is_dynamic:
            try:
                text = gen.generate_simple(
                    prompt,
                    max_new_tokens=max_new,
                    temperature=temp,
                    top_p=top_p,
                )
            except TypeError:
                # fall through to Settings-based call
                pass
            else:
                for s in (args.get("stop") or []):
                    if s and s in text:
                        text = text.split(s, 1)[0]
                return text

        # Base (and compatible) API: use Sampler.Settings
        from exllamav2.generator import ExLlamaV2Sampler
        settings = ExLlamaV2Sampler.Settings()
        settings.temperature = temp
        settings.top_p = top_p

        # handle both possible orders
        try:
            # some versions: (prompt, settings, max_new_tokens)
            out = gen.generate_simple(prompt, settings, max_new)
        except TypeError:
            # other versions: (prompt, max_new_tokens, settings) or named
            try:
                out = gen.generate_simple(prompt, max_new, settings)
            except TypeError:
                out = gen.generate_simple(prompt, settings=settings, max_new_tokens=max_new)

        # out can be str or (str, tokens)
        text = out[0] if isinstance(out, (tuple, list)) else out
        for s in (args.get("stop") or []):
            if s and s in text:
                text = text.split(s, 1)[0]
        return text

    print("ExLlamaV2 model loaded successfully!")
    return ModelHandle("exllama", model, tok, _generate)


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
        load_kwargs["device_map"] = {k: i for i, k in enumerate(max_memory.keys())}  # let HF shard across listed devices
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

    return ModelHandle(backend, model, tok, _generate)


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