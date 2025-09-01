# core/model_loader.py
from __future__ import annotations
"""
Model loader — minimal edition (AWQ for local 4-bit; HF full-precision for server)

What this module provides
-------------------------
- A single `load_model(settings)` function that returns a dict handle:
    {
      "backend": "awq" | "hf-fp16" | "hf-bf16",
      "tokenizer": <AutoTokenizer>,
      "model": <HF model>,
      "generate": callable(prompt: str, *, max_new_tokens=None, temperature=None, top_p=None, stop=None) -> str
    }

Backends
--------
- awq     : AutoAWQ 4-bit (pre-quantized; recommended for local dev)
- hf-fp16 : FP16 full-precision (server)
- hf-bf16 : BF16 full-precision (server)

Relevant Settings / .env keys
-----------------------------
MODEL_BACKEND        = awq | hf-fp16 | hf-bf16
MODEL_PATH           = /path/to/model or HF repo id
MODEL_MAX_SEQ_LEN    = 4096            # optional
DEVICE_MAP           = auto            # auto | balanced | sequential
MAX_GPU_MEMORY       = "0:28GiB,1:10GiB"  # to shape sharding across 5090+3060
TRUST_REMOTE_CODE    = true/false      # default true

GENERATION_MAX_NEW_TOKENS = 256
GENERATION_TEMPERATURE    = 0.2
GENERATION_TOP_P          = 0.9

Notes
-----
- Keys for MAX_GPU_MEMORY **must be integers** (0,1,...) — not 'cuda:0'.
- We purposely do **not** access `model.device` or `hf_device_map` as those can be
  absent with AWQ/accelerate. We let Transformers/accelerate route tensors.
"""

from typing import Any, Dict, Optional
import os
from contextlib import suppress
import torch

def _ensure_single_process_pg() -> None:
    """
    Initialize a trivial single-process process group (gloo) if not already set.
    This satisfies torch.distributed.checkpoint code paths that Accelerate may trigger.
    Safe to call multiple times.
    """
    import torch.distributed as dist
    if not dist.is_available():
        return
    if dist.is_initialized():
        return
    os.environ.setdefault("MASTER_ADDR", "127.0.0.1")
    os.environ.setdefault("MASTER_PORT", "29500")
    with suppress(RuntimeError):
        dist.init_process_group(backend="gloo", rank=0, world_size=1)

def _offload_dir(settings) -> Optional[str]:
    p = settings.get("OFFLOAD_FOLDER")
    if not p:
        return None
    p = str(p)
    try:
        os.makedirs(p, exist_ok=True)
    except Exception:
        pass
    return p

# ---------------------------- small helpers ----------------------------

def _b(val: Any, default: bool = False) -> bool:
    """Coerce common truthy strings to bool (after processing: normalize)"""
    if val is None:
        return default
    return str(val).strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _parse_max_mem(val: Any | None) -> Optional[Dict[int, str]]:
    """
    Parse MAX_GPU_MEMORY to dict[int,str] for accelerate/transformers.
    Accepts:
      - dict: {"0": "28GiB", "1": "10GiB"} or {0: "28GiB", 1: "10GiB"}
      - str : "0:28GiB,1:10GiB"
    After processing: returns {0:"28GiB", 1:"10GiB"} or None
    """
    if not val:
        return None
    if isinstance(val, dict):
        out: Dict[int, str] = {}
        for k, v in val.items():
            try:
                out[int(k)] = str(v)
            except Exception:
                pass  # after processing: silently ignore bad keys
        return out or None

    out: Dict[int, str] = {}
    for part in str(val).split(","):
        part = part.strip()
        if not part or ":" not in part:
            continue
        k, cap = part.split(":", 1)
        try:
            out[int(k.strip())] = cap.strip()
        except Exception:
            pass  # after processing: ignore malformed piece
    return out or None


def _sanitize_dist_env():
    """
    Ensure we don't accidentally trigger DDP when using single-process, multi-GPU.
    Removes common DDP hints if present.
    """
    import os
    for k in [
        "RANK", "LOCAL_RANK", "WORLD_SIZE", "NODE_RANK",
        "MASTER_ADDR", "MASTER_PORT",
        # accelerate/deepspeed hints
        "ACCELERATE_USE_DEEPSPEED", "ACCELERATE_USE_FSDP",
        "DEEPSPEED_CONFIG_FILE", "FSDP_BACKEND",
    ]:
        os.environ.pop(k, None)

def _gen_defaults(settings) -> Dict[str, Any]:
    """Collect generation defaults from settings/env (after processing: merged defaults)."""
    return {
        "max_new_tokens": int(settings.get("GENERATION_MAX_NEW_TOKENS", 256) or 256),
        "temperature": float(settings.get("GENERATION_TEMPERATURE", 0.2) or 0.2),
        "top_p": float(settings.get("GENERATION_TOP_P", 0.9) or 0.9),
    }


# ---------------------------- AWQ backend ----------------------------

def _load_awq(model_path: str, max_seq_len: int, gen_def: dict, settings) -> Dict[str, Any]:
    """
    Load 4-bit AWQ model (AutoAWQ). Robust to common accelerate/meta/distributed issues.
    Tries (device_map+max_memory) → no max_memory → single-GPU, and will lazy-init a
    single-process process group if accelerate/t.distributed paths require it.
    """
    # --- imports local to keep module import cheap ---
    try:
        try:
            from awq import AutoAWQForCausalLM  # preferred package name
        except Exception:
            from autoawq import AutoAWQForCausalLM  # some wheels install this name
    except Exception as e:
        raise RuntimeError(f"AutoAWQ not installed correctly: {e}") from e

    from transformers import AutoTokenizer
    import torch, os

    # --- config from settings/env ---
    trust_rc  = _b(settings.get("TRUST_REMOTE_CODE", True), True)
    device_map = (settings.get("DEVICE_MAP") or "auto")
    max_mem    = _parse_max_mem(settings.get("MAX_GPU_MEMORY"))
    off_dir = settings.get("OFFLOAD_FOLDER") or os.getenv("OFFLOAD_FOLDER") or None
    if off_dir:
        os.makedirs(off_dir, exist_ok=True)

    # --- tokenizer ---
    tok = AutoTokenizer.from_pretrained(model_path, use_fast=True, trust_remote_code=trust_rc)
    if tok.pad_token is None and tok.eos_token is not None:
        tok.pad_token = tok.eos_token  # ensure padding is set

    # --- helper that actually calls from_quantized with optional offload args ---
    def _try_load(dm, mm, allow_offload: bool = True):
        kwargs = dict(
            device_map=dm,
            max_memory=mm,
            fuse_layers=True,
            trust_remote_code=trust_rc,
            safetensors=True,
            max_seq_len=max_seq_len,
        )
        if allow_offload and off_dir:
            kwargs.update(dict(offload_folder=off_dir, offload_buffers=True))
        try:
            return AutoAWQForCausalLM.from_quantized(model_path, **kwargs)
        except TypeError as te:
            # Some builds don’t accept offload_* kwargs → retry without them
            msg = str(te).lower()
            if "offload" in msg or "unexpected keyword argument" in msg:
                kwargs.pop("offload_folder", None)
                kwargs.pop("offload_buffers", None)
                return AutoAWQForCausalLM.from_quantized(model_path, **kwargs)
            raise

    # --- attempt 1: multi-GPU with max_memory ---
    try:
        model = _try_load(device_map, max_mem, allow_offload=True)
    except ValueError as e:
        emsg = str(e).lower()

        # Accelerate demands offload folder
        if ("offloaded to disk" in emsg or "pass along an offload_folder" in emsg) and not off_dir:
            off_dir = "/tmp/awq_offload"
            os.makedirs(off_dir, exist_ok=True)
            try:
                model = _try_load(device_map, max_mem, allow_offload=True)
            except Exception:
                pass  # fall through to other fallbacks

        # Meta/qweight/distributed default group issues → progressive relax + init PG
        if ('qweight' in emsg or 'meta' in emsg or 'set_module_tensor_to_device' in emsg
                or 'default process group has not been initialized' in emsg):
            _ensure_single_process_pg()
            try:
                # attempt 2: multi-GPU, no max_memory
                model = _try_load(device_map, None, allow_offload=True)
            except Exception:
                # attempt 3: single-device
                model = _try_load(None, None, allow_offload=True)
        else:
            raise
    except Exception:
        # Non-ValueError fallback: single-device without explicit max_memory
        _ensure_single_process_pg()
        model = _try_load(None, None, allow_offload=True)

    # --- define generate BEFORE returning to avoid 'unresolved reference' ---
    def generate(prompt: str, *, max_new_tokens=None, temperature=None, top_p=None, stop=None) -> str:
        """
        Generate text with the loaded AWQ model. Stop strings are trimmed client-side.
        """
        mnt  = int(max_new_tokens if max_new_tokens is not None else gen_def["max_new_tokens"])
        temp = float(gen_def["temperature"] if temperature is None else temperature)
        topp = float(gen_def["top_p"]       if top_p is None       else top_p)

        # Let accelerate handle device placement; no manual .to(device)
        inputs = tok(prompt, return_tensors="pt")
        with torch.inference_mode():
            out = model.generate(
                **inputs,
                max_new_tokens=mnt,
                do_sample=(temp > 0.0),
                temperature=temp,
                top_p=topp,
                eos_token_id=tok.eos_token_id,
                pad_token_id=tok.eos_token_id,
            )
        text = tok.decode(out[0], skip_special_tokens=True)
        if stop:
            for s in stop:
                i = text.find(s)
                if i >= 0:
                    text = text[:i]
                    break
        return text

    return {"backend": "awq", "tokenizer": tok, "model": model, "generate": generate}


# ---------------------------- HF full-precision (server) ----------------------------

def _load_hf_full(model_path: str, dtype: str, gen_def: dict, settings) -> Dict[str, Any]:
    """
    Load full-precision Transformers model for server (FP16 or BF16).

    Steps:
      1) Pick torch dtype (float16|bfloat16)
      2) Load tokenizer
      3) Load model with device_map + max_memory (multi-GPU friendly)
      4) Generate by moving inputs to cuda:0 if available (good default when sharded)
    """
    from transformers import AutoTokenizer, AutoModelForCausalLM

    trust_rc = _b(settings.get("TRUST_REMOTE_CODE", True), True)
    device_map = (settings.get("DEVICE_MAP") or "auto")
    max_mem = _parse_max_mem(settings.get("MAX_GPU_MEMORY"))

    torch_dtype = torch.float16 if dtype == "fp16" else torch.bfloat16

    tok = AutoTokenizer.from_pretrained(model_path, trust_remote_code=trust_rc)
    if tok.pad_token is None and tok.eos_token is not None:
        tok.pad_token = tok.eos_token  # after processing: ensure padding

    off_dir = _offload_dir(settings)
    model = AutoModelForCausalLM.from_pretrained(
        model_path,
        trust_remote_code=trust_rc,
        torch_dtype=torch_dtype,
        device_map=device_map,
        max_memory=max_mem,
        offload_folder=off_dir,  # <-- NEW
        offload_state_dict=True,  # <-- recommended when offloading
    )

    def generate(prompt: str, *, max_new_tokens=None, temperature=None, top_p=None, stop=None) -> str:
        """
        Generate with HF model.
        After processing:
          - Inputs are moved to cuda:0 when available; this aligns with embeddings usually on the first GPU.
        """
        mnt = int(max_new_tokens if max_new_tokens is not None else gen_def["max_new_tokens"])
        temp = float(gen_def["temperature"] if temperature is None else temperature)
        topp = float(gen_def["top_p"]       if top_p is None       else top_p)

        dev = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
        inputs = tok(prompt, return_tensors="pt").to(dev)

        with torch.inference_mode():
            out = model.generate(
                **inputs,
                max_new_tokens=mnt,
                do_sample=(temp > 0.0),
                temperature=temp,
                top_p=topp,
                eos_token_id=tok.eos_token_id,
                pad_token_id=tok.eos_token_id,
            )
        text = tok.decode(out[0], skip_special_tokens=True)
        if stop:
            for s in stop:
                i = text.find(s)
                if i >= 0:
                    text = text[:i]
                    break
        return text  # after processing: final decoded text

    backend = "hf-fp16" if dtype == "fp16" else "hf-bf16"
    return {"backend": backend, "tokenizer": tok, "model": model, "generate": generate}


# ---------------------------- public entrypoint ----------------------------

def load_model(settings) -> Dict[str, Any]:
    """
    Resolve backend + model path from Settings/env, load the model, and return a
    small handle with `.generate(...)`.

    After processing:
      - Only AWQ and HF(full) are supported (gptq/exllama removed).
      - Multi-GPU handled via device_map + MAX_GPU_MEMORY.
    """
    backend = (settings.get("MODEL_BACKEND", "awq") or "awq").lower()
    model_path = settings.get("MODEL_PATH")
    if not model_path:
        raise ValueError("MODEL_PATH is required")

    max_seq_len = int(settings.get("MODEL_MAX_SEQ_LEN", 4096) or 4096)
    gen_def = _gen_defaults(settings)

    if backend == "awq":
        return _load_awq(model_path, max_seq_len, gen_def, settings)

    if backend in {"hf-fp16", "hf-bf16"}:
        return _load_hf_full(model_path, "fp16" if backend == "hf-fp16" else "bf16", gen_def, settings)

    raise ValueError(f"Unknown MODEL_BACKEND: {backend} (supported: awq, hf-fp16, hf-bf16)")
